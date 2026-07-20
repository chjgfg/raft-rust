//! 使用 HashMap/BTreeMap 存储的进程内 3 节点 Raft 集群示例。
//!
//! 演示独立库的用法：
//! * 日志存储：`storage::Memory`（内存 BTreeMap）
//! * 状态机：简单的字符串键值映射
//! * 传输：crossbeam channel（无 TCP）
//!
//! 运行：
//! ```text
//! cargo run --example kv_cluster
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender};
use raft_rust::error::{Error, Result};
use raft_rust::raft::{
    self, Envelope, Entry, Index, Log, Message, Node, NodeID, Options, Request, Response, State,
    TICK_INTERVAL,
};
use raft_rust::storage::Memory;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// 应用层编解码（bincode）
// ---------------------------------------------------------------------------

const BINCODE: bincode::config::Configuration = bincode::config::standard();

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(value, BINCODE).expect("value must be serializable")
}

fn decode<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)?.0)
}

// ---------------------------------------------------------------------------
// 应用状态机：字符串键值存储
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvCommand {
    Get { key: String },
    Put { key: String, value: String },
    Scan,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvResponse {
    Get(Option<String>),
    Put(Index),
    Scan(BTreeMap<String, String>),
}

struct KvState {
    applied_index: Index,
    data: BTreeMap<String, String>,
}

impl KvState {
    fn new() -> Box<Self> {
        Box::new(Self { applied_index: 0, data: BTreeMap::new() })
    }
}

impl State for KvState {
    fn get_applied_index(&self) -> Index {
        self.applied_index
    }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        let command = entry.command.as_deref().map(decode::<KvCommand>).transpose()?;
        let response = match command {
            Some(KvCommand::Put { key, value }) => {
                self.data.insert(key, value);
                encode(&KvResponse::Put(entry.index))
            }
            Some(other) => panic!("{other:?} submitted as write command"),
            None => Vec::new(), // 领导者选举后的 Raft noop
        };
        self.applied_index = entry.index;
        Ok(response)
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match decode::<KvCommand>(&command)? {
            KvCommand::Get { key } => Ok(encode(&KvResponse::Get(self.data.get(&key).cloned()))),
            KvCommand::Scan => Ok(encode(&KvResponse::Scan(self.data.clone()))),
            other => panic!("{other:?} submitted as read command"),
        }
    }
}

// ---------------------------------------------------------------------------
// 集群脚手架
// ---------------------------------------------------------------------------

/// 客户端句柄：可向任意本地节点提交请求（跟随者会转发到领导者）。
struct Client {
    /// 各节点的请求注入通道。节点线程会填入当前任期。
    request_txs: HashMap<NodeID, Sender<(Request, Sender<Result<Response>>)>>,
    preferred: NodeID,
}

impl Client {
    fn request(&mut self, request: Request) -> Result<Response> {
        let mut order: Vec<NodeID> = self.request_txs.keys().copied().collect();
        order.sort();
        if let Some(pos) = order.iter().position(|&id| id == self.preferred) {
            let id = order.remove(pos);
            order.insert(0, id);
        }

        let mut last_err = Error::Abort;
        for _ in 0..30 {
            for &node_id in &order {
                let (resp_tx, resp_rx) = channel::bounded(1);
                if self.request_txs[&node_id].send((request.clone(), resp_tx)).is_err() {
                    continue;
                }
                match resp_rx.recv_timeout(Duration::from_secs(2)) {
                    Ok(Ok(resp)) => {
                        self.preferred = node_id;
                        return Ok(resp);
                    }
                    Ok(Err(Error::Abort)) => {
                        last_err = Error::Abort;
                        // 尝试下一节点 / 稍后重试
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        last_err = Error::IO("request timed out".into());
                    }
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(last_err)
    }

    fn put(&mut self, key: &str, value: &str) -> Result<Index> {
        let req = Request::Write(encode(&KvCommand::Put {
            key: key.into(),
            value: value.into(),
        }));
        match self.request(req)? {
            Response::Write(bytes) => match decode::<KvResponse>(&bytes)? {
                KvResponse::Put(index) => Ok(index),
                other => Err(Error::InvalidData(format!("unexpected write response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Write, got {other:?}"))),
        }
    }

    fn get(&mut self, key: &str) -> Result<Option<String>> {
        let req = Request::Read(encode(&KvCommand::Get { key: key.into() }));
        match self.request(req)? {
            Response::Read(bytes) => match decode::<KvResponse>(&bytes)? {
                KvResponse::Get(v) => Ok(v),
                other => Err(Error::InvalidData(format!("unexpected read response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Read, got {other:?}"))),
        }
    }

    fn scan(&mut self) -> Result<BTreeMap<String, String>> {
        let req = Request::Read(encode(&KvCommand::Scan));
        match self.request(req)? {
            Response::Read(bytes) => match decode::<KvResponse>(&bytes)? {
                KvResponse::Scan(map) => Ok(map),
                other => Err(Error::InvalidData(format!("unexpected scan response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Read, got {other:?}"))),
        }
    }

    fn status(&mut self) -> Result<raft::Status> {
        match self.request(Request::Status)? {
            Response::Status(s) => Ok(s),
            other => Err(Error::InvalidData(format!("expected Status, got {other:?}"))),
        }
    }
}

/// 启动进程内 Raft 集群，返回客户端句柄。
fn spawn_cluster(node_ids: &[NodeID]) -> Client {
    // 节点间 Raft 协议消息的同伴邮箱。
    let mut peer_tx: HashMap<NodeID, Sender<Envelope>> = HashMap::new();
    let mut peer_rx: HashMap<NodeID, Receiver<Envelope>> = HashMap::new();
    for &id in node_ids {
        let (tx, rx) = channel::unbounded();
        peer_tx.insert(id, tx);
        peer_rx.insert(id, rx);
    }

    let mut request_txs = HashMap::new();

    for &id in node_ids {
        let peers: HashSet<NodeID> = node_ids.iter().copied().filter(|&p| p != id).collect();
        let inbound = peer_rx.remove(&id).unwrap();
        let outbound: HashMap<NodeID, Sender<Envelope>> = peer_tx
            .iter()
            .filter(|(pid, _)| **pid != id)
            .map(|(pid, tx)| (*pid, tx.clone()))
            .collect();

        let (request_tx, request_rx) = channel::unbounded();
        request_txs.insert(id, request_tx);

        let (node_tx, node_rx) = channel::unbounded();
        let log = Log::new(Box::new(Memory::new())).expect("log");
        // 演示用更快超时，便于快速选主。
        let opts = Options {
            heartbeat_interval: 2,
            election_timeout_range: 5..10,
            max_append_entries: 100,
        };
        let node = Node::new(id, peers, log, KvState::new(), node_tx, opts).expect("node");

        thread::spawn(move || {
            run_node(node, inbound, node_rx, outbound, request_rx);
        });
    }

    Client { request_txs, preferred: node_ids[0] }
}

/// 单个 Raft 节点的事件循环（对应 toydb 的 `raft_route`）。
fn run_node(
    mut node: Node,
    peers_rx: Receiver<Envelope>,
    node_rx: Receiver<Envelope>,
    mut peers_tx: HashMap<NodeID, Sender<Envelope>>,
    request_rx: Receiver<(Request, Sender<Result<Response>>)>,
) {
    let ticker = channel::tick(TICK_INTERVAL);
    // 待返回的客户端响应通道，按请求 id 索引。
    let mut response_txs: HashMap<raft::RequestID, Sender<Result<Response>>> = HashMap::new();
    let node_id = node.id();

    loop {
        crossbeam::select! {
            // 推进 Raft 逻辑时间。
            recv(ticker) -> _ => {
                node = match node.tick() {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("node {node_id} tick error: {err}");
                        break;
                    }
                };
            }

            // 入站同伴消息。
            recv(peers_rx) -> msg => {
                let Ok(msg) = msg else { break };
                node = match node.step(msg) {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("node {node_id} step error: {err}");
                        break;
                    }
                };
            }

            // 来自 Raft 核心的出站消息。
            recv(node_rx) -> msg => {
                let Ok(msg) = msg else { break };
                // 本地客户端响应（to == self）。
                if msg.to == node_id {
                    if let Message::ClientResponse { id, response } = msg.message {
                        if let Some(tx) = response_txs.remove(&id) {
                            let _ = tx.send(response);
                        }
                    }
                    continue;
                }
                // 同伴消息。
                if let Some(tx) = peers_tx.get_mut(&msg.to) {
                    match tx.try_send(msg) {
                        Ok(()) => {}
                        Err(channel::TrySendError::Full(_)) => {
                            eprintln!("node {node_id}: peer channel full, dropping message");
                        }
                        Err(channel::TrySendError::Disconnected(_)) => {
                            // 同伴已消失；Raft 会通过心跳/选举重试。
                        }
                    }
                }
            }

            // 本地客户端请求——像 toydb 的 server 一样填入当前任期。
            recv(request_rx) -> result => {
                let Ok((request, response_tx)) = result else { break };
                let id = Uuid::new_v4();
                let msg = Envelope {
                    from: node.id(),
                    to: node.id(),
                    term: node.term(),
                    message: Message::ClientRequest { id, request },
                };
                response_txs.insert(id, response_tx);
                node = match node.step(msg) {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("node {node_id} client step error: {err}");
                        break;
                    }
                };
            }
        }
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let node_ids: Vec<NodeID> = vec![1, 2, 3];
    println!("Starting 3-node Raft cluster with in-memory (BTreeMap) storage...");
    let mut client = spawn_cluster(&node_ids);

    // 等待领导者选举。
    println!("Waiting for leader election...");
    let mut status = None;
    for _ in 0..50 {
        match client.status() {
            Ok(s) => {
                status = Some(s);
                break;
            }
            Err(Error::Abort) => thread::sleep(Duration::from_millis(100)),
            Err(e) => return Err(e),
        }
    }
    let status = status.expect("failed to elect a leader");
    println!(
        "Leader={} term={} commit={} applied={}",
        status.leader, status.term, status.commit_index, status.applied_index
    );
    println!("match_index={:?}", status.match_index);

    // 写
    println!("\nWriting key/value pairs...");
    for (k, v) in [("a", "apple"), ("b", "banana"), ("c", "cherry")] {
        let index = client.put(k, v)?;
        println!("  put {k}={v}  (committed at index {index})");
    }

    // 读
    println!("\nReading back...");
    for k in ["a", "b", "c", "missing"] {
        let v = client.get(k)?;
        println!("  get {k} => {v:?}");
    }

    // 扫描
    let all = client.scan()?;
    println!("\nFull scan: {all:?}");

    let status = client.status()?;
    println!(
        "\nFinal status: leader={} term={} commit={} applied={}",
        status.leader, status.term, status.commit_index, status.applied_index
    );
    println!("Done.");
    Ok(())
}
