//! 进程内 Raft 集群运行时：channel 传输、客户端与故障注入。
//!
//! 供 `main`、example 与集成测试复用。不包含真实网络或磁盘持久化。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender};
use rand::RngExt as _;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::raft::kv::{self, Command, Kv};
use crate::raft::{
    Envelope, Index, Log, Message, Node, NodeID, Options, Request, RequestID, Response, Status,
    TICK_INTERVAL,
};
use crate::storage::BitCask;

// ---------------------------------------------------------------------------
// 传输层（分区 / 丢包 / 乱序）
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TransportInner {
    /// `(from, to)` 被阻断时，`from` 无法把消息投递给 `to`。
    partitions: HashSet<(NodeID, NodeID)>,
    /// 随机丢包率 `[0.0, 1.0]`。
    drop_rate: f64,
    /// 简单乱序：缓存一条出站消息，下次再与新消息交换顺序发出。
    reorder: bool,
    held: HashMap<NodeID, Envelope>,
    /// 节点是否在线（stop 后为 false）。
    online: HashMap<NodeID, bool>,
    /// 各节点入站邮箱。
    mailboxes: HashMap<NodeID, Sender<Envelope>>,
}

/// 可故障注入的进程内传输。
#[derive(Clone, Default)]
pub struct Transport {
    inner: Arc<Mutex<TransportInner>>,
}

impl Transport {
    fn register(&self, id: NodeID, tx: Sender<Envelope>) {
        let mut g = self.inner.lock().expect("transport lock");
        g.mailboxes.insert(id, tx);
        g.online.insert(id, true);
    }

    fn set_online(&self, id: NodeID, online: bool) {
        let mut g = self.inner.lock().expect("transport lock");
        g.online.insert(id, online);
        if !online {
            g.held.remove(&id);
        }
    }

    fn deliver(&self, msg: Envelope) {
        let mut g = self.inner.lock().expect("transport lock");
        let from = msg.from;
        let to = msg.to;

        if !g.online.get(&from).copied().unwrap_or(false) {
            return;
        }
        if !g.online.get(&to).copied().unwrap_or(false) {
            return;
        }
        if g.partitions.contains(&(from, to)) {
            return;
        }
        if g.drop_rate > 0.0 && rand::rng().random::<f64>() < g.drop_rate {
            return;
        }

        if g.reorder {
            if let Some(prev) = g.held.remove(&to) {
                // 先发当前，再发缓存 → 乱序
                if let Some(tx) = g.mailboxes.get(&to) {
                    let _ = tx.try_send(msg);
                    let _ = tx.try_send(prev);
                }
                return;
            }
            g.held.insert(to, msg);
            return;
        }

        if let Some(tx) = g.mailboxes.get(&to) {
            let _ = tx.try_send(msg);
        }
    }

    /// 双向隔离 `a` 与 `b`。
    pub fn partition(&self, a: NodeID, b: NodeID) {
        let mut g = self.inner.lock().expect("transport lock");
        g.partitions.insert((a, b));
        g.partitions.insert((b, a));
    }

    /// 单向阻断 `from -> to`。
    pub fn partition_one_way(&self, from: NodeID, to: NodeID) {
        let mut g = self.inner.lock().expect("transport lock");
        g.partitions.insert((from, to));
    }

    /// 按组双向分区：`left` 内节点与 `right` 内节点互不可达。
    pub fn partition_groups(&self, left: &[NodeID], right: &[NodeID]) {
        let mut g = self.inner.lock().expect("transport lock");
        for &a in left {
            for &b in right {
                g.partitions.insert((a, b));
                g.partitions.insert((b, a));
            }
        }
    }

    /// 恢复 `a` 与 `b` 之间的连通（双向）。
    pub fn heal_pair(&self, a: NodeID, b: NodeID) {
        let mut g = self.inner.lock().expect("transport lock");
        g.partitions.remove(&(a, b));
        g.partitions.remove(&(b, a));
    }

    /// 清除全部分区，并冲刷乱序缓存。
    pub fn heal_all(&self) {
        let mut g = self.inner.lock().expect("transport lock");
        g.partitions.clear();
        let held = std::mem::take(&mut g.held);
        for (to, msg) in held {
            if let Some(tx) = g.mailboxes.get(&to) {
                let _ = tx.try_send(msg);
            }
        }
    }

    pub fn set_drop_rate(&self, rate: f64) {
        let mut g = self.inner.lock().expect("transport lock");
        g.drop_rate = rate.clamp(0.0, 1.0);
    }

    pub fn set_reorder(&self, on: bool) {
        let mut g = self.inner.lock().expect("transport lock");
        g.reorder = on;
        if !on {
            let held = std::mem::take(&mut g.held);
            for (to, msg) in held {
                if let Some(tx) = g.mailboxes.get(&to) {
                    let _ = tx.try_send(msg);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 客户端
// ---------------------------------------------------------------------------

type RequestTx = Sender<(Request, Sender<Result<Response>>)>;

/// 可向任意本地节点提交请求的客户端（跟随者会转发到领导者）。
#[derive(Clone)]
pub struct Client {
    request_txs: Arc<Mutex<HashMap<NodeID, RequestTx>>>,
    preferred: NodeID,
    attempts: u32,
    per_attempt_timeout: Duration,
    retry_sleep: Duration,
}

impl Client {
    fn new(request_txs: HashMap<NodeID, RequestTx>, preferred: NodeID) -> Self {
        Self {
            request_txs: Arc::new(Mutex::new(request_txs)),
            preferred,
            attempts: 40,
            per_attempt_timeout: Duration::from_millis(200),
            retry_sleep: Duration::from_millis(50),
        }
    }

    /// 注册或更新某节点的请求通道（节点 start 后调用）。
    pub fn register_node(&self, id: NodeID, tx: RequestTx) {
        self.request_txs.lock().expect("client lock").insert(id, tx);
    }

    pub fn unregister_node(&self, id: NodeID) {
        self.request_txs.lock().expect("client lock").remove(&id);
    }

    /// 提示下次优先向该节点发请求。
    pub fn preferred_hint(&mut self, id: NodeID) {
        self.preferred = id;
    }

    pub fn request(&mut self, request: Request) -> Result<Response> {
        let txs = self.request_txs.lock().expect("client lock").clone();
        let mut order: Vec<NodeID> = txs.keys().copied().collect();
        order.sort();
        if let Some(pos) = order.iter().position(|&id| id == self.preferred) {
            let id = order.remove(pos);
            order.insert(0, id);
        }

        let mut last_err = Error::Abort;
        for _ in 0..self.attempts {
            for &node_id in &order {
                let Some(tx) = txs.get(&node_id) else { continue };
                let (resp_tx, resp_rx) = channel::bounded(1);
                if tx.send((request.clone(), resp_tx)).is_err() {
                    continue;
                }
                match resp_rx.recv_timeout(self.per_attempt_timeout) {
                    Ok(Ok(resp)) => {
                        self.preferred = node_id;
                        return Ok(resp);
                    }
                    Ok(Err(Error::Abort)) => last_err = Error::Abort,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => last_err = Error::IO("request timed out".into()),
                }
            }
            thread::sleep(self.retry_sleep);
        }
        Err(last_err)
    }

    /// 只向指定节点发请求（仍在 Abort 时对该节点重试）。
    pub fn request_on(&mut self, node_id: NodeID, request: Request) -> Result<Response> {
        let txs = self.request_txs.lock().expect("client lock").clone();
        let Some(tx) = txs.get(&node_id).cloned() else {
            return Err(Error::IO(format!("node {node_id} not registered")));
        };
        let mut last_err = Error::Abort;
        for _ in 0..self.attempts {
            let (resp_tx, resp_rx) = channel::bounded(1);
            if tx.send((request.clone(), resp_tx)).is_err() {
                return Err(Error::IO(format!("node {node_id} request channel closed")));
            }
            match resp_rx.recv_timeout(self.per_attempt_timeout) {
                Ok(Ok(resp)) => {
                    self.preferred = node_id;
                    return Ok(resp);
                }
                Ok(Err(Error::Abort)) => last_err = Error::Abort,
                Ok(Err(e)) => return Err(e),
                Err(_) => last_err = Error::IO("request timed out".into()),
            }
            thread::sleep(self.retry_sleep);
        }
        Err(last_err)
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<Index> {
        let req = Request::Write(kv::encode(&Command::Put {
            key: key.into(),
            value: value.into(),
        }));
        match self.request(req)? {
            Response::Write(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                kv::Response::Put(index) => Ok(index),
                other => Err(Error::InvalidData(format!("unexpected write response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Write, got {other:?}"))),
        }
    }

    pub fn put_on(&mut self, node_id: NodeID, key: &str, value: &str) -> Result<Index> {
        let req = Request::Write(kv::encode(&Command::Put {
            key: key.into(),
            value: value.into(),
        }));
        match self.request_on(node_id, req)? {
            Response::Write(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                kv::Response::Put(index) => Ok(index),
                other => Err(Error::InvalidData(format!("unexpected write response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Write, got {other:?}"))),
        }
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let req = Request::Read(kv::encode(&Command::Get { key: key.into() }));
        match self.request(req)? {
            Response::Read(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                kv::Response::Get(v) => Ok(v),
                other => Err(Error::InvalidData(format!("unexpected read response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Read, got {other:?}"))),
        }
    }

    pub fn scan(&mut self) -> Result<std::collections::BTreeMap<String, String>> {
        let req = Request::Read(kv::encode(&Command::Scan));
        match self.request(req)? {
            Response::Read(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                kv::Response::Scan(map) => Ok(map),
                other => Err(Error::InvalidData(format!("unexpected scan response: {other:?}"))),
            },
            other => Err(Error::InvalidData(format!("expected Read, got {other:?}"))),
        }
    }

    pub fn status(&mut self) -> Result<Status> {
        match self.request(Request::Status)? {
            Response::Status(s) => Ok(s),
            other => Err(Error::InvalidData(format!("expected Status, got {other:?}"))),
        }
    }

    pub fn status_on(&mut self, node_id: NodeID) -> Result<Status> {
        match self.request_on(node_id, Request::Status)? {
            Response::Status(s) => Ok(s),
            other => Err(Error::InvalidData(format!("expected Status, got {other:?}"))),
        }
    }

    /// 变更集群成员（目标投票人集合）。需协议侧支持 `Request::ChangeMembership`。
    pub fn change_membership(&mut self, voters: HashSet<NodeID>) -> Result<Index> {
        match self.request(Request::ChangeMembership { voters })? {
            Response::ChangeMembership { index } => Ok(index),
            other => Err(Error::InvalidData(format!("expected ChangeMembership, got {other:?}"))),
        }
    }
}

/// 等待选出领导者。
pub fn wait_for_leader(client: &mut Client) -> Result<Status> {
    let mut last_err = Error::Abort;
    for _ in 0..100 {
        match client.status() {
            Ok(s) => return Ok(s),
            Err(Error::Abort) => {
                last_err = Error::Abort;
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err)
}

// ---------------------------------------------------------------------------
// 集群
// ---------------------------------------------------------------------------

struct NodeControl {
    stop_tx: Sender<()>,
}

/// 进程内多节点 Raft 集群。
pub struct Cluster {
    opts: Options,
    transport: Transport,
    nodes: HashMap<NodeID, NodeControl>,
    /// 逻辑成员集合（用于新节点 peers 计算）；成员变更协议生效前由测试/调用方维护。
    members: HashSet<NodeID>,
    client: Client,
}

impl Cluster {
    /// 使用默认快速测试选项启动集群。
    pub fn spawn(node_ids: &[NodeID]) -> Self {
        Self::spawn_with_options(node_ids, test_options())
    }

    pub fn spawn_with_options(node_ids: &[NodeID], opts: Options) -> Self {
        let transport = Transport::default();
        let mut request_txs = HashMap::new();
        let mut nodes = HashMap::new();
        let members: HashSet<NodeID> = node_ids.iter().copied().collect();

        for &id in node_ids {
            let (control, req_tx) = spawn_node(id, &members, opts.clone(), transport.clone());
            request_txs.insert(id, req_tx.clone());
            nodes.insert(id, control);
        }

        let preferred = node_ids.first().copied().unwrap_or(1);
        let client = Client::new(request_txs, preferred);

        Self { opts, transport, nodes, members, client }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub fn transport(&self) -> Transport {
        self.transport.clone()
    }

    pub fn members(&self) -> HashSet<NodeID> {
        self.members.clone()
    }

    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// 停止节点（模拟崩溃）：不再处理消息/请求，传输层视为离线。
    pub fn stop(&mut self, id: NodeID) {
        if let Some(ctrl) = self.nodes.remove(&id) {
            let _ = ctrl.stop_tx.send(());
            self.client.unregister_node(id);
            self.transport.set_online(id, false);
        }
    }

    /// 以空 BitCask 日志重新拉起节点（用于成员加入；新路径空库）。
    pub fn start(&mut self, id: NodeID) {
        if self.nodes.contains_key(&id) {
            return;
        }
        self.members.insert(id);
        let (control, req_tx) =
            spawn_node(id, &self.members, self.opts.clone(), self.transport.clone());
        self.client.register_node(id, req_tx);
        self.nodes.insert(id, control);
    }

    /// 仅更新本地成员集合视图（协议提交成员变更后由测试调用）。
    pub fn set_members(&mut self, members: HashSet<NodeID>) {
        self.members = members;
    }

    pub fn partition(&self, a: NodeID, b: NodeID) {
        self.transport.partition(a, b);
    }

    pub fn partition_groups(&self, left: &[NodeID], right: &[NodeID]) {
        self.transport.partition_groups(left, right);
    }

    pub fn heal_all(&self) {
        self.transport.heal_all();
    }

    pub fn set_drop_rate(&self, rate: f64) {
        self.transport.set_drop_rate(rate);
    }

    pub fn set_reorder(&self, on: bool) {
        self.transport.set_reorder(on);
    }

    pub fn is_running(&self, id: NodeID) -> bool {
        self.nodes.contains_key(&id)
    }
}

/// 测试/演示用较快超时。
pub fn test_options() -> Options {
    Options {
        heartbeat_interval: 2,
        election_timeout_range: 5..10,
        max_append_entries: 100,
        // 集成测试默认开启；若 flaky 可在具体用例里覆盖。
        pre_vote: true,
        check_quorum: true,
        snapshot_threshold: 0,
    }
}

fn spawn_node(
    id: NodeID,
    members: &HashSet<NodeID>,
    opts: Options,
    transport: Transport,
) -> (NodeControl, RequestTx) {
    let peers: HashSet<NodeID> = members.iter().copied().filter(|&p| p != id).collect();
    let (inbox_tx, inbox_rx) = channel::unbounded();
    transport.register(id, inbox_tx);

    let (request_tx, request_rx) = channel::unbounded();
    let (stop_tx, stop_rx) = channel::bounded(1);
    let (node_tx, node_rx) = channel::unbounded();

    // 并行测试会同时打开多个 BitCask；路径必须全局唯一以免文件锁冲突。
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "raft-cluster-{}-{}-{}-{}.log",
        std::process::id(),
        id,
        seq,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let log = Log::new(Box::new(BitCask::new(path).expect("bitcask"))).expect("log");
    let node = Node::new(id, peers, log, Kv::new(), node_tx, opts).expect("node");

    let transport_out = transport.clone();
    thread::spawn(move || {
        run_node(node, inbox_rx, node_rx, request_rx, stop_rx, transport_out);
    });

    (NodeControl { stop_tx }, request_tx)
}

fn run_node(
    mut node: Node,
    peers_rx: Receiver<Envelope>,
    node_rx: Receiver<Envelope>,
    request_rx: Receiver<(Request, Sender<Result<Response>>)>,
    stop_rx: Receiver<()>,
    transport: Transport,
) {
    let ticker = channel::tick(TICK_INTERVAL);
    let mut response_txs: HashMap<RequestID, Sender<Result<Response>>> = HashMap::new();
    let node_id = node.id();

    loop {
        crossbeam::select! {
            recv(stop_rx) -> _ => break,

            recv(ticker) -> _ => {
                node = match node.tick() {
                    Ok(n) => n,
                    Err(_) => break,
                };
            }

            recv(peers_rx) -> msg => {
                let Ok(msg) = msg else { break };
                node = match node.step(msg) {
                    Ok(n) => n,
                    Err(_) => break,
                };
            }

            recv(node_rx) -> msg => {
                let Ok(msg) = msg else { break };
                if msg.to == node_id {
                    if let Message::ClientResponse { id, response } = msg.message {
                        if let Some(tx) = response_txs.remove(&id) {
                            let _ = tx.send(response);
                        }
                    }
                    continue;
                }
                transport.deliver(msg);
            }

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
                    Err(_) => break,
                };
            }
        }
    }

    transport.set_online(node_id, false);
}
