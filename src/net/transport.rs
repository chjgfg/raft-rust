//! TCP 监听与出站 peer 发送。

use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::Sender;
use log::{debug, info, warn};
use uuid::Uuid;

use super::codec::{read_msg, write_msg, WireMsg};
use crate::error::{Error, Result};
use crate::raft::{Envelope, NodeID, Request, Response};

/// 向已知 peer 发送 Raft 信封（懒连接 + 失败重试一次）。
#[derive(Clone)]
pub struct PeerOutbox {
    addrs: Arc<HashMap<NodeID, SocketAddr>>,
    conns: Arc<Mutex<HashMap<NodeID, TcpStream>>>,
}

impl PeerOutbox {
    pub fn new(peers: Vec<(NodeID, SocketAddr)>) -> Self {
        Self {
            addrs: Arc::new(peers.into_iter().collect()),
            conns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn send_raft(&self, env: Envelope) -> Result<()> {
        let to = env.to;
        let msg = WireMsg::Raft(env);
        if let Err(e) = self.send_to(to, &msg) {
            // 断线后清连接再试一次。
            self.invalidate(to);
            if let Err(e2) = self.send_to(to, &msg) {
                self.invalidate(to);
                return Err(e2);
            }
            let _ = e;
        }
        Ok(())
    }

    fn send_to(&self, id: NodeID, msg: &WireMsg) -> Result<()> {
        let addr = self
            .addrs
            .get(&id)
            .copied()
            .ok_or_else(|| Error::IO(format!("unknown peer {id}")))?;
        let mut guard = self.conns.lock().expect("lock");
        if !guard.contains_key(&id) {
            let stream = Self::connect(addr)?;
            guard.insert(id, stream);
        }
        let stream = guard.get_mut(&id).unwrap();
        match write_msg(stream, msg) {
            Ok(()) => Ok(()),
            Err(e) => {
                // 写失败时丢掉连接，下次懒重连。
                guard.remove(&id);
                Err(e)
            }
        }
    }

    fn connect(addr: SocketAddr) -> Result<TcpStream> {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            .map_err(|e| Error::IO(format!("connect {addr}: {e}")))?;
        stream.set_nodelay(true).ok();
        // 避免对端卡住时 write 永久阻塞领导发送路径。
        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
        Ok(stream)
    }

    /// 主动丢弃某 peer 的缓存连接（例如连续发送失败时）。
    pub fn invalidate(&self, id: NodeID) {
        self.conns.lock().expect("lock").remove(&id);
    }
}

/// 入站事件：Raft 信封，或客户端请求（需异步回 ClientReply）。
pub enum Inbound {
    Raft(Envelope),
    Client {
        id: Uuid,
        request: Request,
        reply: Sender<std::result::Result<Response, Error>>,
    },
}

/// 在后台接受连接，把解码后的消息送入 `inbound_tx`。
pub fn spawn_listener(addr: SocketAddr, inbound_tx: Sender<Inbound>) -> Result<()> {
    let listener = TcpListener::bind(addr).map_err(|e| Error::IO(format!("bind {addr}: {e}")))?;
    info!("Listening on {addr}");
    thread::spawn(move || {
        for conn in listener.incoming() {
            match conn {
                Ok(stream) => {
                    stream.set_nodelay(true).ok();
                    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
                    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
                    let tx = inbound_tx.clone();
                    thread::spawn(move || handle_conn(stream, tx));
                }
                Err(e) => warn!("accept error: {e}"),
            }
        }
    });
    Ok(())
}

fn handle_conn(stream: TcpStream, inbound_tx: Sender<Inbound>) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let writer = Arc::new(Mutex::new(BufWriter::new(stream)));
    loop {
        let msg = match read_msg(&mut reader) {
            Ok(m) => m,
            Err(e) => {
                debug!("connection closed: {e}");
                break;
            }
        };
        match msg {
            WireMsg::Raft(env) => {
                if inbound_tx.send(Inbound::Raft(env)).is_err() {
                    break;
                }
            }
            WireMsg::Client { id, request } => {
                let (reply_tx, reply_rx) = crossbeam::channel::bounded(1);
                if inbound_tx
                    .send(Inbound::Client { id, request, reply: reply_tx })
                    .is_err()
                {
                    break;
                }
                // 等待 Raft 线程处理完再写回（阻塞本连接线程，简单可靠）。
                match reply_rx.recv_timeout(Duration::from_secs(30)) {
                    Ok(response) => {
                        let mut w = writer.lock().expect("writer");
                        if write_msg(&mut *w, &WireMsg::ClientReply { id, response }).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        let mut w = writer.lock().expect("writer");
                        let _ = write_msg(
                            &mut *w,
                            &WireMsg::ClientReply {
                                id,
                                response: Err(Error::IO("request timed out".into())),
                            },
                        );
                    }
                }
            }
            WireMsg::ClientReply { .. } => {
                // 服务端不应收到
            }
        }
    }
}

/// CLI：向单个 peer 发送客户端请求并等待响应。
pub fn run_client_request(
    addr: SocketAddr,
    request: Request,
    timeout: Duration,
) -> Result<Response> {
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .map_err(|e| Error::IO(format!("connect {addr}: {e}")))?;
    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    let mut stream = stream;
    let id = Uuid::new_v4();
    write_msg(&mut stream, &WireMsg::Client { id, request })?;
    match read_msg(&mut stream)? {
        WireMsg::ClientReply { id: rid, response } if rid == id => response,
        other => Err(Error::InvalidData(format!("unexpected reply: {other:?}"))),
    }
}
