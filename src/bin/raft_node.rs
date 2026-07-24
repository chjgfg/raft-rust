//! Raft 节点服务进程（单节点或多节点中的一员）。
//!
//! ```text
//! # 单节点（立即成为领导者）
//! cargo run --bin raft-node -- --config config/single.yaml
//!
//! # 三节点集群中的节点 1
//! cargo run --bin raft-node -- --config config/node1.yaml
//! ```

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;

use crossbeam::channel;
use log::{error, info, warn};
use raft_rust::config::NodeFileConfig;
use raft_rust::error::{Error, Result};
use raft_rust::net::{self, Inbound, PeerOutbox};
use raft_rust::raft::kv::Kv;
use raft_rust::raft::session::SessionState;
use raft_rust::raft::{
    Envelope, Key, Log, Message, Node, NodeID, RequestID, Response, TICK_INTERVAL,
};
use raft_rust::storage::{BitCask, Engine};
use uuid::Uuid;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if let Err(e) = run() {
        eprintln!("raft-node error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config_path = parse_config_path();
    info!("Loading {}", config_path.display());
    let cfg = NodeFileConfig::load(&config_path)?;
    let opts = cfg.options.to_options();
    let id = cfg.node.id;
    let listen = cfg.listen_addr()?;
    let peers_addr = cfg.peer_addrs()?;
    let peer_ids: HashSet<NodeID> = peers_addr.iter().map(|(i, _)| *i).collect();

    std::fs::create_dir_all(&cfg.node.data_dir).map_err(|e| Error::IO(e.to_string()))?;
    let mut engine = BitCask::new(cfg.node.data_dir.join("bitcask.log"))?;
    // 与 raft::log::Key::SnapshotData 编码一致：单字节 0x04。
    let snap_bytes = engine.get(&Key::SnapshotData.encode())?;
    let log = Log::new(Box::new(engine))?;
    let mut state = SessionState::new(Kv::new());
    if let Some(bytes) = snap_bytes {
        let (idx, _) = log.get_snapshot_meta();
        if idx > 0 {
            use raft_rust::raft::State;
            state.restore(&bytes, idx)?;
            info!("Restored snapshot at index {idx}");
        }
    }

    let (node_tx, node_rx) = channel::unbounded();
    let mut node = Node::new(id, peer_ids, log, state, node_tx, opts)?;

    let (inbound_tx, inbound_rx) = channel::unbounded();
    net::spawn_listener(listen, inbound_tx)?;

    let outbox = PeerOutbox::new(peers_addr);
    let ticker = channel::tick(TICK_INTERVAL);

    // 待回复的客户端：RequestID -> reply channel
    let mut client_replies: HashMap<RequestID, channel::Sender<std::result::Result<Response, Error>>> =
        HashMap::new();

    let peer_list: Vec<_> = cfg.peers.iter().map(|p| p.id).collect();
    if peer_list.is_empty() {
        info!("Node {id} started in SINGLE-NODE mode (no peers), listening on {listen}");
    } else {
        info!("Node {id} started, peers={peer_list:?}, listening on {listen}");
    }

    loop {
        crossbeam::select! {
            recv(ticker) -> _ => {
                node = match node.tick() {
                    Ok(n) => n,
                    Err(e) => {
                        error!("tick error: {e}");
                        break;
                    }
                };
            }
            recv(inbound_rx) -> msg => {
                let Ok(msg) = msg else { break };
                match msg {
                    Inbound::Raft(env) => {
                        node = match node.step(env) {
                            Ok(n) => n,
                            Err(e) => {
                                error!("step error: {e}");
                                break;
                            }
                        };
                    }
                    Inbound::Client { id: _wire_id, request, reply } => {
                        let req_id = Uuid::new_v4();
                        let env = Envelope {
                            from: node.id(),
                            to: node.id(),
                            term: node.term(),
                            message: Message::ClientRequest { id: req_id, request },
                        };
                        client_replies.insert(req_id, reply);
                        node = match node.step(env) {
                            Ok(n) => n,
                            Err(e) => {
                                error!("client step error: {e}");
                                break;
                            }
                        };
                    }
                }
            }
            recv(node_rx) -> msg => {
                let Ok(msg) = msg else { break };
                if msg.to == id {
                    if let Message::ClientResponse { id: rid, response } = msg.message {
                        if let Some(tx) = client_replies.remove(&rid) {
                            let _ = tx.send(response);
                        }
                    }
                    continue;
                }
                if let Err(e) = outbox.send_raft(msg) {
                    warn!("send to peer failed: {e}");
                }
            }
        }
    }
    Ok(())
}

fn parse_config_path() -> PathBuf {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            if let Some(p) = args.next() {
                return PathBuf::from(p);
            }
        } else if let Some(p) = arg.strip_prefix("--config=") {
            return PathBuf::from(p);
        }
    }
    PathBuf::from("config/node1.yaml")
}
