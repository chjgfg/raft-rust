//! Raft 节点服务进程（单节点或多节点中的一员）。
//!
//! ```text
//! # 单节点（立即成为领导者）
//! cargo run --bin raft-node -- --config config/single.yaml
//!
//! # 三节点集群中的节点 1
//! cargo run --bin raft-node -- --config config/node1.yaml
//! ```

// 节点 ID 集合与客户端回复路由表
use std::collections::{HashMap, HashSet};
// 解析命令行配置路径
use std::env;
// 配置文件路径类型
use std::path::PathBuf;

// 无界通道与定时 tick，驱动主事件循环
use crossbeam::channel;
// 节点运行期日志：加载配置、恢复快照、错误与出站失败
use log::{error, info, warn};
// 从 YAML 加载本节点与对等节点配置
use raft_rust::config::NodeFileConfig;
// 统一错误类型与 Result
use raft_rust::error::{Error, Result};
// TCP 入站监听与对等节点出站邮箱
use raft_rust::net::{self, Inbound, PeerOutbox};
// 状态机：键值存储
use raft_rust::raft::kv::Kv;
// 会话状态：包装 KV 并支持快照恢复
use raft_rust::raft::session::SessionState;
// Raft 核心类型：信封、日志、消息、节点与 tick 间隔
use raft_rust::raft::{
    // Raft 核心类型导入列表续行
    Envelope, Key, Log, Message, Node, NodeID, RequestID, Response, TICK_INTERVAL,
};
// 持久化引擎：BitCask 存日志与快照
use raft_rust::storage::{BitCask, Engine};
// 为客户端请求生成唯一 RequestID
use uuid::Uuid;

// 进程入口：初始化日志并运行节点主逻辑
fn main() {
    // 默认 info 级别，便于观察多进程启动与协议事件
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // 业务失败时打印错误并以非 0 退出，便于进程管理器感知
    if let Err(e) = run() {
        // 向 stderr 打印致命错误
        eprintln!("raft-node error: {e}");
        // 非 0 退出供进程管理器感知失败
        std::process::exit(1);
    }
}

// 单进程节点完整生命周期：配置 → 恢复 → 网络 → 主循环
fn run() -> Result<()> {
    // 解析 --config / -c，得到本节点 YAML 路径
    let config_path = parse_config_path();
    // 记录关键运行事件
    info!("Loading {}", config_path.display());
    // 加载节点 ID、数据目录、监听地址与对等节点列表
    let cfg = NodeFileConfig::load(&config_path)?;
    // 将文件中的 Raft 选项转为运行时 Options
    let opts = cfg.options.to_options();
    // 本节点在集群中的稳定 NodeID
    let id = cfg.node.id;
    // 本节点 TCP 监听地址（入站 Raft/客户端连接）
    let listen = cfg.listen_addr()?;
    // 对等节点 ID → 地址映射，用于出站
    let peers_addr = cfg.peer_addrs()?;
    // 对等节点 ID 集合，交给 Node 做成员视图
    let peer_ids: HashSet<NodeID> = peers_addr.iter().map(|(i, _)| *i).collect();

    // 确保数据目录存在，供日志与快照持久化
    std::fs::create_dir_all(&cfg.node.data_dir).map_err(|e| Error::IO(e.to_string()))?;
    // 打开 BitCask 引擎，底层文件为 bitcask.log
    let mut engine = BitCask::new(cfg.node.data_dir.join("bitcask.log"))?;
    // 与 raft::log::Key::SnapshotData 编码一致：单字节 0x04。
    // 启动时尝试读出已持久化的快照字节，用于状态机恢复
    let snap_bytes = engine.get(&Key::SnapshotData.encode())?;
    // 用同一引擎构造 Raft 日志（任期、条目、提交索引等）
    let log = Log::new(Box::new(engine))?;
    // 空会话状态机，后续若有快照会覆盖
    let mut state = SessionState::new(Kv::new());
    // 若磁盘上存在快照数据则恢复应用状态
    if let Some(bytes) = snap_bytes {
        // 从日志元数据取快照对应的已应用索引
        let (idx, _) = log.get_snapshot_meta();
        // 仅当快照索引有效时才 restore，避免空快照误恢复
        if idx > 0 {
            // 引入 State trait 以调用 restore
            use raft_rust::raft::State;
            // 将快照字节灌入状态机，对齐 last_applied
            state.restore(&bytes, idx)?;
            // 记录关键运行事件
            info!("Restored snapshot at index {idx}");
        }
    }

    // node_tx：Node 内部产出待发送/本地投递的 Envelope
    let (node_tx, node_rx) = channel::unbounded();
    // 构造 Raft 节点：本 ID、对等集合、日志、状态机、出站通道与选项
    let mut node = Node::new(id, peer_ids, log, state, node_tx, opts)?;

    // inbound_tx：监听线程把 Raft 信封与客户端请求送入主循环
    let (inbound_tx, inbound_rx) = channel::unbounded();
    // 在 listen 地址启动 TCP 监听线程，接收对等与客户端连接
    net::spawn_listener(listen, inbound_tx)?;

    // 按对等地址建立/复用出站连接，发送 Raft 消息
    let outbox = PeerOutbox::new(peers_addr);
    // 按协议 tick 间隔驱动选举超时与心跳
    let ticker = channel::tick(TICK_INTERVAL);

    // 待回复的客户端：RequestID -> reply channel
    // 客户端请求经本地 step 后，响应经 node_rx 回填到对应 reply
    let mut client_replies: HashMap<RequestID, channel::Sender<std::result::Result<Response, Error>>> =
        // 初始化空的客户端回复路由表
        HashMap::new();

    // 日志用：列出配置中的对等节点 ID
    let peer_list: Vec<_> = cfg.peers.iter().map(|p| p.id).collect();
    // 无对等则单节点模式（可立即成为 Leader）
    if peer_list.is_empty() {
        // 记录关键运行事件
        info!("Node {id} started in SINGLE-NODE mode (no peers), listening on {listen}");
    // 否则分支
    } else {
        // 多进程集群：本节点与 peers 一起形成法定人数
        info!("Node {id} started, peers={peer_list:?}, listening on {listen}");
    }

    // 主事件循环：tick / 入站消息 / 节点出站 三路 select
    loop {
        // 多路复用 tick / 入站 / 节点出站
        crossbeam::select! {
            // 定时 tick：推进选举时钟、心跳与可能的状态转换
            recv(ticker) -> _ => {
                // 推进逻辑时钟，可能触发选举或心跳
                node = match node.tick() {
                    // 成功则采用新的 Node 状态
                    Ok(n) => n,
                    // 失败则记录并结束主循环
                    Err(e) => {
                        // 记录错误并准备退出主循环
                        error!("tick error: {e}");
                        // tick 失败则退出主循环，进程结束
                        break;
                    }
                };
            }
            // 网络入站：对等 Raft 消息或客户端读写请求
            recv(inbound_rx) -> msg => {
                // 监听线程退出或通道关闭时结束节点
                let Ok(msg) = msg else { break };
                // 区分对等 Raft 消息与客户端请求
                match msg {
                    // 对等节点发来的 Raft 信封，直接 step 进状态机
                    Inbound::Raft(env) => {
                        // 将消息送入 Raft 状态机，可能切换角色
                        node = match node.step(env) {
                            // 成功则采用新的 Node 状态
                            Ok(n) => n,
                            // 失败则记录并结束主循环
                            Err(e) => {
                                // 记录错误并准备退出主循环
                                error!("step error: {e}");
                                // 退出主循环，结束进程生命周期
                                break;
                            }
                        };
                    }
                    // 客户端请求：生成 RequestID，封装为本地 ClientRequest 再 step
                    Inbound::Client { id: _wire_id, request, reply } => {
                        // 业务侧唯一请求 ID，用于匹配异步 ClientResponse
                        let req_id = Uuid::new_v4();
                        // 自环信封：from/to 均为本节点，由 Node 当 Leader 处理或拒绝
                        let env = Envelope {
                            // 自环请求：来源为本节点
                            from: node.id(),
                            // 自环请求：目标为本节点
                            to: node.id(),
                            // 带上当前任期，满足 Envelope 协议字段
                            term: node.term(),
                            // 封装为本地 ClientRequest 供 Node 处理
                            message: Message::ClientRequest { id: req_id, request },
                        };
                        // 登记 reply 通道，等后续 ClientResponse 回写客户端
                        client_replies.insert(req_id, reply);
                        // 将消息送入 Raft 状态机，可能切换角色
                        node = match node.step(env) {
                            // 成功则采用新的 Node 状态
                            Ok(n) => n,
                            // 失败则记录并结束主循环
                            Err(e) => {
                                // 记录错误并准备退出主循环
                                error!("client step error: {e}");
                                // 退出主循环，结束进程生命周期
                                break;
                            }
                        };
                    }
                }
            }
            // Node 产出的出站/本地消息：本机客户端回复或发往对等节点
            recv(node_rx) -> msg => {
                // 节点内部通道关闭则退出
                let Ok(msg) = msg else { break };
                // 目标为本节点：通常是客户端响应，回填给等待中的 TCP 客户端
                if msg.to == id {
                    // 本地目标消息中的客户端响应
                    if let Message::ClientResponse { id: rid, response } = msg.message {
                        // 按 RequestID 取出发送端并投递结果
                        if let Some(tx) = client_replies.remove(&rid) {
                            // 回写 TCP 客户端；接收端已关则忽略
                            let _ = tx.send(response);
                        }
                    }
                    // 本地消息处理完毕，不走对等出站
                    continue;
                }
                // 目标为其他节点：经 PeerOutbox TCP 发出
                if let Err(e) = outbox.send_raft(msg) {
                    // 对等不可达时仅告警，不中断本节点（网络抖动可恢复）
                    warn!("send to peer failed: {e}");
                }
            }
        }
    }
    // 主循环结束后正常返回
    Ok(())
}

// 从 argv 解析配置文件路径，支持 --config PATH / -c PATH / --config=PATH
fn parse_config_path() -> PathBuf {
    // 跳过程序名，遍历后续参数
    let mut args = env::args().skip(1);
    // 遍历命令行参数查找配置路径
    while let Some(arg) = args.next() {
        // 短/长选项后跟独立路径参数
        if arg == "--config" || arg == "-c" {
            // 读取选项后的路径参数
            if let Some(p) = args.next() {
                // 使用用户指定的配置文件
                return PathBuf::from(p);
            }
        // 等号形式：--config=path/to.yaml
        } else if let Some(p) = arg.strip_prefix("--config=") {
            // 使用用户指定的配置文件
            return PathBuf::from(p);
        }
    }
    // 未指定时默认 node1，便于本地多进程联调
    PathBuf::from("config/node1.yaml")
}
