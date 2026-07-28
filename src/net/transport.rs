//! TCP 监听与出站 peer 发送。

// peer 地址表与连接缓存
use std::collections::HashMap;
// 按连接缓冲读写
use std::io::{BufReader, BufWriter};
// 监听与已连接流
use std::net::{SocketAddr, TcpListener, TcpStream};
// 跨线程共享连接表与 writer
use std::sync::{Arc, Mutex};
// 每连接一线程处理
use std::thread;
// 连接/读写超时
use std::time::Duration;

// 入站事件投递给 Raft 驱动线程
use crossbeam::channel::Sender;
// 连接生命周期日志
use log::{debug, info, warn};
// 客户端请求关联 ID
use uuid::Uuid;

// 帧读写与线路消息
use super::codec::{read_msg, write_msg, WireMsg};
// 网络错误映射
use crate::error::{Error, Result};
// Raft 信封与客户端请求/响应
use crate::raft::{Envelope, NodeID, Request, Response};

/// 向已知 peer 发送 Raft 信封（懒连接 + 失败重试一次）。
// Clone 便于多个发送点共享同一出站连接池
#[derive(Clone)]
// 出站邮箱：地址表 + 复用连接池
pub struct PeerOutbox {
    // 节点 ID → 静态配置地址（启动时固定）
    addrs: Arc<HashMap<NodeID, SocketAddr>>,
    // 节点 ID → 复用的 TCP 连接（写失败后剔除）
    conns: Arc<Mutex<HashMap<NodeID, TcpStream>>>,
// PeerOutbox 字段定义结束
}

// 出站发送与连接管理实现
impl PeerOutbox {
    // 用配置中的 peer 列表构造出站邮箱
    pub fn new(peers: Vec<(NodeID, SocketAddr)>) -> Self {
        // 初始化地址表与空连接池
        Self {
            // 列表转 HashMap，便于 O(1) 查地址
            addrs: Arc::new(peers.into_iter().collect()),
            // 初始无连接，首次发送时懒建立
            conns: Arc::new(Mutex::new(HashMap::new())),
        // Self 构造结束
        }
    // new 结束
    }

    // 发送一条 Raft 信封；失败则清连接重试一次
    pub fn send_raft(&self, env: Envelope) -> Result<()> {
        // 目标节点（信封已带 to）
        let to = env.to;
        // 包装为线路层 Raft 消息
        let msg = WireMsg::Raft(env);
        // 首次尝试
        if let Err(e) = self.send_to(to, &msg) {
            // 断线后清连接再试一次。
            self.invalidate(to);
            // 重试仍失败则返回第二次错误
            if let Err(e2) = self.send_to(to, &msg) {
                // 再次失败，丢弃坏连接
                self.invalidate(to);
                // 向上抛出最终错误
                return Err(e2);
            // 二次重试分支结束
            }
            // 抑制首次错误的 unused 告警（已重试成功）
            let _ = e;
        // 首次失败分支结束
        }
        // 首次或重试成功
        Ok(())
    // send_raft 结束
    }

    // 向指定 peer 写一条消息，必要时懒连接
    fn send_to(&self, id: NodeID, msg: &WireMsg) -> Result<()> {
        // 查配置地址；未知 ID 视为配置/路由错误
        let addr = self
            // 从地址表按节点 ID 查询
            .addrs
            // 取引用
            .get(&id)
            // 复制 SocketAddr 所有权
            .copied()
            // 未配置 peer 则报 IO 路由错误
            .ok_or_else(|| Error::IO(format!("unknown peer {id}")))?;
        // 锁定连接表
        let mut guard = self.conns.lock().expect("lock");
        // 无缓存连接则建立
        if !guard.contains_key(&id) {
            // 按配置地址建立新 TCP
            let stream = Self::connect(addr)?;
            // 放入连接池供后续复用
            guard.insert(id, stream);
        // 懒连接分支结束
        }
        // 取出可变引用写入
        let stream = guard.get_mut(&id).unwrap();
        // 写帧；失败则移除连接以便下次重连
        match write_msg(stream, msg) {
            // 写成功直接返回
            Ok(()) => Ok(()),
            // 写失败进入清理路径
            Err(e) => {
                // 写失败时丢掉连接，下次懒重连。
                guard.remove(&id);
                // 把原始写错误继续向上抛
                Err(e)
            // Err 分支结束
            }
        // match write_msg 结束
        }
    // send_to 结束
    }

    // 带超时建立 TCP，并设置 nodelay/读写超时
    fn connect(addr: SocketAddr) -> Result<TcpStream> {
        // 2 秒连接超时，避免领导发送路径长时间卡住
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            // 连接失败附带目标地址信息
            .map_err(|e| Error::IO(format!("connect {addr}: {e}")))?;
        // 关闭 Nagle，降低小包延迟（心跳/投票）
        stream.set_nodelay(true).ok();
        // 避免对端卡住时 write 永久阻塞领导发送路径。
        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
        // 读超时：本 outbox 主要写，但设置以免异常路径阻塞
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
        // 返回已配置超时的流
        Ok(stream)
    // connect 结束
    }

    /// 主动丢弃某 peer 的缓存连接（例如连续发送失败时）。
    pub fn invalidate(&self, id: NodeID) {
        // 从连接池移除，下次 send 会重新 connect
        self.conns.lock().expect("lock").remove(&id);
    // invalidate 结束
    }
// PeerOutbox impl 结束
}

/// 入站事件：Raft 信封，或客户端请求（需异步回 ClientReply）。
pub enum Inbound {
    // 来自 peer 的 Raft 协议消息，交给 Node::step
    Raft(Envelope),
    // 来自 CLI/客户端的业务请求；处理完后通过 reply 回写
    Client {
        // 与 ClientReply 匹配的请求 ID
        id: Uuid,
        // 客户端请求体
        request: Request,
        // 单次响应通道，由连接处理线程阻塞等待
        reply: Sender<std::result::Result<Response, Error>>,
    // Client 变体字段结束
    },
// Inbound 枚举结束
}

/// 在后台接受连接，把解码后的消息送入 `inbound_tx`。
pub fn spawn_listener(addr: SocketAddr, inbound_tx: Sender<Inbound>) -> Result<()> {
    // 绑定监听地址
    let listener = TcpListener::bind(addr).map_err(|e| Error::IO(format!("bind {addr}: {e}")))?;
    // 便于运维确认节点已就绪
    info!("Listening on {addr}");
    // 独立 accept 线程，避免阻塞 Raft tick 循环
    thread::spawn(move || {
        // 持续接受连接
        for conn in listener.incoming() {
            // 区分 accept 成功与失败
            match conn {
                // 新连接建立成功
                Ok(stream) => {
                    // 关闭 Nagle
                    stream.set_nodelay(true).ok();
                    // 读超时 30s：空闲连接最终会退出 handle_conn
                    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
                    // 写超时 2s：避免回写 ClientReply 卡死
                    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
                    // 克隆 inbound 发送端给连接线程
                    let tx = inbound_tx.clone();
                    // 每连接一线程，简化同步模型
                    thread::spawn(move || handle_conn(stream, tx));
                // Ok 分支结束
                }
                // accept 失败只告警，继续监听
                Err(e) => warn!("accept error: {e}"),
            // match conn 结束
            }
        // for incoming 结束
        }
    // accept 线程闭包结束
    });
    // 监听线程已启动，主路径立即返回
    Ok(())
// spawn_listener 结束
}

// 单连接读写循环：Raft 直接转发；Client 同步等待响应再回写
fn handle_conn(stream: TcpStream, inbound_tx: Sender<Inbound>) {
    // 读半边用 BufReader
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    // 写半边共享给可能的超时回写路径
    let writer = Arc::new(Mutex::new(BufWriter::new(stream)));
    // 直到读失败/通道关闭
    loop {
        // 读一帧
        let msg = match read_msg(&mut reader) {
            // 解码成功得到业务消息
            Ok(m) => m,
            // 读/解码失败：对端关闭或超时
            Err(e) => {
                // 对端关闭或超时
                debug!("connection closed: {e}");
                // 退出本连接循环
                break;
            // Err 分支结束
            }
        // match read_msg 结束
        };
        // 按消息类型分发
        match msg {
            // Raft 消息：投递给驱动线程
            WireMsg::Raft(env) => {
                // 驱动线程已退出则结束连接
                if inbound_tx.send(Inbound::Raft(env)).is_err() {
                    // 入站通道已关，停止读循环
                    break;
                // send 失败分支结束
                }
            // Raft 分支结束
            }
            // 客户端请求：建立 reply 通道并等待处理结果
            WireMsg::Client { id, request } => {
                // 容量 1：一次请求一次响应
                let (reply_tx, reply_rx) = crossbeam::channel::bounded(1);
                // 投递到 Raft 线程；失败则断连
                if inbound_tx
                    // 封装为 Inbound::Client 投递给驱动
                    .send(Inbound::Client { id, request, reply: reply_tx })
                    // 通道关闭视为节点关闭
                    .is_err()
                // if 条件后进入失败体
                {
                    // 驱动已退出，关闭本连接
                    break;
                // send 失败分支结束
                }
                // 等待 Raft 线程处理完再写回（阻塞本连接线程，简单可靠）。
                match reply_rx.recv_timeout(Duration::from_secs(30)) {
                    // 正常拿到响应，写 ClientReply
                    Ok(response) => {
                        // 独占写半边，串行回写
                        let mut w = writer.lock().expect("writer");
                        // 写失败则关闭连接
                        if write_msg(&mut *w, &WireMsg::ClientReply { id, response }).is_err() {
                            // 对端写失败，结束连接
                            break;
                        // write 失败分支结束
                        }
                    // Ok 响应分支结束
                    }
                    // 超时：向客户端返回 IO 超时错误
                    Err(_) => {
                        // 超时仍尝试回写错误响应
                        let mut w = writer.lock().expect("writer");
                        // 忽略回写失败（连接可能已断）
                        let _ = write_msg(
                            // 解引用 Mutex 得到可写 BufWriter
                            &mut *w,
                            // 构造超时错误的 ClientReply
                            &WireMsg::ClientReply {
                                // 与请求相同的关联 ID
                                id,
                                // 30s 内未完成（选举/复制慢或领导者不可达）
                                response: Err(Error::IO("request timed out".into())),
                            // ClientReply 构造结束
                            },
                        // write_msg 调用结束
                        );
                    // 超时分支结束
                    }
                // match recv_timeout 结束
                }
            // Client 请求分支结束
            }
            // 服务端入站不应出现 ClientReply
            WireMsg::ClientReply { .. } => {
                // 服务端不应收到
            }
        // match msg 结束
        }
    // 连接读循环结束
    }
// handle_conn 结束
}

/// CLI：向单个 peer 发送客户端请求并等待响应。
pub fn run_client_request(
    // 目标节点地址
    addr: SocketAddr,
    // 业务请求
    request: Request,
    // 等待响应超时
    timeout: Duration,
// 返回业务响应或网络/协议错误
) -> Result<Response> {
    // 建立短连接
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        // 连接失败附带地址
        .map_err(|e| Error::IO(format!("connect {addr}: {e}")))?;
    // 关闭 Nagle
    stream.set_nodelay(true).ok();
    // 读超时使用调用方指定值
    stream.set_read_timeout(Some(timeout)).ok();
    // 写超时同样限制
    stream.set_write_timeout(Some(timeout)).ok();
    // 可变以便 write_msg/read_msg
    let mut stream = stream;
    // 生成本次请求关联 ID
    let id = Uuid::new_v4();
    // 发送 Client 帧
    write_msg(&mut stream, &WireMsg::Client { id, request })?;
    // 读取并校验响应
    match read_msg(&mut stream)? {
        // ID 匹配则解包 Result
        WireMsg::ClientReply { id: rid, response } if rid == id => response,
        // 类型或 ID 不符视为协议错误
        other => Err(Error::InvalidData(format!("unexpected reply: {other:?}"))),
    // match 响应结束
    }
// run_client_request 结束
}
