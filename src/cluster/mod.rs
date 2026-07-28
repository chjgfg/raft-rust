//! 进程内 Raft 集群运行时：channel 传输、客户端与故障注入。
//!
//! 供 `main`、example 与集成测试复用。不包含真实网络或磁盘持久化。

// 哈希集合：投票人/成员/分区边
use std::collections::{HashMap, HashSet};
// 跨线程共享可变传输与客户端状态
use std::sync::{Arc, Mutex};
// 节点线程与客户端重试休眠
use std::thread;
// 超时与退避间隔
use std::time::Duration;

// 进程内消息与请求应答通道
use crossbeam::channel::{self, Receiver, Sender};
// 丢包注入随机源
use rand::RngExt as _;
// 会话/请求唯一 ID
use uuid::Uuid;

// 统一 Error/Result
use crate::error::{Error, Result};
// KV 命令与状态机
use crate::raft::kv::{self, Command, Kv};
// Raft 协议核心类型
use crate::raft::{
    // 协议类型：信封、日志、消息、节点与请求响应
    Envelope, Index, Log, Message, Node, NodeID, Options, Request, RequestID, Response, Status,
    // tick 周期常量
    TICK_INTERVAL,
// 当前作用域结束
};
// 临时 BitCask 日志引擎
use crate::storage::BitCask;

// ---------------------------------------------------------------------------
// 传输层（分区 / 丢包 / 乱序）
// ---------------------------------------------------------------------------

// 派生 Default，便于 Transport 空状态启动
#[derive(Default)]
// 传输可变内核：故障策略与邮箱
struct TransportInner {
    /// `(from, to)` 被阻断时，`from` 无法把消息投递给 `to`。
    partitions: HashSet<(NodeID, NodeID)>,
    /// 随机丢包率 `[0.0, 1.0]`。
    drop_rate: f64,
    /// 简单乱序：缓存一条出站消息，下次再与新消息交换顺序发出。
    reorder: bool,
    // 乱序注入时按目标缓存的待交换消息
    held: HashMap<NodeID, Envelope>,
    /// 节点是否在线（stop 后为 false）。
    online: HashMap<NodeID, bool>,
    /// 各节点入站邮箱。
    mailboxes: HashMap<NodeID, Sender<Envelope>>,
// 当前作用域结束
}

/// 可故障注入的进程内传输。
#[derive(Clone, Default)]
// 可故障注入的进程内传输
pub struct Transport {
    // 业务：inner: Arc<Mutex<TransportInner>>,
    inner: Arc<Mutex<TransportInner>>,
// 当前作用域结束
}

// 实现该类型的方法
impl Transport {
    // 登记节点入站通道并标在线
    fn register(&self, id: NodeID, tx: Sender<Envelope>) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 写入路由/分区/挂起表项
        g.mailboxes.insert(id, tx);
        // 写入路由/分区/挂起表项
        g.online.insert(id, true);
    // 当前作用域结束
    }

    // 模拟崩溃/恢复：离线丢弃收发
    fn set_online(&self, id: NodeID, online: bool) {
        // TransportInner 字段定义结束
        let mut g = self.inner.lock().expect("transport lock");
        // 写入路由/分区/挂起表项
        g.online.insert(id, online);
        // 检查节点是否在线
        if !online {
            // 移除表项或清理缓存
            g.held.remove(&id);
        // 当前作用域结束
        }
    // 当前作用域结束
    }

    // 按分区/丢包/乱序规则投递
    fn deliver(&self, msg: Envelope) {
        // Transport 包装结构结束
        let mut g = self.inner.lock().expect("transport lock");
        // 信封发送方
        let from = msg.from;
        // 信封接收方
        let to = msg.to;

        // 检查节点是否在线
        if !g.online.get(&from).copied().unwrap_or(false) {
            // 完成当前语句
            return;
        // 当前作用域结束
        }
        // 检查节点是否在线
        if !g.online.get(&to).copied().unwrap_or(false) {
            // 完成当前语句
            return;
        // 当前作用域结束
        }
        // 命中分区则阻断投递
        if g.partitions.contains(&(from, to)) {
            // 完成当前语句
            return;
        // register：节点入站通道登记完毕
        }
        // 按丢包率随机丢弃
        if g.drop_rate > 0.0 && rand::rng().random::<f64>() < g.drop_rate {
            // 完成当前语句
            return;
        // 当前作用域结束
        }

        // 进入乱序注入逻辑
        if g.reorder {
            // 可选值解构分支
            if let Some(prev) = g.held.remove(&to) {
                // 先发当前，再发缓存 → 乱序
                if let Some(tx) = g.mailboxes.get(&to) {
                    // 发送端通道
                    let _ = tx.try_send(msg);
                    // 发送端通道
                    let _ = tx.try_send(prev);
                // 当前作用域结束
                }
                // 离线时清理 held 缓存的分支结束
                return;
            // set_online 方法结束
            }
            // 写入路由/分区/挂起表项
            g.held.insert(to, msg);
            // 完成当前语句
            return;
        // 当前作用域结束
        }

        // 可选值解构分支
        if let Some(tx) = g.mailboxes.get(&to) {
            // 发送端通道
            let _ = tx.try_send(msg);
        // 当前作用域结束
        }
    // 当前作用域结束
    }

    /// 双向隔离 `a` 与 `b`。
    pub fn partition(&self, a: NodeID, b: NodeID) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 写入路由/分区/挂起表项
        g.partitions.insert((a, b));
        // 写入路由/分区/挂起表项
        g.partitions.insert((b, a));
    // 发送方离线则静默丢弃的分支结束
    }

    /// 单向阻断 `from -> to`。
    pub fn partition_one_way(&self, from: NodeID, to: NodeID) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 接收方离线则静默丢弃的分支结束
        g.partitions.insert((from, to));
    // 当前作用域结束
    }

    /// 按组双向分区：`left` 内节点与 `right` 内节点互不可达。
    pub fn partition_groups(&self, left: &[NodeID], right: &[NodeID]) {
        // 命中分区规则阻断的分支结束
        let mut g = self.inner.lock().expect("transport lock");
        // 遍历集合或重试轮次
        for &a in left {
            // 遍历集合或重试轮次
            for &b in right {
                // 写入路由/分区/挂起表项
                g.partitions.insert((a, b));
                // 写入路由/分区/挂起表项
                g.partitions.insert((b, a));
            // 随机丢包分支结束
            }
        // 当前作用域结束
        }
    // 当前作用域结束
    }

    /// 恢复 `a` 与 `b` 之间的连通（双向）。
    pub fn heal_pair(&self, a: NodeID, b: NodeID) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 移除表项或清理缓存
        g.partitions.remove(&(a, b));
        // 移除表项或清理缓存
        g.partitions.remove(&(b, a));
    // 当前作用域结束
    }

    /// 清除全部分区，并冲刷乱序缓存。
    pub fn heal_all(&self) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 清空全部分区
        g.partitions.clear();
        // 已有 held 时交换发出的 if 结束
        let held = std::mem::take(&mut g.held);
        // 遍历集合或重试轮次
        for (to, msg) in held {
            // 可选值解构分支
            if let Some(tx) = g.mailboxes.get(&to) {
                // 发送端通道
                let _ = tx.try_send(msg);
            // 当前作用域结束
            }
        // 尚无缓存则 hold 本条的分支结束
        }
    // 当前作用域结束
    }

    // 设置随机丢包率并夹紧到[0,1]
    pub fn set_drop_rate(&self, rate: f64) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 语句/调用结束
        g.drop_rate = rate.clamp(0.0, 1.0);
    // 正常路径 try_send 到目标 inbox 结束
    }

    // 开关乱序；关闭时冲刷 held
    pub fn set_reorder(&self, on: bool) {
        // 独占传输内核锁
        let mut g = self.inner.lock().expect("transport lock");
        // 完成当前语句
        g.reorder = on;
        // 关闭乱序时冲刷 held
        if !on {
            // 独占传输内核锁
            let held = std::mem::take(&mut g.held);
            // 遍历集合或重试轮次
            for (to, msg) in held {
                // 可选值解构分支
                if let Some(tx) = g.mailboxes.get(&to) {
                    // 发送端通道
                    let _ = tx.try_send(msg);
                // 当前作用域结束
                }
            // 双向 partition 注入结束
            }
        // 当前作用域结束
        }
    // 当前作用域结束
    }
// 当前作用域结束
}

// ---------------------------------------------------------------------------
// 客户端
// ---------------------------------------------------------------------------

// 节点侧请求+应答通道别名
type RequestTx = Sender<(Request, Sender<Result<Response>>)>;

/// 可向任意本地节点提交请求的客户端（跟随者会转发到领导者）。
#[derive(Clone)]
// 可向任意本地节点提交请求的客户端
pub struct Client {
    // 各节点请求通道表，可动态增删
    request_txs: Arc<Mutex<HashMap<NodeID, RequestTx>>>,
    // 优先尝试的入口节点（常为领导者）
    preferred: NodeID,
    // 外层最大重试轮数
    attempts: u32,
    // 单次等待节点应答超时
    per_attempt_timeout: Duration,
    // 一轮全失败后的选举等待
    retry_sleep: Duration,
// 当前作用域结束
}

// 实现该类型的方法
impl Client {
    // 左右组一对节点双向隔离结束
    fn new(request_txs: HashMap<NodeID, RequestTx>, preferred: NodeID) -> Self {
        // 左组遍历结束
        Self {
            // partition_groups 结束
            request_txs: Arc::new(Mutex::new(request_txs)),
            // 优先尝试的入口节点（常为领导者）
            preferred,
            // 外层最大重试轮数
            attempts: 40,
            // 单次等待节点应答超时
            per_attempt_timeout: Duration::from_millis(200),
            // 一轮全失败后的选举等待
            retry_sleep: Duration::from_millis(50),
        // 当前作用域结束
        }
    // 当前作用域结束
    }

    /// 注册或更新某节点的请求通道（节点 start 后调用）。
    pub fn register_node(&self, id: NodeID, tx: RequestTx) {
        // heal_pair 双向恢复结束
        self.request_txs.lock().expect("client lock").insert(id, tx);
    // 当前作用域结束
    }

    // 节点 start 后注册请求通道
    pub fn unregister_node(&self, id: NodeID) {
        // 移除表项或清理缓存
        self.request_txs.lock().expect("client lock").remove(&id);
    // 当前作用域结束
    }

    /// 提示下次优先向该节点发请求。
    pub fn preferred_hint(&mut self, id: NodeID) {
        // 更新自身状态字段
        self.preferred = id;
    // 当前作用域结束
    }

    // 按 preferred 优先轮询提交请求
    pub fn request(&mut self, request: Request) -> Result<Response> {
        // 协议层请求
        let txs = self.request_txs.lock().expect("client lock").clone();
        // 节点尝试顺序（preferred 优先）
        let mut order: Vec<NodeID> = txs.keys().copied().collect();
        // 语句/调用结束
        order.sort();
        // 冲刷单条 held 到邮箱结束
        if let Some(pos) = order.iter().position(|&id| id == self.preferred) {
            // held 补发循环结束
            let id = order.remove(pos);
            // heal_all 结束
            order.insert(0, id);
        // 当前作用域结束
        }

        // 可重试的最后错误
        let mut last_err = Error::Abort;
        // 遍历集合或重试轮次
        for _ in 0..self.attempts {
            // 遍历集合或重试轮次
            for &node_id in &order {
                // 请求通道表快照
                let Some(tx) = txs.get(&node_id) else { continue };
                // 集群响应
                let (resp_tx, resp_rx) = channel::bounded(1);
                // set_drop_rate 夹紧并写回结束
                if tx.send((request.clone(), resp_tx)).is_err() {
                    // 继续下一轮
                    continue;
                // 当前作用域结束
                }
                // 单次等待节点应答超时
                match resp_rx.recv_timeout(self.per_attempt_timeout) {
                    // 成功：缓存 preferred 并返回响应
                    Ok(Ok(resp)) => {
                        // 更新自身状态字段
                        self.preferred = node_id;
                        // 返回业务结果或错误
                        return Ok(resp);
                    // 当前作用域结束
                    }
                    // 无主/转发失败，可换节点重试
                    Ok(Err(Error::Abort)) => last_err = Error::Abort,
                    // 业务错误立即返回，不重试
                    Ok(Err(e)) => return Err(e),
                    // 超时或通道错误处理
                    Err(_) => last_err = Error::IO("request timed out".into()),
                // match 分支结束
                }
            // 当前作用域结束
            }
            // 一轮全失败后的选举等待
            thread::sleep(self.retry_sleep);
        // 当前作用域结束
        }
        // 业务：Err(last_err)
        Err(last_err)
    // 当前作用域结束
    }

    /// 只向指定节点发请求（仍在 Abort 时对该节点重试）。
    pub fn request_on(&mut self, node_id: NodeID, request: Request) -> Result<Response> {
        // 关闭乱序清理分支结束
        let txs = self.request_txs.lock().expect("client lock").clone();
        // set_reorder 结束
        let Some(tx) = txs.get(&node_id).cloned() else {
            // Transport 实现块结束
            return Err(Error::IO(format!("node {node_id} not registered")));
        // 当前作用域结束
        };
        // 可重试的最后错误
        let mut last_err = Error::Abort;
        // 遍历集合或重试轮次
        for _ in 0..self.attempts {
            // 集群响应
            let (resp_tx, resp_rx) = channel::bounded(1);
            // 通道关闭则跳过/失败
            if tx.send((request.clone(), resp_tx)).is_err() {
                // 返回业务结果或错误
                return Err(Error::IO(format!("node {node_id} request channel closed")));
            // 当前作用域结束
            }
            // 单次等待节点应答超时
            match resp_rx.recv_timeout(self.per_attempt_timeout) {
                // 成功：缓存 preferred 并返回响应
                Ok(Ok(resp)) => {
                    // 更新自身状态字段
                    self.preferred = node_id;
                    // 返回业务结果或错误
                    return Ok(resp);
                // 当前作用域结束
                }
                // 无主/转发失败，可换节点重试
                Ok(Err(Error::Abort)) => last_err = Error::Abort,
                // 业务错误立即返回，不重试
                Ok(Err(e)) => return Err(e),
                // 超时或通道错误处理
                Err(_) => last_err = Error::IO("request timed out".into()),
            // match 分支结束
            }
            // 一轮全失败后的选举等待
            thread::sleep(self.retry_sleep);
        // 当前作用域结束
        }
        // 业务：Err(last_err)
        Err(last_err)
    // 当前作用域结束
    }

    // 便捷 Put：编码写并解析提交索引
    pub fn put(&mut self, key: &str, value: &str) -> Result<Index> {
        // Client 字段定义结束
        let req = Request::Write(kv::encode(&Command::Put {
            // 填充键字段
            key: key.into(),
            // 填充值字段
            value: value.into(),
        // 语句/调用结束
        }));
        // 按响应/结果分支处理
        match self.request(req)? {
            // 写回包：解码 KV 层响应
            Response::Write(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                // Put 成功，返回提交索引
                kv::Response::Put(index) => Ok(index),
                // 未知命令或非预期响应
                other => Err(Error::InvalidData(format!("unexpected write response: {other:?}"))),
            // match 分支结束
            },
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected Write, got {other:?}"))),
        // match 分支结束
        }
    // 当前作用域结束
    }

    // 指定节点上的 Put（测转发）
    pub fn put_on(&mut self, node_id: NodeID, key: &str, value: &str) -> Result<Index> {
        // 协议层请求
        let req = Request::Write(kv::encode(&Command::Put {
            // 填充键字段
            key: key.into(),
            // 填充值字段
            value: value.into(),
        // 语句/调用结束
        }));
        // Client::new 默认超时参数组装结束
        match self.request_on(node_id, req)? {
            // Client::new 结束
            Response::Write(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                // Put 成功，返回提交索引
                kv::Response::Put(index) => Ok(index),
                // 未知命令或非预期响应
                other => Err(Error::InvalidData(format!("unexpected write response: {other:?}"))),
            // match 分支结束
            },
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected Write, got {other:?}"))),
        // match 分支结束
        }
    // register_node 写入路由表结束
    }

    // 便捷 Get：线性读路径
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        // 协议层请求
        let req = Request::Read(kv::encode(&Command::Get { key: key.into() }));
        // 按响应/结果分支处理
        match self.request(req)? {
            // 读回包：解码 KV 层响应
            Response::Read(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                // unregister_node 移除路由结束
                kv::Response::Get(v) => Ok(v),
                // 未知命令或非预期响应
                other => Err(Error::InvalidData(format!("unexpected read response: {other:?}"))),
            // match 分支结束
            },
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected Read, got {other:?}"))),
        // match 分支结束
        }
    // 当前作用域结束
    }

    // 全量扫描状态机
    pub fn scan(&mut self) -> Result<std::collections::BTreeMap<String, String>> {
        // 协议层请求
        let req = Request::Read(kv::encode(&Command::Scan));
        // 按响应/结果分支处理
        match self.request(req)? {
            // 读回包：解码 KV 层响应
            Response::Read(bytes) => match kv::decode::<kv::Response>(&bytes)? {
                // Scan 返回有序 map
                kv::Response::Scan(map) => Ok(map),
                // 未知命令或非预期响应
                other => Err(Error::InvalidData(format!("unexpected scan response: {other:?}"))),
            // match 分支结束
            },
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected Read, got {other:?}"))),
        // match 分支结束
        }
    // 当前作用域结束
    }

    // 查询集群 Status
    pub fn status(&mut self) -> Result<Status> {
        // 按响应/结果分支处理
        match self.request(Request::Status)? {
            // 状态回包：展示主从与位点
            Response::Status(s) => Ok(s),
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected Status, got {other:?}"))),
        // 将 preferred 提到队首的调整结束
        }
    // 当前作用域结束
    }

    // 指定节点 Status（观察分区视图）
    pub fn status_on(&mut self, node_id: NodeID) -> Result<Status> {
        // 只向指定节点发请求并重试
        match self.request_on(node_id, Request::Status)? {
            // 状态回包：展示主从与位点
            Response::Status(s) => Ok(s),
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected Status, got {other:?}"))),
        // match 分支结束
        }
    // 当前作用域结束
    }

    /// 变更集群成员（目标投票人集合）。需协议侧支持 `Request::ChangeMembership`。
    pub fn change_membership(&mut self, voters: HashSet<NodeID>) -> Result<Index> {
        // 按响应/结果分支处理
        match self.request(Request::ChangeMembership { voters })? {
            // 成员变更已提议，打印日志索引
            Response::ChangeMembership { index } => Ok(index),
            // 未知命令或非预期响应
            other => Err(Error::InvalidData(format!("expected ChangeMembership, got {other:?}"))),
        // match 分支结束
        }
    // 通道已关闭则跳过该节点的分支结束
    }
// 当前作用域结束
}

/// 等待选出领导者。
pub fn wait_for_leader(client: &mut Client) -> Result<Status> {
    // 可重试的最后错误
    let mut last_err = Error::Abort;
    // 遍历集合或重试轮次
    for _ in 0..100 {
        // 按响应/结果分支处理
        match client.status() {
            // 成功路径：推进状态或返回
            Ok(s) => return Ok(s),
            // 成功响应并缓存 preferred 的分支结束
            Err(Error::Abort) => {
                // 完成当前语句
                last_err = Error::Abort;
                // 失败后短暂退避，等待选主稳定
                thread::sleep(Duration::from_millis(50));
            // 当前作用域结束
            }
            // 超时或通道错误处理
            Err(e) => return Err(e),
        // match 分支结束
        }
    // 当前作用域结束
    }
    // 单次 recv_timeout 结果 match 结束
    Err(last_err)
// 内层按节点顺序尝试结束
}

// ---------------------------------------------------------------------------
// 集群
// ---------------------------------------------------------------------------

// request 多节点轮询结束
struct NodeControl {
    // 通知节点线程退出的发送端
    stop_tx: Sender<()>,
// 当前作用域结束
}

/// 进程内多节点 Raft 集群。
pub struct Cluster {
    // 集群统一 Raft 选项快照
    opts: Options,
    // 共享故障注入传输
    transport: Transport,
    // 运行中节点控制句柄
    nodes: HashMap<NodeID, NodeControl>,
    /// 逻辑成员集合（用于新节点 peers 计算）；成员变更协议生效前由测试/调用方维护。
    members: HashSet<NodeID>,
    // 绑定各节点请求通道的客户端
    client: Client,
// 当前作用域结束
}

// 实现该类型的方法
impl Cluster {
    /// 使用默认快速测试选项启动集群。
    pub fn spawn(node_ids: &[NodeID]) -> Self {
        // 按 Options 拉起节点组与客户端
        Self::spawn_with_options(node_ids, test_options())
    // 当前作用域结束
    }

    // 集群统一 Raft 选项快照
    pub fn spawn_with_options(node_ids: &[NodeID], opts: Options) -> Self {
        // 通道关闭直接失败的分支结束
        let transport = Transport::default();
        // 协议层请求
        let mut request_txs = HashMap::new();
        // 构造或 tick/step 后的 Node
        let mut nodes = HashMap::new();
        // 逻辑成员集合（peers 计算用）
        let members: HashSet<NodeID> = node_ids.iter().copied().collect();

        // 遍历集合或重试轮次
        for &id in node_ids {
            // 启动单节点线程：邮箱、日志、事件循环
            let (control, req_tx) = spawn_node(id, &members, opts.clone(), transport.clone());
            // 写入路由/分区/挂起表项
            request_txs.insert(id, req_tx.clone());
            // 写入路由/分区/挂起表项
            nodes.insert(id, control);
        // request_on 成功返回分支结束
        }

        // 构造或 tick/step 后的 Node
        let preferred = node_ids.first().copied().unwrap_or(1);
        // 协议层请求
        let client = Client::new(request_txs, preferred);

        // 组装结构体字段
        Self { opts, transport, nodes, members, client }
    // 当前作用域结束
    }

    // 克隆共享客户端句柄
    pub fn client(&self) -> Client {
        // 业务：self.client.clone()
        self.client.clone()
    // request_on 重试循环结束
    }

    // 暴露传输以便故障注入
    pub fn transport(&self) -> Transport {
        // request_on 方法结束
        self.transport.clone()
    // 当前作用域结束
    }

    // 返回逻辑成员视图副本
    pub fn members(&self) -> HashSet<NodeID> {
        // 业务：self.members.clone()
        self.members.clone()
    // 当前作用域结束
    }

    // 只读访问启动选项
    pub fn options(&self) -> &Options {
        // 业务：&self.opts
        &self.opts
    // 当前作用域结束
    }

    /// 停止节点（模拟崩溃）：不再处理消息/请求，传输层视为离线。
    pub fn stop(&mut self, id: NodeID) {
        // 可选值解构分支
        if let Some(ctrl) = self.nodes.remove(&id) {
            // 发送端通道
            let _ = ctrl.stop_tx.send(());
            // 节点 start 后注册请求通道
            self.client.unregister_node(id);
            // 线程退出前标离线，避免继续投递
            self.transport.set_online(id, false);
        // 当前作用域结束
        }
    // 当前作用域结束
    }

    /// 以空 BitCask 日志重新拉起节点（用于成员加入；新路径空库）。
    pub fn start(&mut self, id: NodeID) {
        // put 协议响应 match 结束
        if self.nodes.contains_key(&id) {
            // put 便捷方法结束
            return;
        // 当前作用域结束
        }
        // 写入路由/分区/挂起表项
        self.members.insert(id);
        // 协议层请求
        let (control, req_tx) =
            // 启动单节点线程：邮箱、日志、事件循环
            spawn_node(id, &self.members, self.opts.clone(), self.transport.clone());
        // 节点 start 后注册请求通道
        self.client.register_node(id, req_tx);
        // 写入路由/分区/挂起表项
        self.nodes.insert(id, control);
    // 当前作用域结束
    }

    /// 仅更新本地成员集合视图（协议提交成员变更后由测试调用）。
    pub fn set_members(&mut self, members: HashSet<NodeID>) {
        // 更新自身状态字段
        self.members = members;
    // 当前作用域结束
    }

    // 双向隔离两节点
    pub fn partition(&self, a: NodeID, b: NodeID) {
        // 语句/调用结束
        self.transport.partition(a, b);
    // 当前作用域结束
    }

    // 左右组双向分区（多数/少数场景）
    pub fn partition_groups(&self, left: &[NodeID], right: &[NodeID]) {
        // put_on 的 KV 响应内层 match 结束
        self.transport.partition_groups(left, right);
    // 当前作用域结束
    }

    // put_on 协议响应 match 结束
    pub fn heal_all(&self) {
        // put_on 方法结束
        self.transport.heal_all();
    // 当前作用域结束
    }

    // 设置随机丢包率并夹紧到[0,1]
    pub fn set_drop_rate(&self, rate: f64) {
        // 设置随机丢包率并夹紧到[0,1]
        self.transport.set_drop_rate(rate);
    // 当前作用域结束
    }

    // 开关乱序；关闭时冲刷 held
    pub fn set_reorder(&self, on: bool) {
        // 开关乱序；关闭时冲刷 held
        self.transport.set_reorder(on);
    // 当前作用域结束
    }

    // 节点控制表是否仍登记运行
    pub fn is_running(&self, id: NodeID) -> bool {
        // 业务：self.nodes.contains_key(&id)
        self.nodes.contains_key(&id)
    // 当前作用域结束
    }
// get 的 KV 响应内层 match 结束
}

/// 测试/演示用较快超时。
pub fn test_options() -> Options {
    // get 便捷方法结束
    Options {
        // 心跳间隔（tick 数）
        heartbeat_interval: 2,
        // 选举超时随机区间
        election_timeout_range: 5..10,
        // 单次 AppendEntries 批量上限
        max_append_entries: 100,
        // 集成测试默认开启；若 flaky 可在具体用例里覆盖。
        pre_vote: true,
        // 领导者定期确认多数派存活
        check_quorum: true,
        // 快照阈值（0 表示默认/关闭）
        snapshot_threshold: 0,
    // 当前作用域结束
    }
// 当前作用域结束
}

// 启动单节点线程：邮箱、日志、事件循环
fn spawn_node(
    // 业务：id: NodeID,
    id: NodeID,
    // 逻辑成员集合（peers 计算用）
    members: &HashSet<NodeID>,
    // scan 的 KV 响应内层 match 结束
    opts: Options,
    // 共享故障注入传输
    transport: Transport,
// 进入代码块
) -> (NodeControl, RequestTx) {
    // scan 协议响应 match 结束
    let peers: HashSet<NodeID> = members.iter().copied().filter(|&p| p != id).collect();
    // scan 便捷方法结束
    let (inbox_tx, inbox_rx) = channel::unbounded();
    // 语句/调用结束
    transport.register(id, inbox_tx);

    // 协议层请求
    let (request_tx, request_rx) = channel::unbounded();
    // 发送端通道
    let (stop_tx, stop_rx) = channel::bounded(1);
    // 发送端通道
    let (node_tx, node_rx) = channel::unbounded();

    // 并行测试会同时打开多个 BitCask；路径必须全局唯一以免文件锁冲突。
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    // 单调写序号/路径唯一序号
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // status 响应 match 结束
    let path = std::env::temp_dir().join(format!(
        // status 便捷方法结束
        "raft-cluster-{}-{}-{}-{}.log",
        // 路径嵌入 pid 避免跨进程冲突
        std::process::id(),
        // 业务：id,
        id,
        // 业务：seq,
        seq,
        // 业务：std::time::SystemTime::now()
        std::time::SystemTime::now()
            // 纳秒时间戳进一步保证路径唯一
            .duration_since(std::time::UNIX_EPOCH)
            // 时间戳换算（启动期不可失败）
            .unwrap()
            // 纳秒时间戳进一步保证路径唯一
            .as_nanos()
    // 语句/调用结束
    ));
    // 日志或会话文件路径
    let log = Log::new(Box::new(BitCask::new(path).expect("bitcask"))).expect("log");
    // status_on 响应 match 结束
    let node = Node::new(id, peers, log, Kv::new(), node_tx, opts).expect("node");

    // 共享传输
    let transport_out = transport.clone();
    // 后台启动节点事件循环
    thread::spawn(move || {
        // 单节点主循环：tick/入站/出站/请求
        run_node(node, inbox_rx, node_rx, request_rx, stop_rx, transport_out);
    // 语句/调用结束
    });

    // 业务：(NodeControl { stop_tx }, request_tx)
    (NodeControl { stop_tx }, request_tx)
// 当前作用域结束
}

// 单节点主循环：tick/入站/出站/请求
fn run_node(
    // change_membership 响应 match 结束
    mut node: Node,
    // change_membership 方法结束
    peers_rx: Receiver<Envelope>,
    // Client 实现块结束
    node_rx: Receiver<Envelope>,
    // 业务：request_rx: Receiver<(Request, Sender<Re...
    request_rx: Receiver<(Request, Sender<Result<Response>>)>,
    // 业务：stop_rx: Receiver<()>,
    stop_rx: Receiver<()>,
    // 共享故障注入传输
    transport: Transport,
// 进入代码块
) {
    // 协议 tick 时钟
    let ticker = channel::tick(TICK_INTERVAL);
    // 集群响应
    let mut response_txs: HashMap<RequestID, Sender<Result<Response>>> = HashMap::new();
    // 构造或 tick/step 后的 Node
    let node_id = node.id();

    // 直到 stop 或致命错误
    loop {
        // 多路复用 tick/入站/出站/客户端请求
        crossbeam::select! {
            // 集群 stop：退出事件循环
            recv(stop_rx) -> _ => break,

            // 周期 tick：推进超时与心跳
            recv(ticker) -> _ => {
                // 推进协议时钟（选举/心跳）
                node = match node.tick() {
                    // 成功路径：推进状态或返回
                    Ok(n) => n,
                    // 超时或通道错误处理
                    Err(_) => break,
                // match 分支结束
                };
            // 尚无主时休眠再询的分支结束
            }

            // 传输层投递的对端消息
            recv(peers_rx) -> msg => {
                // wait_for_leader 单次 status 匹配结束
                let Ok(msg) = msg else { break };
                // 等待选主轮询循环结束
                node = match node.step(msg) {
                    // 成功路径：推进状态或返回
                    Ok(n) => n,
                    // 超时或通道错误处理
                    Err(_) => break,
                // wait_for_leader 辅助函数结束
                };
            // 当前作用域结束
            }

            // 本节点协议出站信封
            recv(node_rx) -> msg => {
                // 构造/接收的信封
                let Ok(msg) = msg else { break };
                // 目标是自己：处理客户端回环
                if msg.to == node_id {
                    // 本机回环：把结果交还调用方
                    if let Message::ClientResponse { id, response } = msg.message {
                        // 可选值解构分支
                        if let Some(tx) = response_txs.remove(&id) {
                            // 集群响应
                            let _ = tx.send(response);
                        // 当前作用域结束
                        }
                    // NodeControl 仅含 stop 信号发送端
                    }
                    // 继续下一轮
                    continue;
                // 当前作用域结束
                }
                // 经故障注入规则发往对端
                transport.deliver(msg);
            // 当前作用域结束
            }

            // 本地 Client 提交的请求
            recv(request_rx) -> result => {
                // 协议层请求
                let Ok((request, response_tx)) = result else { break };
                // 请求 ID 或节点 ID
                let id = Uuid::new_v4();
                // 构造/接收的信封
                let msg = Envelope {
                    // 信封来源节点
                    from: node.id(),
                    // 信封目标节点
                    to: node.id(),
                    // 信封携带当前任期
                    term: node.term(),
                    // 封装为自发自收客户端请求
                    message: Message::ClientRequest { id, request },
                // Cluster 字段定义结束
                };
                // 写入路由/分区/挂起表项
                response_txs.insert(id, response_tx);
                // 步进处理一封协议/客户端消息
                node = match node.step(msg) {
                    // 成功路径：推进状态或返回
                    Ok(n) => n,
                    // 超时或通道错误处理
                    Err(_) => break,
                // match 分支结束
                };
            // 当前作用域结束
            }
        // 当前作用域结束
        }
    // spawn 委托到带 Options 的实现结束
    }

    // 线程退出前标离线，避免继续投递
    transport.set_online(node_id, false);
// 当前作用域结束
}
