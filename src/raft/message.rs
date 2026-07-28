// 领导者 Status 中 match_index 用有序 map 保证确定性
use std::collections::BTreeMap;

// 消息经 bincode 跨节点传输
use serde::{Deserialize, Serialize};

// 日志条目、索引与节点/任期标识
use super::{Entry, Index, NodeID, Term};
// ClientResponse 中携带 Result
use crate::error::Result;
// 存储引擎状态嵌入集群 Status
use crate::storage;

/// 消息信封，标明发送方与接收方。
// 可克隆以便重试；可序列化走网络
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义数据结构
pub struct Envelope {
    /// 发送方。
    pub from: NodeID,
    /// 发送方当前任期。
    // 接收方用 term 判断是否过期或需要降级为跟随者
    pub term: Term,
    /// 接收方。
    pub to: NodeID,
    /// 消息本体。
    pub message: Message,
// 结束当前作用域
}

/// Raft 节点之间发送的消息。消息异步发送（非请求/响应模式），可能丢失或乱序。
///
/// 实践中它们经 TCP 连接与 crossbeam channel 传递；只要连接保持，通常不会丢失或乱序。
/// 一条消息及其响应走各自独立的出站 TCP 连接。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义枚举
pub enum Message {
    /// 预投票请求（Pre-vote）：不提升任期、不持久化投票。
    /// 用于在真正选举前确认能否获得多数，避免分区节点抬升任期打断稳定领导。
    PreCampaign {
        /// 候选人最后一条日志的索引。
        // 投票方用 last_index/last_term 判断日志新旧
        last_index: Index,
        /// 候选人最后一条日志的任期。
        last_term: Term,
    // 结构项闭合
    },

    /// 预投票响应。不持久化。
    PreCampaignResponse {
        /// 为 true 表示授予预选票。
        vote: bool,
    // 结构项闭合
    },

    /// 候选人向同伴拉票竞选领导者。
    /// 仅当候选人的日志至少与投票者一样新时才会被授予选票。
    Campaign {
        /// 候选人最后一条日志的索引。
        last_index: Index,
        /// 候选人最后一条日志的任期。
        last_term: Term,
    // 结构项闭合
    },

    /// 跟随者每个任期只能投一票，且仅当候选人的日志至少与自己一样新时才投票。
    /// 候选人隐式投票给自己。
    CampaignResponse {
        /// 为 true 表示授予选票。false 响应并非必须，但为清晰起见仍会发出。
        vote: bool,
    // 结构项闭合
    },

    /// 领导者发送的周期性心跳，作用包括：
    ///
    /// * 告知节点当前领导者，并阻止选举。
    /// * 检测丢失的 append / read，作为重试机制。
    /// * 推进跟随者的 commit 索引，以便它们应用条目。
    ///
    /// Raft 论文没有独立的心跳消息，而是使用空的 AppendEntries RPC；
    /// 这里单独定义以便职责更清晰。
    Heartbeat {
        /// 领导者最后一条日志的索引。任期即领导者当前任期（当选时会追加 noop）。
        /// 跟随者据此与本地日志比较，判断是否跟上。
        last_index: Index,
        /// 领导者最后已提交日志的索引。跟随者用它推进 commit 索引并应用条目。
        /// 仅当本地日志在 last_index 处与领导者一致时，提交到此索引才安全。
        commit_index: Index,
        /// 领导者在本任期内最新的读序列号。
        // 兼作线性一致读的 quorum 确认载体
        read_seq: ReadSequence,
    // 结构项闭合
    },

    /// 跟随者在仍认可其为领导者时，对心跳作出响应。
    HeartbeatResponse {
        /// 非零表示心跳中的 last_index 与跟随者日志匹配；否则跟随者日志分叉或落后。
        match_index: Index,
        /// 心跳中的读序列号。
        // 回传以便领导者统计该 seq 的多数确认
        read_seq: ReadSequence,
    // 结构项闭合
    },

    /// 领导者在给定 base 条目之后，向跟随者追加日志条目以进行复制。
    ///
    /// 若 base 条目与跟随者日志匹配，则两边日志在此之前完全一致（见论文 5.3 节），
    /// 可以追加（可能替换冲突条目）。否则拒绝追加，领导者需用更早的 base 索引重试，
    /// 直到找到公共 base。
    ///
    /// 空 append（无条目）用于在日志分叉、节点重启或消息丢失时探测公共 match 索引。
    /// 通常通过递减 base 索引探测，匹配后再发送后续条目。
    Append {
        /// 在此日志索引之后追加。
        base_index: Index,
        /// base 条目的任期。
        base_term: Term,
        /// 要追加的日志条目，必须从 base_index + 1 开始。
        // 空 Vec 表示仅探测 match 点
        entries: Vec<Entry>,
    // 结构项闭合
    },

    /// 跟随者根据 base 条目是否匹配本地日志，接受或拒绝领导者的 append。
    AppendResponse {
        /// 非零表示跟随者已追加到该索引（此前日志与领导者一致）。
        /// 若未发送条目（探测），则为匹配的 base 索引。
        match_index: Index,
        /// 非零表示在该 base 索引处拒绝（base 索引/任期不匹配）。
        /// 若本地日志短于 base 索引，reject 会降到 last_index+1，避免逐个探测缺失索引。
        reject_index: Index,
    // 结构项闭合
    },

    /// 领导者在提供读服务前需确认自己仍是领导者，以保证线性一致性
    ///（防止别处已选出新领导者）。读请求在序列号被多数派确认后才执行。
    // 独立 Read 消息可与心跳并行推进读确认
    Read { seq: ReadSequence },

    /// 跟随者确认该读序列号下的领导权。
    ReadResponse { seq: ReadSequence },

    /// 客户端请求。可提交给领导者，或提交给跟随者由后者转发给领导者。
    /// 若无领导者，或领导者/任期变更，请求会以 `Error::Abort` 的
    /// ClientResponse 中止，客户端必须重试。
    ClientRequest {
        /// 请求 ID。在请求生命周期内必须全局唯一。
        id: RequestID,
        /// 请求本体。
        request: Request,
    // 结构项闭合
    },

    /// 客户端响应，通常透传给状态机。
    ClientResponse {
        /// 对应原始 ClientRequest 的 ID。
        id: RequestID,
        /// 响应，或错误。
        // 含 Abort/状态机错误等
        response: Result<Response>,
    // 结构项闭合
    },

    /// 安装快照（整包；教学实现不分块）。
    InstallSnapshot {
        // 快照覆盖到的最后日志索引
        last_included_index: Index,
        // 该索引处日志任期，用于一致性校验
        last_included_term: Term,
        // 状态机完整快照字节
        data: Vec<u8>,
        // 快照时刻的成员配置，安装后立即生效
        membership: crate::raft::membership::MembershipEntry,
    // 结构项闭合
    },

    /// 快照安装确认。
    InstallSnapshotResponse {
        // 回传已安装的 last_included_index，领导者据此推进 match
        last_included_index: Index,
    // 结构项闭合
    },
// 结束当前作用域
}

/// 客户端请求 ID。在途期间必须全局唯一。
///
/// 为简单起见使用随机 UUIDv4。也可加入节点/进程/MAC 与时间戳以更好避撞
///（例如 UUIDv6），但在此规模下不必。
pub type RequestID = uuid::Uuid;

/// 读序列号，用于为线性一致读确认领导权。
// 领导者本任期内单调递增
pub type ReadSequence = u64;

/// 客户端请求，通常透传给状态机。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义枚举
pub enum Request {
    /// 状态机读命令，经 `State::read` 执行。不复制，仅在领导者上求值。
    Read(Vec<u8>),
    /// 状态机写命令，经 `State::apply` 执行。复制到所有节点，结果必须确定。
    Write(Vec<u8>),
    /// 带客户端 session 的写：日志中保存 (client_id, seq)，apply 时去重。
    WriteSession {
        // 客户端会话 ID
        client_id: uuid::Uuid,
        // 该会话内单调序号
        seq: u64,
        // 实际应用层写命令
        command: Vec<u8>,
    // 结构项闭合
    },
    /// 向领导者查询 Raft 集群状态。
    Status,
    /// 变更集群投票成员为目标集合（联合共识）。
    ChangeMembership {
        // 目标投票成员集合
        voters: std::collections::HashSet<NodeID>,
    // 结构项闭合
    },
// 结束当前作用域
}

/// 客户端响应。外层用 Result 表示错误。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义枚举
pub enum Response {
    /// 状态机读结果。
    Read(Vec<u8>),
    /// 状态机写结果。
    Write(Vec<u8>),
    /// 当前 Raft 领导者状态。
    Status(Status),
    /// 成员变更已提出（提交索引，可能是 Joint 条目索引）。
    ChangeMembership { index: Index },
// 结束当前作用域
}

/// Raft 集群状态，由领导者生成。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义数据结构
pub struct Status {
    /// 生成该状态的当前 Raft 领导者。
    pub leader: NodeID,
    /// 当前 Raft 任期。
    pub term: Term,
    /// 各节点的 match 索引，表示复制进度。使用 BTreeMap 以保证测试确定性。
    pub match_index: BTreeMap<NodeID, Index>,
    /// 当前 commit 索引。
    pub commit_index: Index,
    /// 当前 applied 索引。
    pub applied_index: Index,
    /// 日志存储引擎状态。
    pub storage: storage::Status,
    /// 当前生效的投票成员集合。
    // 缺省空集，兼容旧序列化载荷
    #[serde(default)]
    // 业务逻辑步骤
    pub voters: std::collections::BTreeSet<NodeID>,
// 结束当前作用域
}
