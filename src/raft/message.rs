use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Entry, Index, NodeID, Term};
use crate::encoding;
use crate::error::Result;
use crate::storage;

/// 消息信封，标明发送方与接收方。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    /// 发送方。
    pub from: NodeID,
    /// 发送方当前任期。
    pub term: Term,
    /// 接收方。
    pub to: NodeID,
    /// 消息本体。
    pub message: Message,
}

impl encoding::Value for Envelope {}

/// Raft 节点之间发送的消息。消息异步发送（非请求/响应模式），可能丢失或乱序。
///
/// 实践中它们经 TCP 连接与 crossbeam channel 传递；只要连接保持，通常不会丢失或乱序。
/// 一条消息及其响应走各自独立的出站 TCP 连接。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// 候选人向同伴拉票竞选领导者。
    /// 仅当候选人的日志至少与投票者一样新时才会被授予选票。
    Campaign {
        /// 候选人最后一条日志的索引。
        last_index: Index,
        /// 候选人最后一条日志的任期。
        last_term: Term,
    },

    /// 跟随者每个任期只能投一票，且仅当候选人的日志至少与自己一样新时才投票。
    /// 候选人隐式投票给自己。
    CampaignResponse {
        /// 为 true 表示授予选票。false 响应并非必须，但为清晰起见仍会发出。
        vote: bool,
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
        read_seq: ReadSequence,
    },

    /// 跟随者在仍认可其为领导者时，对心跳作出响应。
    HeartbeatResponse {
        /// 非零表示心跳中的 last_index 与跟随者日志匹配；否则跟随者日志分叉或落后。
        match_index: Index,
        /// 心跳中的读序列号。
        read_seq: ReadSequence,
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
        entries: Vec<Entry>,
    },

    /// 跟随者根据 base 条目是否匹配本地日志，接受或拒绝领导者的 append。
    AppendResponse {
        /// 非零表示跟随者已追加到该索引（此前日志与领导者一致）。
        /// 若未发送条目（探测），则为匹配的 base 索引。
        match_index: Index,
        /// 非零表示在该 base 索引处拒绝（base 索引/任期不匹配）。
        /// 若本地日志短于 base 索引，reject 会降到 last_index+1，避免逐个探测缺失索引。
        reject_index: Index,
    },

    /// 领导者在提供读服务前需确认自己仍是领导者，以保证线性一致性
    ///（防止别处已选出新领导者）。读请求在序列号被多数派确认后才执行。
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
    },

    /// 客户端响应。
    ClientResponse {
        /// 对应原始 ClientRequest 的 ID。
        id: RequestID,
        /// 响应，或错误。
        response: Result<Response>,
    },
}

/// 客户端请求 ID。在途期间必须全局唯一。
///
/// 为简单起见使用随机 UUIDv4。也可加入节点/进程/MAC 与时间戳以更好避撞
///（例如 UUIDv6），但在此规模下不必。
pub type RequestID = uuid::Uuid;

/// 读序列号，用于为线性一致读确认领导权。
pub type ReadSequence = u64;

/// 客户端请求，通常透传给状态机。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    /// 状态机读命令，经 `State::read` 执行。不复制，仅在领导者上求值。
    Read(Vec<u8>),
    /// 状态机写命令，经 `State::apply` 执行。复制到所有节点，结果必须确定。
    Write(Vec<u8>),
    /// 向领导者查询 Raft 集群状态。
    Status,
}

impl encoding::Value for Request {}

/// 客户端响应。外层用 Result 表示错误。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// 状态机读结果。
    Read(Vec<u8>),
    /// 状态机写结果。
    Write(Vec<u8>),
    /// 当前 Raft 领导者状态。
    Status(Status),
}

impl encoding::Value for Response {}

/// Raft 集群状态，由领导者生成。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
}
