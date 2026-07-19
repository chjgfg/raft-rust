use std::cmp::{max, min};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;

use crossbeam::channel::Sender;
use itertools::Itertools as _;
use log::{debug, info};
use rand::RngExt as _;

use super::log::{Index, Log};
use super::message::{Envelope, Message, ReadSequence, Request, RequestID, Response, Status};
use super::state::State;
use super::{ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL, MAX_APPEND_ENTRIES};
use crate::errinput;
use crate::error::{Error, Result};

/// 节点 ID，在集群内唯一。启动时手动分配。
pub type NodeID = u8;

/// 领导者任期号。选举时单调递增。
pub type Term = u64;

/// 逻辑时钟间隔，以 tick 数量表示。
pub type Ticks = u8;

/// Raft 节点选项。
#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    /// 领导者心跳之间的 tick 数。
    pub heartbeat_interval: Ticks,
    /// 跟随者与候选人的随机选举超时范围。
    pub election_timeout_range: Range<Ticks>,
    /// 单条 Append 消息中最多发送的条目数。
    pub max_append_entries: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat_interval: HEARTBEAT_INTERVAL,
            election_timeout_range: ELECTION_TIMEOUT_RANGE,
            max_append_entries: MAX_APPEND_ENTRIES,
        }
    }
}

/// 具有动态角色的 Raft 节点。实现 Raft 分布式
/// 共识协议，详见 `raft` 模块文档。
///
/// 节点由 `step()` 处理入站消息、
/// 由 `tick()` 推进时间同步驱动。这些方法消费节点，
/// 并返回可能具有不同角色的新节点。出站消息
/// 经给定 `tx` 通道发送，须投递给同伴或客户端。
///
/// 该枚举是节点的公开接口，角色集合封闭。
/// 它包装实现实际逻辑的 `RawNode<Role>`。
/// 枚举可表示所有角色，因此角色转换时使用方便，
/// 例如：`node = node.step()?`。
pub enum Node {
    /// 候选人竞选领导者。
    Candidate(RawNode<Candidate>),
    /// 跟随者从领导者复制条目。
    Follower(RawNode<Follower>),
    /// 领导者处理客户端请求并向跟随者复制条目。
    Leader(RawNode<Leader>),
}

impl Node {
    /// 创建新的 Raft 节点。以无领导者的跟随者起步，等待
    /// 领导者消息，否则转为候选人并
    /// 竞选。单节点集群（无
    /// 同伴）创建时立即成为领导者。
    pub fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        log: Log,
        state: Box<dyn State>,
        tx: Sender<Envelope>,
        opts: Options,
    ) -> Result<Self> {
        let node = RawNode::new(id, peers, log, state, tx, opts)?;
        // 单节点集群立即成为领导者。
        if node.cluster_size() == 1 {
            return Ok(node.into_candidate()?.into_leader()?.into());
        }
        Ok(node.into())
    }

    /// 返回节点 ID。
    pub fn id(&self) -> NodeID {
        match self {
            Self::Candidate(node) => node.id,
            Self::Follower(node) => node.id,
            Self::Leader(node) => node.id,
        }
    }

    /// 返回节点任期。
    pub fn term(&self) -> Term {
        match self {
            Self::Candidate(node) => node.term(),
            Self::Follower(node) => node.term(),
            Self::Leader(node) => node.term(),
        }
    }

    /// 处理入站消息。
    pub fn step(self, msg: Envelope) -> Result<Self> {
        let peers = match &self {
            Self::Candidate(node) => &node.peers,
            Self::Follower(node) => &node.peers,
            Self::Leader(node) => &node.peers,
        };
        assert_eq!(msg.to, self.id(), "message to other node: {msg:?}");
        assert!(peers.contains(&msg.from) || msg.from == self.id(), "unknown sender: {msg:?}");
        debug!("Stepping {msg:?}");

        match self {
            Self::Candidate(node) => node.step(msg),
            Self::Follower(node) => node.step(msg),
            Self::Leader(node) => node.step(msg),
        }
    }

    /// 将时间推进一个 tick。
    pub fn tick(self) -> Result<Self> {
        match self {
            Self::Candidate(node) => node.tick(),
            Self::Follower(node) => node.tick(),
            Self::Leader(node) => node.tick(),
        }
    }
}

impl From<RawNode<Candidate>> for Node {
    fn from(node: RawNode<Candidate>) -> Self {
        Node::Candidate(node)
    }
}

impl From<RawNode<Follower>> for Node {
    fn from(node: RawNode<Follower>) -> Self {
        Node::Follower(node)
    }
}

impl From<RawNode<Leader>> for Node {
    fn from(node: RawNode<Leader>) -> Self {
        Node::Leader(node)
    }
}

/// Raft 角色的标记 trait：领导者、跟随者或候选人。
pub trait Role {}

/// 角色为 R 的 Raft 节点。
///
/// 采用 typestate 模式，各节点状态（角色）
/// 编码为 RawNode<Role>。参见 http://cliffle.com/blog/rust-typestate/。
pub struct RawNode<R: Role> {
    /// 节点 ID。在集群内必须唯一。
    id: NodeID,
    /// 集群中其它节点的 ID。运行期间
    /// 不变。重启时可变更，但所有节点必须有相同
    /// 节点集合，否则可能产生多领导者（脑裂）。
    peers: HashSet<NodeID>,
    /// Raft 日志，保存待执行的客户端命令。
    log: Log,
    /// Raft 状态机，从日志执行客户端命令。
    state: Box<dyn State>,
    /// 向其它节点发送出站消息的通道。
    tx: Sender<Envelope>,
    /// 节点选项。
    opts: Options,
    /// 角色相关状态。
    role: R,
}

impl<R: Role> RawNode<R> {
    /// 角色转换辅助方法。
    fn into_role<T: Role>(self, role: T) -> RawNode<T> {
        RawNode {
            id: self.id,
            peers: self.peers,
            log: self.log,
            state: self.state,
            tx: self.tx,
            opts: self.opts,
            role,
        }
    }

    /// 返回节点当前任期。
    fn term(&self) -> Term {
        self.log.get_term_vote().0
    }

    /// 返回集群节点数。
    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// 返回集群法定人数（严格多数）。
    fn quorum_size(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// 返回给定未排序向量的法定人数值（即中位数）。
    /// 长度必须等于集群大小。
    fn quorum_value<T: Ord + Copy>(&self, mut values: Vec<T>) -> T {
        assert_eq!(values.len(), self.cluster_size(), "vector size must match cluster size");
        *values.select_nth_unstable_by(self.quorum_size() - 1, |a, b| a.cmp(b).reverse()).1
    }

    /// 生成随机选举超时。
    fn random_election_timeout(&self) -> Ticks {
        rand::rng().random_range(self.opts.election_timeout_range.clone())
    }

    /// 向给定接收方发送消息。
    fn send(&self, to: NodeID, message: Message) -> Result<()> {
        Self::send_via(&self.tx, Envelope { from: self.id, to, term: self.term(), message })
    }

    /// 经给定通道发送消息。避免借用 self，
    /// 以便在持有 self 部分借用时仍可发送。
    fn send_via(tx: &Sender<Envelope>, msg: Envelope) -> Result<()> {
        debug!("Sending {msg:?}");
        Ok(tx.send(msg)?)
    }

    /// 向所有同伴广播消息。
    fn broadcast(&self, message: Message) -> Result<()> {
        // 按 ID 升序发送，保证测试确定性。
        for id in self.peers.iter().copied().sorted() {
            self.send(id, message.clone())?;
        }
        Ok(())
    }
}

/// 跟随者从领导者复制日志，并将客户端请求
/// 转发给它。节点以无领导者的跟随者起步，直到发现
/// 领导者或发起选举。
pub struct Follower {
    /// 领导者；若为无领导者的跟随者则为 None。
    leader: Option<NodeID>,
    /// 自上次收到领导者消息以来经过的 tick 数。
    leader_seen: Ticks,
    /// 触发选举前的 leader_seen 超时。
    election_timeout: Ticks,
    // 已转发给领导者的本地客户端请求。
    // 领导者/任期变更时会中止。
    forwarded: HashSet<RequestID>,
}

impl Follower {
    /// 创建新的跟随者角色。
    fn new(leader: Option<NodeID>, election_timeout: Ticks) -> Self {
        Self { leader, leader_seen: 0, election_timeout, forwarded: HashSet::new() }
    }
}

impl Role for Follower {}

impl RawNode<Follower> {
    /// 创建无领导者的跟随者节点。
    fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        log: Log,
        state: Box<dyn State>,
        tx: Sender<Envelope>,
        opts: Options,
    ) -> Result<Self> {
        if peers.contains(&id) {
            return errinput!("node ID {id} can't be in peers");
        }
        let role = Follower::new(None, 0);
        let mut node = Self { id, peers, log, state, tx, opts, role };
        node.role.election_timeout = node.random_election_timeout();

        // 重启后应用待处理条目。状态机写入
        // 不刷持久存储，主机崩溃或重启可能丢失
        // 尾部写入。Raft 日志是持久的，总可
        // 从中恢复状态。此处重新应用
        // 缺失条目。
        node.maybe_apply()?;
        Ok(node)
    }

    /// 将跟随者转为候选人，通过
    /// 在新任期竞选领导者。
    fn into_candidate(mut self) -> Result<RawNode<Candidate>> {
        // 中止所有已转发请求。须向新领导者重试。
        self.abort_forwarded()?;

        // 应用待处理日志，以便若当选时已追上。
        self.maybe_apply()?;

        // 成为候选人并发起竞选。
        let election_timeout = self.random_election_timeout();
        let mut node = self.into_role(Candidate::new(election_timeout));
        node.campaign()?;

        let (term, vote) = node.log.get_term_vote();
        assert!(node.role.votes.contains(&node.id), "candidate did not vote for self");
        assert_ne!(term, 0, "candidate can't have term 0");
        assert_eq!(vote, Some(node.id), "log vote does not match self");

        Ok(node)
    }

    /// 将跟随者转为新任期的无领导者跟随者
    /// （例如有人发起新选举），或转为当前领导者的跟随者。
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        assert_ne!(term, 0, "can't become follower in term 0");

        // 中止所有已转发请求。须向新领导者重试。
        self.abort_forwarded()?;

        if let Some(leader) = leader {
            // 在当前任期发现领导者。
            assert!(self.peers.contains(&leader), "leader is not a peer");
            assert_eq!(self.role.leader, None, "already have leader in term");
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Following leader {leader} in term {term}");
            self.role = Follower::new(Some(leader), self.role.election_timeout);
        } else {
            // 发现新任期，但尚不知领导者是谁。
            // 处理来自它的消息时会得知。
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term_vote(term, None)?;
            self.role = Follower::new(None, self.random_election_timeout());
        }
        Ok(self)
    }

    /// 处理入站消息。
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // 过去任期：过时同伴，丢弃消息。
        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        // 未来任期：更新的领导者或候选人，成为无领导者跟随者
        // 并处理该消息。
        if msg.term > self.term() {
            return self.into_follower(msg.term, None)?.step(msg);
        }

        // 记录上次收到领导者消息的时间（若有）。
        if Some(msg.from) == self.role.leader {
            self.role.leader_seen = 0
        }

        match msg.message {
            // 领导者发送周期性心跳。若尚无领导者
            // 则跟随它。若 commit_index 推进则应用命令。
            Message::Heartbeat { last_index, commit_index, read_seq } => {
                assert!(commit_index <= last_index, "commit_index after last_index");

                // 确认心跳来自我们的领导者，否则跟随它。
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }

                // 检查本地日志到 last_index 是否与领导者匹配，
                // 并响应心跳。last_index 总是
                // 领导者任期，因为领导者只在本任期追加条目。
                let match_index = if self.log.has(last_index, msg.term)? { last_index } else { 0 };
                self.send(msg.from, Message::HeartbeatResponse { match_index, read_seq })?;

                // 推进 commit 索引并应用条目。仅当
                // 匹配领导者 last_index 时才能做，这意味着
                // 日志到 match_index 一致，也意味着
                // commit_index 在本地日志中。
                if match_index != 0 && commit_index > self.log.get_commit_index().0 {
                    self.log.commit(commit_index)?;
                    self.maybe_apply()?;
                }
            }

            // 将领导者的日志条目追加到本地日志。
            Message::Append { base_index, base_term, entries } => {
                if let Some(first) = entries.first() {
                    assert_eq!(base_index, first.index - 1, "base index mismatch");
                }

                // 确认 append 来自我们的领导者，否则跟随它。
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }

                // 若 base 条目匹配本地日志，则追加条目。
                if base_index == 0 || self.log.has(base_index, base_term)? {
                    let match_index = entries.last().map(|e| e.index).unwrap_or(base_index);
                    self.log.splice(entries)?;
                    self.send(msg.from, Message::AppendResponse { match_index, reject_index: 0 })?;
                } else {
                    // 否则拒绝 base 索引。若本地日志
                    // 短于 base 索引，降低 reject 索引以
                    // 跳过缺失条目。
                    let reject_index = min(base_index, self.log.get_last_index().0 + 1);
                    self.send(msg.from, Message::AppendResponse { reject_index, match_index: 0 })?;
                }
            }

            // 确认领导者的读序列号。
            Message::Read { seq } => {
                // 确认读请求来自我们的领导者，否则跟随它。
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }

                // 确认读。
                self.send(msg.from, Message::ReadResponse { seq })?;
            }

            // 候选人请求我们的选票。每个任期只投一票。
            Message::Campaign { last_index, last_term } => {
                // 若本任期已投给他人则不再投票。
                // 可对同一节点重复投票。
                if let (_, Some(vote)) = self.log.get_term_vote()
                    && msg.from != vote
                {
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    return Ok(self.into());
                }

                // 仅当候选人日志至少与我们一样新时才投票。
                // 任何法定人数中至少有一个节点拥有全部已提交
                // 条目，这保证只选出拥有全部已提交条目的
                // 领导者。见论文 5.4.1 节。
                let (log_index, log_term) = self.log.get_last_index();
                if log_term > last_term || log_term == last_term && log_index > last_index {
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    return Ok(self.into());
                }

                // 授予选票。
                info!("Voting for {} in term {} election", msg.from, msg.term);
                self.log.set_term_vote(msg.term, Some(msg.from))?;
                self.send(msg.from, Message::CampaignResponse { vote: true })?;
            }

            // 将客户端请求转发给领导者；若无领导者
            // 则中止。不会内部重试，客户端应使用超时。
            // 本地客户端请求以本节点 ID 为发送方。
            Message::ClientRequest { id, request: _ } => {
                assert_eq!(msg.from, self.id, "client request from other node");

                if let Some(leader) = self.role.leader {
                    debug!("Forwarding request to leader {leader}: {msg:?}");
                    self.role.forwarded.insert(id);
                    self.send(leader, msg.message)?
                } else {
                    let response = Err(Error::Abort);
                    self.send(msg.from, Message::ClientResponse { id, response })?
                }
            }

            // 来自领导者的客户端响应转交给客户端。
            Message::ClientResponse { id, response } => {
                assert_eq!(Some(msg.from), self.role.leader, "client response from non-leader");

                if self.role.forwarded.remove(&id) {
                    self.send(self.id, Message::ClientResponse { id, response })?;
                }
            }

            // 选举失败后仍可能收到选票，忽略。
            Message::CampaignResponse { .. } => {}

            // 本任期不是领导者，不应收到这些消息。
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ReadResponse { .. } => {
                panic!("follower received unexpected message {msg:?}")
            }
        };
        Ok(self.into())
    }

    /// 处理一个逻辑时钟 tick。
    fn tick(mut self) -> Result<Node> {
        // 若一段时间未收到领导者消息则发起竞选。
        self.role.leader_seen += 1;
        if self.role.leader_seen >= self.role.election_timeout {
            return Ok(self.into_candidate()?.into());
        }
        Ok(self.into())
    }

    /// 中止所有已转发请求（例如任期/领导者变更时）。
    fn abort_forwarded(&mut self) -> Result<()> {
        // 按 ID 排序以保证测试确定性。
        for id in std::mem::take(&mut self.role.forwarded).into_iter().sorted() {
            debug!("Aborting forwarded request {id}");
            self.send(self.id, Message::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// 应用所有待处理日志条目。
    fn maybe_apply(&mut self) -> Result<()> {
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            // 丢弃结果，因为只有领导者向客户端响应。
            // 错误也一样——任何非确定性错误（如 IO
            // 错误）必须 panic，以免节点分叉。
            _ = self.state.apply(entry);
        }
        Ok(())
    }
}

/// 候选人正在竞选成为领导者。
pub struct Candidate {
    /// 已收到的选票（含自己）。
    votes: HashSet<NodeID>,
    /// 自选举开始经过的 tick 数。
    election_duration: Ticks,
    /// 选举超时（以 tick 计）。
    election_timeout: Ticks,
}

impl Candidate {
    /// 创建新的候选人角色。
    fn new(election_timeout: Ticks) -> Self {
        Self { votes: HashSet::new(), election_duration: 0, election_timeout }
    }
}

impl Role for Candidate {}

impl RawNode<Candidate> {
    /// 将候选人转为跟随者。要么落选
    /// 跟随胜者，要么发现新任期并以
    /// 无领导者跟随者进入。
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        let election_timeout = self.random_election_timeout();
        if let Some(leader) = leader {
            // 落选，跟随胜者。
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Lost election, following leader {leader} in term {term}");
            Ok(self.into_role(Follower::new(Some(leader), election_timeout)))
        } else {
            // 发现新任期，但尚不一定知道领导者
            // 是谁。处理来自它的消息时会得知。
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term_vote(term, None)?;
            Ok(self.into_role(Follower::new(None, election_timeout)))
        }
    }

    /// 将候选人转为领导者。我们赢得了选举。
    fn into_leader(self) -> Result<RawNode<Leader>> {
        let (term, vote) = self.log.get_term_vote();
        assert_ne!(term, 0, "leaders can't have term 0");
        assert_eq!(vote, Some(self.id), "leader did not vote for self");

        info!("Won election for term {term}, becoming leader");
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));

        // 就任时提出空命令，以消除
        // 日志中此前条目的歧义。见论文 5.4.2 节。
        // 在心跳之前做，以免
        // 心跳响应显示同伴落后时浪费一轮复制。
        node.propose(None)?;
        node.maybe_commit_and_apply()?;
        node.heartbeat()?;

        Ok(node)
    }

    /// 处理入站消息。
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // 过去任期：过时同伴，丢弃消息。
        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        // 未来任期：更新的领导者或候选人，成为无领导者跟随者
        // 并处理该消息。
        if msg.term > self.term() {
            return self.into_follower(msg.term, None)?.step(msg);
        }

        match msg.message {
            // 若收到选票则记录。若达到法定人数
            // 则就任领导者。
            Message::CampaignResponse { vote: true } => {
                self.role.votes.insert(msg.from);
                if self.role.votes.len() >= self.quorum_size() {
                    return Ok(self.into_leader()?.into());
                }
            }

            // 未获得该选票。
            Message::CampaignResponse { vote: false } => {}

            // 不给其它候选人投票。
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // 若本任期听到领导者，则落选。
            // 跟随它并处理该消息。
            Message::Heartbeat { .. } | Message::Append { .. } | Message::Read { .. } => {
                return self.into_follower(msg.term, Some(msg.from))?.step(msg);
            }

            // 竞选期间中止客户端请求。客户端必须重试。
            Message::ClientRequest { id, request: _ } => {
                self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
            }

            // 本任期不是领导者，也不转发请求，
            // 不应收到这些。
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ReadResponse { .. }
            | Message::ClientResponse { .. } => panic!("unexpected message {msg:?}"),
        }
        Ok(self.into())
    }

    /// 处理一个逻辑时钟 tick。
    fn tick(mut self) -> Result<Node> {
        // 若无人赢得本次选举，稍后开始新选举。
        self.role.election_duration += 1;
        if self.role.election_duration >= self.role.election_timeout {
            self.campaign()?;
        }
        Ok(self.into())
    }

    /// 通过提升任期、投自己、
    /// 向所有同伴拉票发起新选举。
    fn campaign(&mut self) -> Result<()> {
        let term = self.term() + 1;
        info!("Starting new election for term {term}");
        self.role = Candidate::new(self.random_election_timeout());
        self.role.votes.insert(self.id); // 投自己
        self.log.set_term_vote(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })
    }
}

/// 领导者服务客户端请求并向跟随者复制日志。
/// 若失去领导权，所有客户端请求被中止。
pub struct Leader {
    /// 跟随者复制进度。
    progress: HashMap<NodeID, Progress>,
    /// 按日志索引跟踪待处理写请求。写被
    /// 提出并追加到领导者日志时加入，命令
    /// 应用到状态机并向客户端返回结果时移除。
    writes: HashMap<Index, Write>,
    /// 跟踪待处理读请求。为保证线性一致性，读请求
    /// 分配序列号，仅在多数节点确认
    /// 我们仍是领导者后才执行。否则旧领导者可能
    /// 在别处已选出新领导者时提供陈旧读。
    reads: VecDeque<Read>,
    /// 上次读使用的读序列号。本任期
    /// 初始化为 0，每个读命令递增。
    read_seq: ReadSequence,
    /// 自上次心跳以来的 tick 数。
    since_heartbeat: Ticks,
}

/// 每个跟随者的复制进度（本任期内）。
struct Progress {
    /// 已知跟随者日志与领导者匹配的最高索引。
    /// 初始化为 0，单调递增。
    match_index: Index,
    /// 向跟随者复制的下一索引。初始化为
    /// last_index+1，探测日志不匹配时减小。始终在
    /// [match_index+1, last_index+1] 内。
    ///
    /// 尚未发送的条目在 [next_index, last_index]。
    /// 尚未确认的条目在 [match_index+1, next_index)。
    next_index: Index,
    /// 该跟随者确认的最后读序列号。为避免
    /// 领导者变更时的陈旧读，读仅在其序列号
    /// 被多数确认后才服务。
    read_seq: ReadSequence,
}

impl Progress {
    /// 尝试推进跟随者的 match 索引，成功则返回 true。
    /// 若 next_index 低于它，则推进到下一索引。
    fn advance(&mut self, match_index: Index) -> bool {
        if match_index <= self.match_index {
            return false;
        }
        self.match_index = match_index;
        self.next_index = max(self.next_index, match_index + 1);
        true
    }

    /// 尝试推进跟随者的 read_seq，成功则返回 true。
    fn advance_read(&mut self, read_seq: ReadSequence) -> bool {
        if read_seq <= self.read_seq {
            return false;
        }
        self.read_seq = read_seq;
        true
    }

    /// 尝试将跟随者的 next 索引回退到给定索引，
    /// 成功则返回 true。不会回退到 match_index + 1 以下。
    fn regress_next(&mut self, next_index: Index) -> bool {
        if next_index >= self.next_index || self.next_index <= self.match_index + 1 {
            return false;
        }
        self.next_index = max(next_index, self.match_index + 1);
        true
    }
}

/// 待处理的客户端写请求。
struct Write {
    /// 提交该写的节点。
    from: NodeID,
    /// 写请求 ID。
    id: RequestID,
}

/// 待处理的客户端读请求。
struct Read {
    /// 本次读的序列号。
    seq: ReadSequence,
    /// 提交该读的节点。
    from: NodeID,
    /// 读请求 ID。
    id: RequestID,
    /// 读命令。
    command: Vec<u8>,
}

impl Leader {
    /// 创建新的领导者角色。
    fn new(peers: HashSet<NodeID>, last_index: Index) -> Self {
        let next_index = last_index + 1;
        let progress = peers
            .into_iter()
            .map(|p| (p, Progress { next_index, match_index: 0, read_seq: 0 }))
            .collect();
        Self {
            progress,
            writes: HashMap::new(),
            reads: VecDeque::new(),
            read_seq: 0,
            since_heartbeat: 0,
        }
    }
}

impl Role for Leader {}

impl RawNode<Leader> {
    /// 将领导者转为跟随者。仅在
    /// 发现新任期时发生，因此成为无领导者跟随者。
    /// 处理收到的消息后可能跟随新领导者（若有）。
    fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        assert!(term > self.term(), "leader can only become follower in later term");
        info!("Discovered new term {term}");

        // 中止在途请求。客户端必须重试。按
        // ID 排序以保证测试确定性。
        for write in std::mem::take(&mut self.role.writes).into_values().sorted_by_key(|w| w.id) {
            let response = Err(Error::Abort);
            self.send(write.from, Message::ClientResponse { id: write.id, response })?;
        }
        for read in std::mem::take(&mut self.role.reads).into_iter().sorted_by_key(|r| r.id) {
            let response = Err(Error::Abort);
            self.send(read.from, Message::ClientResponse { id: read.id, response })?;
        }

        self.log.set_term_vote(term, None)?;
        let election_timeout = self.random_election_timeout();
        Ok(self.into_role(Follower::new(None, election_timeout)))
    }

    /// 处理入站消息。
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // 过去任期：过时同伴，丢弃消息。
        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        // 未来任期：成为无领导者跟随者并处理该消息。
        if msg.term > self.term() {
            return self.into_follower(msg.term)?.step(msg);
        }

        match msg.message {
            // 跟随者收到我们的心跳并确认领导权。
            // 可能可执行新读，也可能发现
            // 跟随者日志落后需要追赶。
            Message::HeartbeatResponse { match_index, read_seq } => {
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");
                assert!(read_seq <= self.role.read_seq, "future read sequence number");

                // 若读序列号推进，尝试执行读。
                if self.progress(msg.from).advance_read(read_seq) {
                    self.maybe_read()?;
                }

                // 若跟随者未匹配我们的 last_index，则对其
                // append 失败（或正在追赶）。探测以找到
                // 匹配条目并开始复制。将 next_index 回退
                // 到 last_index，因为跟随者刚告诉我们没有
                // 它（或此前的 last_index）。
                if match_index == 0 {
                    self.progress(msg.from).regress_next(last_index);
                    self.maybe_send_append(msg.from, true)?;
                }

                // 若跟随者 match 索引推进，说明某次 append 响应
                // 丢失。尝试提交并应用。
                //
                // 不必急切发送待发条目：此心跳之后的
                // 提案在稳态下应已急切复制。
                // 否则下次
                // 心跳会触发上面的探测。
                if self.progress(msg.from).advance(match_index) {
                    self.maybe_commit_and_apply()?;
                }
            }

            // 跟随者追加了我们的日志（或探测找到匹配）。
            // 记录进度并尝试提交应用。
            Message::AppendResponse { match_index, reject_index: 0 } if match_index > 0 => {
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");

                if self.progress(msg.from).advance(match_index) {
                    self.maybe_commit_and_apply()?;
                }

                // 急切发送后续待发条目。可能是
                // 成功的探测响应，或同伴落后，
                // 我们每次以 MAX_APPEND_ENTRIES 一批追赶。
                self.maybe_send_append(msg.from, false)?;
            }

            // 跟随者确认了我们的读序列号。若推进，
            // 则尝试执行读。
            Message::ReadResponse { seq } => {
                if self.progress(msg.from).advance_read(seq) {
                    self.maybe_read()?;
                }
            }

            // 跟随者拒绝 append，因
            // reject_index 处 base 条目不匹配其日志。通过
            // 发送空 append 探测前一条，直到找到公共 base。
            //
            // 对长分叉日志线性探测可能较慢，但为
            // 简单起见如此。见论文 5.3 节。
            Message::AppendResponse { reject_index, match_index: 0 } if reject_index > 0 => {
                let (last_index, _) = self.log.get_last_index();
                assert!(reject_index <= last_index, "future reject index");

                // 若被拒 base 索引不高于 match 索引，
                // 则拒绝已过时，可忽略。
                if reject_index <= self.progress(msg.from).match_index {
                    return Ok(self.into());
                }

                // 若 next_index 尚未低于 reject 索引，
                // 则在其下探测。避免重复探测
                // （丢失时心跳会触发重试）。
                if self.progress(msg.from).regress_next(reject_index) {
                    self.maybe_send_append(msg.from, true)?;
                }
            }

            // AppendResponse 必须设置 match_index 或 reject_index 之一。
            Message::AppendResponse { .. } => panic!("invalid message {msg:?}"),

            // 客户端提交写请求。提出它，待
            // 复制并应用到状态机后再
            // 返回响应给客户端。
            Message::ClientRequest { id, request: Request::Write(command) } => {
                let index = self.propose(Some(command))?;
                self.role.writes.insert(index, Write { from: msg.from, id });
                if self.cluster_size() == 1 {
                    self.maybe_commit_and_apply()?;
                }
            }

            // 客户端提交读请求。为保证线性一致性，
            // 须发送读序列号并等待多数确认
            // 我们仍是领导者。
            Message::ClientRequest { id, request: Request::Read(command) } => {
                self.role.read_seq += 1;
                let read = Read { seq: self.role.read_seq, from: msg.from, id, command };
                self.role.reads.push_back(read);
                self.broadcast(Message::Read { seq: self.role.read_seq })?;
                if self.cluster_size() == 1 {
                    self.maybe_read()?;
                }
            }

            // 客户端提交状态查询。
            Message::ClientRequest { id, request: Request::Status } => {
                let response = self.status().map(Response::Status);
                self.send(msg.from, Message::ClientResponse { id, response })?;
            }

            // 不授予任何选票（已投给自己）。
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // 当选后仍可能收到选票，忽略。
            Message::CampaignResponse { .. } => {}

            // 本任期不能有另一领导者。
            Message::Heartbeat { .. } | Message::Append { .. } | Message::Read { .. } => {
                panic!("saw other leader {} in term {}", msg.from, msg.term);
            }

            // 领导者不代理客户端请求。
            Message::ClientResponse { .. } => panic!("unexpected message {msg:?}"),
        }

        Ok(self.into())
    }

    /// 处理一个逻辑时钟 tick。
    fn tick(mut self) -> Result<Node> {
        // 发送周期性心跳。
        self.role.since_heartbeat += 1;
        if self.role.since_heartbeat >= self.opts.heartbeat_interval {
            self.heartbeat()?;
        }
        Ok(self.into())
    }

    /// 向所有同伴广播心跳。
    fn heartbeat(&mut self) -> Result<()> {
        let (last_index, last_term) = self.log.get_last_index();
        let (commit_index, _) = self.log.get_commit_index();
        let read_seq = self.role.read_seq;
        assert_eq!(last_term, self.term(), "leader's last_term not in current term");

        self.role.since_heartbeat = 0;
        self.broadcast(Message::Heartbeat { last_index, commit_index, read_seq })
    }

    /// 通过追加到本地日志并
    /// 复制给同伴，提出命令以求共识。成功后最终会
    /// 提交并应用到状态机。
    fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(command)?;
        for peer in self.peers.iter().copied().sorted() {
            // 若同伴处于稳态且
            // 此前条目已发送，则急切发送该条目。否则同伴落后，
            // 我们在探测过去条目以寻找匹配。
            if index == self.progress(peer).next_index {
                self.maybe_send_append(peer, false)?;
            }
        }
        Ok(index)
    }

    /// 提交已复制到法定人数的新条目，并
    /// 应用到状态机，向客户端返回结果。
    fn maybe_commit_and_apply(&mut self) -> Result<Index> {
        // 按法定人数确定新的 commit 索引。
        let (last_index, _) = self.log.get_last_index();
        let commit_index = self.quorum_value(
            self.role.progress.values().map(|p| p.match_index).chain([last_index]).collect(),
        );

        // 若 commit 索引未推进则不做。不断言，
        // 因为法定人数值可能回退，例如重启或
        // 领导者变更后跟随者 match 索引初始化为 0。
        let (old_index, old_term) = self.log.get_commit_index();
        if commit_index <= old_index {
            return Ok(old_index);
        }

        // 只能安全提交本任期的条目（见
        // 论文 5.4.2 节）。
        match self.log.get(commit_index)? {
            Some(entry) if entry.term == self.term() => {}
            Some(_) => return Ok(old_index),
            None => panic!("commit index {commit_index} missing"),
        }

        // 提交条目。
        self.log.commit(commit_index)?;

        // 应用条目并响应客户端。
        let term = self.term();
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            let write = self.role.writes.remove(&entry.index);
            let result = self.state.apply(entry);

            if let Some(Write { id, from: to }) = write {
                let message = Message::ClientResponse { id, response: result.map(Response::Write) };
                Self::send_via(&self.tx, Envelope { from: self.id, term, to, message })?;
            }
        }
        drop(iter);

        // 若 commit 任期变化，可能有读在等待我们
        // 提交并应用本任期条目。执行它们。
        if old_term != self.term() {
            self.maybe_read()?;
        }

        Ok(commit_index)
    }

    /// 执行已就绪的读请求（多数已确认
    /// 我们在这些读序列上仍是领导者）。
    fn maybe_read(&mut self) -> Result<()> {
        if self.role.reads.is_empty() {
            return Ok(());
        }

        // 仅当已提交并应用本任期
        // 条目时读才安全（领导者当选时会追加一条）。否则
        // 应用可能落后，会提供陈旧读。
        let (commit_index, commit_term) = self.log.get_commit_index();
        let applied_index = self.state.get_applied_index();
        if commit_term < self.term() || applied_index < commit_index {
            return Ok(());
        }

        // 确定多数确认的最大读序列号。
        let quorum_read_seq = self.quorum_value(
            self.role.progress.values().map(|p| p.read_seq).chain([self.role.read_seq]).collect(),
        );

        // 执行就绪读。VecDeque 按 read_seq 有序，
        // 可一直取到 quorum_read_seq。
        while let Some(read) = self.role.reads.front() {
            if read.seq > quorum_read_seq {
                break;
            }
            let read = self.role.reads.pop_front().unwrap();
            let response = self.state.read(read.command).map(Response::Read);
            self.send(read.from, Message::ClientResponse { id: read.id, response })?;
        }
        Ok(())
    }

    /// 向跟随者发送一批待发日志，范围
    /// [next_index, last_index]，受 max_append_entries 限制。
    ///
    /// 若 probe 为 true，则在寻找跟随者日志与我们匹配的
    /// 索引。发送 base_index 为 next_index-1 的
    /// 空 append 探测。若跟随者确认 base_index
    /// 匹配其日志，下次发送实际条目；否则
    /// 递减 next_index 再探测，直到找到匹配。
    /// 见论文 5.3 节。
    ///
    /// 若跟随者已追上（按
    /// match_index 与 last_index），则跳过探测。若探测的 base_index 已由
    /// match_index 确认，则改为发送实际 append。
    fn maybe_send_append(&mut self, peer: NodeID, mut probe: bool) -> Result<()> {
        let (last_index, _) = self.log.get_last_index();
        let progress = self.role.progress.get_mut(&peer).expect("unknown node");
        assert_ne!(progress.next_index, 0, "invalid next_index");
        assert!(progress.next_index > progress.match_index, "invalid next_index <= match_index");
        assert!(progress.match_index <= last_index, "invalid match_index > last_index");
        assert!(progress.next_index <= last_index + 1, "invalid next_index > last_index + 1");

        // 若同伴已追上，不必发送 append。
        if progress.match_index == last_index {
            return Ok(());
        }

        // 若请求探测，但 base_index 已由
        // match_index 确认，则不必探测，直接
        // 发送条目。
        probe = probe && progress.next_index > progress.match_index + 1;

        // 若无待发条目且非探测，则在
        // 收到跟随者响应前无更多可发。
        if progress.next_index > last_index && !probe {
            return Ok(());
        }

        // 获取 base 与条目。
        let (base_index, base_term) = match progress.next_index {
            0 => panic!("next_index=0 for node {peer}"),
            1 => (0, 0), // 第一条，无 base
            next => self.log.get(next - 1)?.map(|e| (e.index, e.term)).expect("missing base entry"),
        };
        let entries = match probe {
            false => self
                .log
                .scan(progress.next_index..)
                .take(self.opts.max_append_entries)
                .try_collect()?,
            true => Vec::new(),
        };

        // 乐观假设跟随者会接受这些条目，
        // 提升 next_index 以免在响应前重发。
        if let Some(last) = entries.last() {
            progress.next_index = last.index + 1;
        }

        debug!("Replicating {} entries with base {base_index} to {peer}", entries.len());
        self.send(peer, Message::Append { base_index, base_term, entries })
    }

    /// 生成集群状态。
    fn status(&mut self) -> Result<Status> {
        Ok(Status {
            leader: self.id,
            term: self.term(),
            match_index: self
                .role
                .progress
                .iter()
                .map(|(id, p)| (*id, p.match_index))
                .chain(std::iter::once((self.id, self.log.get_last_index().0)))
                .collect(),
            commit_index: self.log.get_commit_index().0,
            applied_index: self.state.get_applied_index(),
            storage: self.log.status()?,
        })
    }

    /// 返回节点进度的可变借用。便捷方法。
    fn progress(&mut self, id: NodeID) -> &mut Progress {
        self.role.progress.get_mut(&id).expect("unknown node")
    }
}
