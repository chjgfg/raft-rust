use std::cmp::{max, min};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Range;

use crossbeam::channel::Sender;
use itertools::Itertools as _;
use log::{debug, info, warn};
use rand::RngExt as _;

use super::log::{Entry, Index, Log};
use super::membership::{Membership, MembershipEntry, MembershipState};
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
    /// 启用 Pre-vote：真选举前先确认多数，避免分区节点抬升任期。
    pub pre_vote: bool,
    /// 启用 CheckQuorum：领导者在无法联系多数时下台。
    pub check_quorum: bool,
    /// 距上次快照 apply 了多少条后触发本地快照；0 表示关闭。
    pub snapshot_threshold: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat_interval: HEARTBEAT_INTERVAL,
            election_timeout_range: ELECTION_TIMEOUT_RANGE,
            max_append_entries: MAX_APPEND_ENTRIES,
            pre_vote: true,
            check_quorum: true,
            snapshot_threshold: 0, // 默认关闭，由节点配置开启
        }
    }
}

/// 具有动态角色的 Raft 节点。
pub enum Node {
    /// 候选人（含 Pre-vote 相位）。
    Candidate(RawNode<Candidate>),
    /// 跟随者。
    Follower(RawNode<Follower>),
    /// 领导者。
    Leader(RawNode<Leader>),
}

impl Node {
    /// 创建新的 Raft 节点。`peers` 为除自身外的初始投票同伴。
    pub fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        log: Log,
        state: Box<dyn State>,
        tx: Sender<Envelope>,
        opts: Options,
    ) -> Result<Self> {
        let node = RawNode::new(id, peers, log, state, tx, opts)?;
        if node.cluster_size() == 1 {
            // 单节点：跳过 pre-vote，直接竞选并当选。
            return Ok(node.into_candidate(false)?.into_leader()?.into());
        }
        Ok(node.into())
    }

    pub fn id(&self) -> NodeID {
        match self {
            Self::Candidate(node) => node.id,
            Self::Follower(node) => node.id,
            Self::Leader(node) => node.id,
        }
    }

    pub fn term(&self) -> Term {
        match self {
            Self::Candidate(node) => node.term(),
            Self::Follower(node) => node.term(),
            Self::Leader(node) => node.term(),
        }
    }

    /// 当前生效的投票成员（含自身）。
    pub fn voters(&self) -> BTreeSet<NodeID> {
        match self {
            Self::Candidate(node) => node.membership.all_voters(),
            Self::Follower(node) => node.membership.all_voters(),
            Self::Leader(node) => node.membership.all_voters(),
        }
    }

    pub fn step(self, msg: Envelope) -> Result<Self> {
        assert_eq!(msg.to, self.id(), "message to other node: {msg:?}");

        // 允许来自当前（可能 joint）配置中的成员或自身；未知发送方丢弃。
        let known = match &self {
            Self::Candidate(node) => node.is_known_sender(msg.from),
            Self::Follower(node) => node.is_known_sender(msg.from),
            Self::Leader(node) => node.is_known_sender(msg.from),
        };
        if !known {
            warn!("Dropping message from unknown sender: {msg:?}");
            return Ok(self);
        }
        debug!("Stepping {msg:?}");

        match self {
            Self::Candidate(node) => node.step(msg),
            Self::Follower(node) => node.step(msg),
            Self::Leader(node) => node.step(msg),
        }
    }

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

pub trait Role {}

pub struct RawNode<R: Role> {
    id: NodeID,
    /// 当前生效的成员配置（日志中最新成员条目，追加后即生效）。
    membership: MembershipState,
    log: Log,
    state: Box<dyn State>,
    tx: Sender<Envelope>,
    opts: Options,
    role: R,
}

impl<R: Role> RawNode<R> {
    fn into_role<T: Role>(self, role: T) -> RawNode<T> {
        RawNode {
            id: self.id,
            membership: self.membership,
            log: self.log,
            state: self.state,
            tx: self.tx,
            opts: self.opts,
            role,
        }
    }

    fn term(&self) -> Term {
        self.log.get_term_vote().0
    }

    fn peers(&self) -> BTreeSet<NodeID> {
        self.membership.peers_of(self.id)
    }

    fn is_known_sender(&self, from: NodeID) -> bool {
        from == self.id || self.membership.all_voters().contains(&from)
    }

    /// 配置中的投票节点数（joint 时为并集大小，仅用于进度 map 等）。
    fn cluster_size(&self) -> usize {
        self.membership.all_voters().len()
    }

    fn random_election_timeout(&self) -> Ticks {
        rand::rng().random_range(self.opts.election_timeout_range.clone())
    }

    /// 选举超时上界，用于 check-quorum 窗口。
    fn election_timeout_max(&self) -> Ticks {
        self.opts.election_timeout_range.end.saturating_sub(1).max(1)
    }

    fn send(&self, to: NodeID, message: Message) -> Result<()> {
        Self::send_via(&self.tx, Envelope { from: self.id, to, term: self.term(), message })
    }

    fn send_via(tx: &Sender<Envelope>, msg: Envelope) -> Result<()> {
        debug!("Sending {msg:?}");
        Ok(tx.send(msg)?)
    }

    fn broadcast(&self, message: Message) -> Result<()> {
        for id in self.peers() {
            self.send(id, message.clone())?;
        }
        Ok(())
    }

    /// 日志追加后立即应用其中的成员配置。
    fn maybe_apply_membership_from_entries(&mut self, entries: &[Entry]) {
        for e in entries {
            if let Some(ref m) = e.membership {
                info!("Node {} adopting membership from log index {}: {m:?}", self.id, e.index);
                self.membership.apply_entry(m);
            }
        }
    }

    /// 从日志恢复最新成员配置（启动时）。
    fn restore_membership_from_log(&mut self) -> Result<()> {
        if let Some((_idx, m)) = self.log.latest_membership()? {
            self.membership.apply_entry(&m);
            // 若该条目已提交且为 Simple，清除 pending。
            let (commit, _) = self.log.get_commit_index();
            if let Some((idx, MembershipEntry::Simple(_))) = self.log.latest_membership()?
                && idx <= commit
            {
                self.membership.change_pending = false;
            }
        }
        Ok(())
    }
}

// =============================================================================
// Follower
// =============================================================================

pub struct Follower {
    leader: Option<NodeID>,
    leader_seen: Ticks,
    election_timeout: Ticks,
    forwarded: HashSet<RequestID>,
}

impl Follower {
    fn new(leader: Option<NodeID>, election_timeout: Ticks) -> Self {
        Self { leader, leader_seen: 0, election_timeout, forwarded: HashSet::new() }
    }
}

impl Role for Follower {}

impl RawNode<Follower> {
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
        let membership = MembershipState::bootstrap(id, peers.iter().copied());
        let role = Follower::new(None, 0);
        let mut node = Self { id, membership, log, state, tx, opts, role };
        node.role.election_timeout = node.random_election_timeout();
        node.restore_membership_from_log()?;
        node.maybe_apply()?;
        Ok(node)
    }

    /// `use_prevote`: true 时先 Pre-vote；false 时直接真选举（单节点或关闭 pre_vote）。
    fn into_candidate(mut self, use_prevote: bool) -> Result<RawNode<Candidate>> {
        self.abort_forwarded()?;
        self.maybe_apply()?;
        let election_timeout = self.random_election_timeout();
        let phase = if use_prevote && self.opts.pre_vote {
            ElectionPhase::PreVote
        } else {
            ElectionPhase::Election
        };
        let mut node = self.into_role(Candidate::new(election_timeout, phase));
        match node.role.phase {
            ElectionPhase::PreVote => node.pre_campaign()?,
            ElectionPhase::Election => node.campaign()?,
        }
        Ok(node)
    }

    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        assert_ne!(term, 0, "can't become follower in term 0");
        self.abort_forwarded()?;

        if let Some(leader) = leader {
            assert!(self.membership.all_voters().contains(&leader), "leader is not a voter");
            assert_eq!(self.role.leader, None, "already have leader in term");
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Following leader {leader} in term {term}");
            self.role = Follower::new(Some(leader), self.role.election_timeout);
        } else {
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term_vote(term, None)?;
            self.role = Follower::new(None, self.random_election_timeout());
        }
        Ok(self)
    }

    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Pre-vote 消息：不因更高 term 而切换任期。
        if matches!(msg.message, Message::PreCampaign { .. } | Message::PreCampaignResponse { .. }) {
            return self.step_prevote(msg);
        }

        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        if msg.term > self.term() {
            return self.into_follower(msg.term, None)?.step(msg);
        }

        if Some(msg.from) == self.role.leader {
            self.role.leader_seen = 0;
        }

        match msg.message {
            Message::Heartbeat { last_index, commit_index, read_seq } => {
                assert!(commit_index <= last_index, "commit_index after last_index");
                match self.role.leader {
                    Some(leader) if msg.from != leader => {
                        warn!(
                            "node {} ignoring Heartbeat from {} (current leader {})",
                            self.id, msg.from, leader
                        );
                        return Ok(self.into());
                    }
                    Some(_) => {}
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }
                let match_index = if self.log.has(last_index, msg.term)? { last_index } else { 0 };
                self.send(msg.from, Message::HeartbeatResponse { match_index, read_seq })?;
                if match_index != 0 && commit_index > self.log.get_commit_index().0 {
                    self.log.commit(commit_index)?;
                    self.maybe_apply()?;
                }
            }

            Message::Append { base_index, base_term, entries } => {
                if let Some(first) = entries.first() {
                    assert_eq!(base_index, first.index - 1, "base index mismatch");
                }
                match self.role.leader {
                    Some(leader) if msg.from != leader => {
                        warn!(
                            "node {} ignoring Append from {} (current leader {})",
                            self.id, msg.from, leader
                        );
                        return Ok(self.into());
                    }
                    Some(_) => {}
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }
                if base_index == 0 || self.log.has(base_index, base_term)? {
                    let match_index = entries.last().map(|e| e.index).unwrap_or(base_index);
                    self.log.splice(entries.clone())?;
                    // 配置在日志中出现后立即生效（论文联合共识）。
                    self.maybe_apply_membership_from_entries(&entries);
                    self.send(msg.from, Message::AppendResponse { match_index, reject_index: 0 })?;
                } else {
                    let reject_index = min(base_index, self.log.get_last_index().0 + 1);
                    self.send(msg.from, Message::AppendResponse { reject_index, match_index: 0 })?;
                }
            }

            Message::Read { seq } => {
                match self.role.leader {
                    Some(leader) if msg.from != leader => {
                        warn!(
                            "node {} ignoring Read from {} (current leader {})",
                            self.id, msg.from, leader
                        );
                        return Ok(self.into());
                    }
                    Some(_) => {}
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }
                self.send(msg.from, Message::ReadResponse { seq })?;
            }

            Message::Campaign { last_index, last_term } => {
                if let (_, Some(vote)) = self.log.get_term_vote()
                    && msg.from != vote
                {
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    return Ok(self.into());
                }
                let (log_index, log_term) = self.log.get_last_index();
                if log_term > last_term || log_term == last_term && log_index > last_index {
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    return Ok(self.into());
                }
                info!("Voting for {} in term {} election", msg.from, msg.term);
                self.log.set_term_vote(msg.term, Some(msg.from))?;
                self.send(msg.from, Message::CampaignResponse { vote: true })?;
            }

            Message::ClientRequest { id, request: _ } => {
                // 仅接受本节点注入的客户端请求；其它 from 直接 Abort，避免 panic。
                if msg.from != self.id {
                    warn!(
                        "node {} rejecting ClientRequest from foreign sender {}",
                        self.id, msg.from
                    );
                    self.send(
                        msg.from,
                        Message::ClientResponse { id, response: Err(Error::Abort) },
                    )?;
                    return Ok(self.into());
                }
                if let Some(leader) = self.role.leader {
                    debug!("Forwarding request to leader {leader}: {msg:?}");
                    self.role.forwarded.insert(id);
                    self.send(leader, msg.message)?;
                } else {
                    self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
                }
            }

            Message::ClientResponse { id, response } => {
                if Some(msg.from) != self.role.leader {
                    warn!(
                        "node {} ignoring ClientResponse from non-leader {}",
                        self.id, msg.from
                    );
                    return Ok(self.into());
                }
                if self.role.forwarded.remove(&id) {
                    self.send(self.id, Message::ClientResponse { id, response })?;
                }
            }

            Message::CampaignResponse { .. } => {}

            Message::PreCampaign { .. } | Message::PreCampaignResponse { .. } => {
                unreachable!("handled above")
            }

            Message::InstallSnapshot {
                last_included_index,
                last_included_term,
                data,
                membership,
            } => {
                match self.role.leader {
                    Some(leader) if msg.from != leader => {
                        warn!(
                            "node {} ignoring InstallSnapshot from {} (current leader {})",
                            self.id, msg.from, leader
                        );
                        return Ok(self.into());
                    }
                    Some(_) => {}
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                }
                // 忽略过期 / 重复快照，避免把状态机回滚到更旧点。
                let (snap_idx, _) = self.log.get_snapshot_meta();
                let applied = self.state.get_applied_index();
                if last_included_index >= snap_idx && last_included_index >= applied {
                    self.state.restore(&data, last_included_index)?;
                    self.log.engine.set(&super::log::Key::SnapshotData.encode(), data.clone())?;
                    self.log.reset_with_snapshot(last_included_index, last_included_term)?;
                    self.membership.apply_entry(&membership);
                    // 快照中的配置视为已提交生效。
                    if matches!(membership, MembershipEntry::Simple(_)) {
                        self.membership.change_pending = false;
                    }
                }
                self.send(
                    msg.from,
                    Message::InstallSnapshotResponse { last_included_index },
                )?;
            }

            Message::InstallSnapshotResponse { .. } => {}

            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ReadResponse { .. } => {
                panic!("follower received unexpected message {msg:?}")
            }
        };
        Ok(self.into())
    }

    fn step_prevote(self, msg: Envelope) -> Result<Node> {
        match msg.message {
            Message::PreCampaign { last_index, last_term } => {
                // 仍能联系当前领导则拒绝预选票。
                if self.role.leader.is_some() && self.role.leader_seen < self.role.election_timeout {
                    self.send(msg.from, Message::PreCampaignResponse { vote: false })?;
                    return Ok(self.into());
                }
                let (log_index, log_term) = self.log.get_last_index();
                let log_ok =
                    !(log_term > last_term || log_term == last_term && log_index > last_index);
                // 预选票请求的 envelope.term 为 intended term（current+1），不更新本地 term。
                let term_ok = msg.term > self.term();
                let vote = log_ok && term_ok;
                debug!(
                    "Pre-vote for {} (term {}): vote={vote} log_ok={log_ok} term_ok={term_ok}",
                    msg.from, msg.term
                );
                self.send(msg.from, Message::PreCampaignResponse { vote })?;
            }
            Message::PreCampaignResponse { .. } => {
                // 跟随者不收集预选票。
            }
            _ => {}
        }
        Ok(self.into())
    }

    fn tick(mut self) -> Result<Node> {
        self.role.leader_seen += 1;
        if self.role.leader_seen >= self.role.election_timeout {
            return Ok(self.into_candidate(true)?.into());
        }
        Ok(self.into())
    }

    fn abort_forwarded(&mut self) -> Result<()> {
        for id in std::mem::take(&mut self.role.forwarded).into_iter().sorted() {
            debug!("Aborting forwarded request {id}");
            self.send(self.id, Message::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    fn maybe_apply(&mut self) -> Result<()> {
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            if let Some(ref m) = entry.membership {
                self.membership.on_commit(m);
            }
            // 成员变更条目：状态机收到 noop 式 command=None。
            _ = self.state.apply(entry);
        }
        Ok(())
    }
}

// =============================================================================
// Candidate (+ Pre-vote phase)
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ElectionPhase {
    PreVote,
    Election,
}

pub struct Candidate {
    votes: HashSet<NodeID>,
    election_duration: Ticks,
    election_timeout: Ticks,
    phase: ElectionPhase,
}

impl Candidate {
    fn new(election_timeout: Ticks, phase: ElectionPhase) -> Self {
        Self { votes: HashSet::new(), election_duration: 0, election_timeout, phase }
    }
}

impl Role for Candidate {}

impl RawNode<Candidate> {
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        let election_timeout = self.random_election_timeout();
        if let Some(leader) = leader {
            assert_eq!(term, self.term(), "can't follow leader in different term");
            info!("Lost election, following leader {leader} in term {term}");
            Ok(self.into_role(Follower::new(Some(leader), election_timeout)))
        } else {
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            info!("Discovered new term {term}");
            self.log.set_term_vote(term, None)?;
            Ok(self.into_role(Follower::new(None, election_timeout)))
        }
    }

    fn into_leader(self) -> Result<RawNode<Leader>> {
        let (term, vote) = self.log.get_term_vote();
        assert_ne!(term, 0, "leaders can't have term 0");
        assert_eq!(vote, Some(self.id), "leader did not vote for self");
        assert_eq!(self.role.phase, ElectionPhase::Election, "must win real election");

        info!("Won election for term {term}, becoming leader");
        let peers = self.peers();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));
        node.propose(None)?;
        let _ = node.maybe_commit_and_apply()?;
        node.heartbeat()?;
        Ok(node)
    }

    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Pre-vote：不因更高 term 切换；可向日志足够新的节点授予预选票。
        if let Message::PreCampaign { last_index, last_term } = msg.message {
            let (log_index, log_term) = self.log.get_last_index();
            let log_ok =
                !(log_term > last_term || log_term == last_term && log_index > last_index);
            // intended term 应大于本地 term；同 term 的预选也允许（大家都在 term T 抢 T+1）。
            let term_ok = msg.term > self.term();
            let vote = log_ok && term_ok;
            self.send(msg.from, Message::PreCampaignResponse { vote })?;
            return Ok(self.into());
        }
        if let Message::PreCampaignResponse { vote } = msg.message {
            if self.role.phase == ElectionPhase::PreVote && vote {
                if self.membership.all_voters().contains(&msg.from) {
                    self.role.votes.insert(msg.from);
                }
                if self.quorum_reached(&self.role.votes) {
                    info!("Pre-vote won, starting real election");
                    self.campaign()?;
                    // 单节点或已够多数时立即当选。
                    if self.quorum_reached(&self.role.votes)
                        && self.role.phase == ElectionPhase::Election
                    {
                        return Ok(self.into_leader()?.into());
                    }
                }
            }
            return Ok(self.into());
        }

        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        if msg.term > self.term() {
            return self.into_follower(msg.term, None)?.step(msg);
        }

        match msg.message {
            Message::CampaignResponse { vote: true } => {
                if self.role.phase == ElectionPhase::Election {
                    if self.membership.all_voters().contains(&msg.from) {
                        self.role.votes.insert(msg.from);
                    }
                    if self.quorum_reached(&self.role.votes) {
                        return Ok(self.into_leader()?.into());
                    }
                }
            }
            Message::CampaignResponse { vote: false } => {}
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?;
            }
            Message::Heartbeat { .. }
            | Message::Append { .. }
            | Message::Read { .. }
            | Message::InstallSnapshot { .. } => {
                return self.into_follower(msg.term, Some(msg.from))?.step(msg);
            }
            Message::ClientRequest { id, request: _ } => {
                self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
            }
            Message::InstallSnapshotResponse { .. } => {}
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ReadResponse { .. }
            | Message::ClientResponse { .. }
            | Message::PreCampaign { .. }
            | Message::PreCampaignResponse { .. } => {
                panic!("unexpected message {msg:?}")
            }
        }
        Ok(self.into())
    }

    fn tick(mut self) -> Result<Node> {
        self.role.election_duration += 1;
        if self.role.election_duration >= self.role.election_timeout {
            // 超时后重新从 Pre-vote 开始（若启用），避免 term 风暴。
            if self.opts.pre_vote {
                self.pre_campaign()?;
            } else {
                self.campaign()?;
            }
        }
        Ok(self.into())
    }

    /// 当前票集是否达到（联合）法定人数。候选人总是含自己。
    fn quorum_reached(&self, votes: &HashSet<NodeID>) -> bool {
        let matched: BTreeSet<NodeID> = votes.iter().copied().collect();
        self.membership.has_quorum(&matched)
    }

    fn pre_campaign(&mut self) -> Result<()> {
        let timeout = self.random_election_timeout();
        self.role = Candidate::new(timeout, ElectionPhase::PreVote);
        self.role.votes.insert(self.id);
        // 单节点：直接进入真选举。
        if self.cluster_size() == 1 {
            return self.campaign();
        }
        let (last_index, last_term) = self.log.get_last_index();
        // Pre-vote 使用 intended term = current+1，接收方不持久化。
        let intended = self.term() + 1;
        info!("Starting pre-vote for intended term {intended}");
        for id in self.peers() {
            Self::send_via(
                &self.tx,
                Envelope {
                    from: self.id,
                    to: id,
                    term: intended,
                    message: Message::PreCampaign { last_index, last_term },
                },
            )?;
        }
        // 已有自己一票；若自己已构成多数（不应发生在多节点）则直接竞选。
        if self.quorum_reached(&self.role.votes) {
            self.campaign()?;
        }
        Ok(())
    }

    fn campaign(&mut self) -> Result<()> {
        let term = self.term() + 1;
        info!("Starting new election for term {term}");
        let timeout = self.random_election_timeout();
        self.role = Candidate::new(timeout, ElectionPhase::Election);
        self.role.votes.insert(self.id);
        self.log.set_term_vote(term, Some(self.id))?;
        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })?;
        if self.quorum_reached(&self.role.votes) {
            // 单节点等情况：立即当选由调用方 into_leader；此处仅标记。
            // 实际 into_leader 在 step/tick 路径；对单节点 Node::new 会链式调用。
        }
        Ok(())
    }
}

// =============================================================================
// Leader
// =============================================================================

pub struct Leader {
    progress: HashMap<NodeID, Progress>,
    writes: HashMap<Index, Write>,
    /// 在途成员变更客户端请求（joint 条目索引）。
    membership_writes: HashMap<Index, Write>,
    reads: VecDeque<Read>,
    read_seq: ReadSequence,
    since_heartbeat: Ticks,
    /// 自上次收到该 peer 有效响应以来的 tick；自身不计入。
    peer_seen: HashMap<NodeID, Ticks>,
}

struct Progress {
    match_index: Index,
    next_index: Index,
    read_seq: ReadSequence,
}

impl Progress {
    fn advance(&mut self, match_index: Index) -> bool {
        if match_index <= self.match_index {
            return false;
        }
        self.match_index = match_index;
        self.next_index = max(self.next_index, match_index + 1);
        true
    }

    fn advance_read(&mut self, read_seq: ReadSequence) -> bool {
        if read_seq <= self.read_seq {
            return false;
        }
        self.read_seq = read_seq;
        true
    }

    fn regress_next(&mut self, next_index: Index) -> bool {
        if next_index >= self.next_index || self.next_index <= self.match_index + 1 {
            return false;
        }
        self.next_index = max(next_index, self.match_index + 1);
        true
    }
}

struct Write {
    from: NodeID,
    id: RequestID,
}

struct Read {
    seq: ReadSequence,
    from: NodeID,
    id: RequestID,
    command: Vec<u8>,
}

impl Leader {
    fn new(peers: BTreeSet<NodeID>, last_index: Index) -> Self {
        let next_index = last_index + 1;
        let progress = peers
            .iter()
            .copied()
            .map(|p| (p, Progress { next_index, match_index: 0, read_seq: 0 }))
            .collect();
        let peer_seen = peers.iter().copied().map(|p| (p, 0)).collect();
        Self {
            progress,
            writes: HashMap::new(),
            membership_writes: HashMap::new(),
            reads: VecDeque::new(),
            read_seq: 0,
            since_heartbeat: 0,
            peer_seen,
        }
    }
}

impl Role for Leader {}

impl RawNode<Leader> {
    /// 因更高任期或 check-quorum 下台。
    fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        // check-quorum 可能同任期下台。
        if term > self.term() {
            info!("Discovered new term {term}");
            self.log.set_term_vote(term, None)?;
        } else {
            info!("Leader stepping down in term {}", self.term());
        }

        for write in std::mem::take(&mut self.role.writes).into_values().sorted_by_key(|w| w.id) {
            self.send(write.from, Message::ClientResponse { id: write.id, response: Err(Error::Abort) })?;
        }
        for write in
            std::mem::take(&mut self.role.membership_writes).into_values().sorted_by_key(|w| w.id)
        {
            self.send(write.from, Message::ClientResponse { id: write.id, response: Err(Error::Abort) })?;
        }
        for read in std::mem::take(&mut self.role.reads).into_iter().sorted_by_key(|r| r.id) {
            self.send(read.from, Message::ClientResponse { id: read.id, response: Err(Error::Abort) })?;
        }

        let election_timeout = self.random_election_timeout();
        Ok(self.into_role(Follower::new(None, election_timeout)))
    }

    fn note_peer_response(&mut self, from: NodeID) {
        if let Some(seen) = self.role.peer_seen.get_mut(&from) {
            *seen = 0;
        }
    }

    fn sync_progress_with_membership(&mut self) {
        let peers = self.peers();
        let (last_index, _) = self.log.get_last_index();
        let next_index = last_index + 1;
        // 添加新同伴。
        for p in &peers {
            self.role.progress.entry(*p).or_insert(Progress {
                next_index,
                match_index: 0,
                read_seq: 0,
            });
            self.role.peer_seen.entry(*p).or_insert(0);
        }
        // 移除旧同伴。
        self.role.progress.retain(|id, _| peers.contains(id));
        self.role.peer_seen.retain(|id, _| peers.contains(id));
    }

    fn step(mut self, msg: Envelope) -> Result<Node> {
        let mut step_down = false;
        if matches!(msg.message, Message::PreCampaign { .. }) {
            // 领导者拒绝预选票（自己仍活着）。
            self.send(msg.from, Message::PreCampaignResponse { vote: false })?;
            return Ok(self.into());
        }
        if matches!(msg.message, Message::PreCampaignResponse { .. }) {
            return Ok(self.into());
        }

        if msg.term < self.term() {
            debug!("Dropping message from past term: {msg:?}");
            return Ok(self.into());
        }
        if msg.term > self.term() {
            return self.into_follower(msg.term)?.step(msg);
        }

        match msg.message {
            Message::HeartbeatResponse { match_index, read_seq } => {
                if !self.role.progress.contains_key(&msg.from) {
                    return Ok(self.into());
                }
                self.note_peer_response(msg.from);
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");
                assert!(read_seq <= self.role.read_seq, "future read sequence number");

                if self.progress(msg.from).advance_read(read_seq) {
                    self.maybe_read()?;
                }
                if match_index == 0 {
                    self.progress(msg.from).regress_next(last_index);
                    self.maybe_send_append(msg.from, true)?;
                }
                if self.progress(msg.from).advance(match_index) {
                    step_down |= self.maybe_commit_and_apply()?.1;
                }
            }

            Message::AppendResponse { match_index, reject_index: 0 } if match_index > 0 => {
                if !self.role.progress.contains_key(&msg.from) {
                    return Ok(self.into());
                }
                self.note_peer_response(msg.from);
                let (last_index, _) = self.log.get_last_index();
                assert!(match_index <= last_index, "future match index");
                if self.progress(msg.from).advance(match_index) {
                    step_down |= self.maybe_commit_and_apply()?.1;
                }
                self.maybe_send_append(msg.from, false)?;
            }

            Message::ReadResponse { seq } => {
                if !self.role.progress.contains_key(&msg.from) {
                    return Ok(self.into());
                }
                self.note_peer_response(msg.from);
                if self.progress(msg.from).advance_read(seq) {
                    self.maybe_read()?;
                }
            }

            Message::AppendResponse { reject_index, match_index: 0 } if reject_index > 0 => {
                if !self.role.progress.contains_key(&msg.from) {
                    return Ok(self.into());
                }
                self.note_peer_response(msg.from);
                let (last_index, _) = self.log.get_last_index();
                assert!(reject_index <= last_index, "future reject index");
                if reject_index <= self.progress(msg.from).match_index {
                    return Ok(self.into());
                }
                if self.progress(msg.from).regress_next(reject_index) {
                    self.maybe_send_append(msg.from, true)?;
                }
            }

            Message::AppendResponse { .. } => panic!("invalid message {msg:?}"),

            Message::ClientRequest { id, request: Request::Write(command) } => {
                let index = self.propose(Some(command))?;
                self.role.writes.insert(index, Write { from: msg.from, id });
                if self.cluster_size() == 1 {
                    step_down |= self.maybe_commit_and_apply()?.1;
                }
            }

            Message::ClientRequest {
                id,
                request: Request::WriteSession { client_id, seq, command },
            } => {
                let wrapped = super::session::encode_session(client_id, seq, command);
                let index = self.propose(Some(wrapped))?;
                self.role.writes.insert(index, Write { from: msg.from, id });
                if self.cluster_size() == 1 {
                    step_down |= self.maybe_commit_and_apply()?.1;
                }
            }

            Message::ClientRequest { id, request: Request::Read(command) } => {
                self.role.read_seq += 1;
                let read = Read { seq: self.role.read_seq, from: msg.from, id, command };
                self.role.reads.push_back(read);
                self.broadcast(Message::Read { seq: self.role.read_seq })?;
                if self.cluster_size() == 1 {
                    self.maybe_read()?;
                }
            }

            Message::ClientRequest { id, request: Request::Status } => {
                let response = self.status().map(Response::Status);
                self.send(msg.from, Message::ClientResponse { id, response })?;
            }

            Message::ClientRequest {
                id,
                request: Request::ChangeMembership { voters },
            } => {
                let result = self.propose_membership_change(voters);
                match result {
                    Ok(index) => {
                        self.role.membership_writes.insert(index, Write { from: msg.from, id });
                        if self.cluster_size() == 1 {
                            step_down |= self.maybe_commit_and_apply()?.1;
                        }
                    }
                    Err(e) => {
                        self.send(
                            msg.from,
                            Message::ClientResponse { id, response: Err(e) },
                        )?;
                    }
                }
            }

            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?;
            }
            Message::CampaignResponse { .. } => {}
            Message::Heartbeat { .. } | Message::Append { .. } | Message::Read { .. } => {
                // 同任期不应出现另一领导；丢弃以免陈旧/异常消息拖垮进程。
                warn!(
                    "leader {} ignoring peer-leader message from {} in term {}",
                    self.id, msg.from, msg.term
                );
            }
            Message::InstallSnapshotResponse { last_included_index } => {
                if !self.role.progress.contains_key(&msg.from) {
                    return Ok(self.into());
                }
                self.note_peer_response(msg.from);
                if self.progress(msg.from).advance(last_included_index) {
                    step_down |= self.maybe_commit_and_apply()?.1;
                }
                self.maybe_send_append(msg.from, false)?;
            }
            Message::InstallSnapshot { .. } => {
                warn!("leader {} ignoring InstallSnapshot from {}", self.id, msg.from);
            }
            Message::ClientResponse { .. }
            | Message::PreCampaign { .. }
            | Message::PreCampaignResponse { .. } => panic!("unexpected message {msg:?}"),
        }

        if step_down {
            let term = self.term();
            return Ok(self.into_follower(term)?.into());
        }
        Ok(self.into())
    }

    fn tick(mut self) -> Result<Node> {
        // 推进 peer_seen。
        for seen in self.role.peer_seen.values_mut() {
            *seen = seen.saturating_add(1);
        }

        self.role.since_heartbeat += 1;
        if self.role.since_heartbeat >= self.opts.heartbeat_interval {
            self.heartbeat()?;
        }

        // CheckQuorum：窗口内活跃节点（含自己）是否构成多数。
        if self.opts.check_quorum && self.cluster_size() > 1 {
            let window = self.election_timeout_max();
            let mut matched: BTreeSet<NodeID> = BTreeSet::new();
            matched.insert(self.id);
            for (peer, seen) in &self.role.peer_seen {
                if *seen < window {
                    matched.insert(*peer);
                }
            }
            if !self.membership.has_quorum(&matched) {
                warn!(
                    "CheckQuorum: leader {} lost majority contact, stepping down",
                    self.id
                );
                let term = self.term();
                return Ok(self.into_follower(term)?.into());
            }
        }

        Ok(self.into())
    }

    fn heartbeat(&mut self) -> Result<()> {
        let (last_index, last_term) = self.log.get_last_index();
        let (commit_index, _) = self.log.get_commit_index();
        let read_seq = self.role.read_seq;
        assert_eq!(last_term, self.term(), "leader's last_term not in current term");
        self.role.since_heartbeat = 0;
        self.broadcast(Message::Heartbeat { last_index, commit_index, read_seq })
    }

    fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(command)?;
        for peer in self.peers() {
            if index == self.progress(peer).next_index {
                self.maybe_send_append(peer, false)?;
            }
        }
        Ok(index)
    }

    fn propose_membership_change(&mut self, voters: HashSet<NodeID>) -> Result<Index> {
        if self.membership.change_pending {
            return Err(Error::InvalidInput("membership change already in progress".into()));
        }
        if voters.is_empty() {
            return Err(Error::InvalidInput("voters must not be empty".into()));
        }
        // 允许移除当前领导：Simple 配置提交后领导 step down（领导转移）。
        let old = match &self.membership.active {
            MembershipEntry::Simple(m) => m.clone(),
            MembershipEntry::Joint { .. } => {
                return Err(Error::InvalidInput("already in joint consensus".into()));
            }
        };
        let new = Membership::from_iter(voters);
        if old == new {
            return Err(Error::InvalidInput("membership unchanged".into()));
        }

        let joint = MembershipEntry::Joint { old, new: new.clone() };
        info!("Proposing joint membership: {joint:?}");
        let index = self.log.append_membership(joint.clone())?;
        self.membership.apply_entry(&joint);
        self.sync_progress_with_membership();

        for peer in self.peers() {
            if index == self.progress(peer).next_index {
                self.maybe_send_append(peer, false)?;
            }
        }
        Ok(index)
    }

    /// 提交 joint 后自动提出 Simple(C_new)。
    fn maybe_propose_simple_after_joint(&mut self, committed: &MembershipEntry) -> Result<()> {
        if let MembershipEntry::Joint { new, .. } = committed {
            let simple = MembershipEntry::Simple(new.clone());
            info!("Joint committed, proposing simple membership: {simple:?}");
            let index = self.log.append_membership(simple.clone())?;
            self.membership.apply_entry(&simple);
            self.sync_progress_with_membership();
            for peer in self.peers() {
                if index == self.progress(peer).next_index {
                    self.maybe_send_append(peer, false)?;
                }
            }
        }
        Ok(())
    }

    fn maybe_commit_and_apply(&mut self) -> Result<(Index, bool)> {
        let (last_index, _) = self.log.get_last_index();

        // 基于当前（可能 joint）配置计算可提交索引：
        // 从 last_index 向下找第一个被多数复制的本任期索引。
        let mut commit_index = self.log.get_commit_index().0;
        for idx in (commit_index + 1..=last_index).rev() {
            let Some(entry) = self.log.get(idx)? else { continue };
            if entry.term != self.term() {
                continue;
            }
            let mut matched: BTreeSet<NodeID> = BTreeSet::new();
            matched.insert(self.id); // 领导者已拥有
            for (peer, p) in &self.role.progress {
                if p.match_index >= idx {
                    matched.insert(*peer);
                }
            }
            if self.membership.has_quorum(&matched) {
                commit_index = idx;
                break;
            }
        }

        let (old_index, old_term) = self.log.get_commit_index();
        if commit_index <= old_index {
            return Ok((old_index, false));
        }

        match self.log.get(commit_index)? {
            Some(entry) if entry.term == self.term() => {}
            Some(_) => return Ok((old_index, false)),
            None => panic!("commit index {commit_index} missing"),
        }

        self.log.commit(commit_index)?;

        let term = self.term();
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        let mut committed_membership = Vec::new();
        while let Some(entry) = iter.next().transpose()? {
            debug!("Applying {entry:?}");
            if let Some(ref m) = entry.membership {
                self.membership.on_commit(m);
                committed_membership.push(m.clone());
            }

            let write = self.role.writes.remove(&entry.index);
            let mwrite = self.role.membership_writes.remove(&entry.index);
            let entry_index = entry.index;

            if entry.membership.is_some() {
                // 成员变更：状态机按 noop 应用，推进 applied_index。
                let _ = self.state.apply(Entry {
                    index: entry.index,
                    term: entry.term,
                    command: None,
                    membership: None,
                });
                if let Some(Write { id, from: to }) = mwrite {
                    Self::send_via(
                        &self.tx,
                        Envelope {
                            from: self.id,
                            term,
                            to,
                            message: Message::ClientResponse {
                                id,
                                response: Ok(Response::ChangeMembership { index: entry_index }),
                            },
                        },
                    )?;
                }
            } else {
                let result = self.state.apply(entry);
                if let Some(Write { id, from: to }) = write {
                    Self::send_via(
                        &self.tx,
                        Envelope {
                            from: self.id,
                            term,
                            to,
                            message: Message::ClientResponse {
                                id,
                                response: result.map(Response::Write),
                            },
                        },
                    )?;
                }
            }
        }
        drop(iter);

        let mut step_down = false;
        for m in committed_membership {
            self.maybe_propose_simple_after_joint(&m)?;
            self.sync_progress_with_membership();
            // Simple 配置已生效且自身不在投票集合 → 领导转移完成，下台。
            if matches!(self.membership.active, MembershipEntry::Simple(_))
                && !self.membership.all_voters().contains(&self.id)
            {
                info!(
                    "Leader {} removed from membership, will step down",
                    self.id
                );
                step_down = true;
            }
        }

        if old_term != self.term() {
            self.maybe_read()?;
        }

        if !step_down {
            self.maybe_snapshot()?;
        }

        Ok((commit_index, step_down))
    }

    fn maybe_snapshot(&mut self) -> Result<()> {
        let threshold = self.opts.snapshot_threshold;
        if threshold == 0 {
            return Ok(());
        }
        let applied = self.state.get_applied_index();
        let (snap_idx, _) = self.log.get_snapshot_meta();
        if applied <= snap_idx || applied - snap_idx < threshold {
            return Ok(());
        }
        let (commit, commit_term) = self.log.get_commit_index();
        if applied < commit {
            return Ok(());
        }
        let term = match self.log.get(applied)? {
            Some(e) => e.term,
            None if applied == snap_idx => return Ok(()),
            None => commit_term,
        };
        let data = self.state.snapshot()?;
        // 将快照字节存入引擎，便于重启恢复状态机。
        self.log.engine.set(&super::log::Key::SnapshotData.encode(), data)?;
        info!("Compacting log through index {applied} term {term}");
        self.log.compact_to(applied, term)?;
        Ok(())
    }

    fn maybe_read(&mut self) -> Result<()> {
        if self.role.reads.is_empty() {
            return Ok(());
        }
        let (commit_index, commit_term) = self.log.get_commit_index();
        let applied_index = self.state.get_applied_index();
        if commit_term < self.term() || applied_index < commit_index {
            return Ok(());
        }

        // 读确认：read_seq 被多数确认。
        // 构造每个 voter 的 read_seq（自己为 role.read_seq）。
        let mut matched_seq: Vec<(NodeID, ReadSequence)> = Vec::new();
        matched_seq.push((self.id, self.role.read_seq));
        for (peer, p) in &self.role.progress {
            matched_seq.push((*peer, p.read_seq));
        }

        // 找最大的 seq，使得拥有 >= seq 的节点构成多数。
        let mut quorum_read_seq = 0;
        let candidates: BTreeSet<ReadSequence> = matched_seq.iter().map(|(_, s)| *s).collect();
        for seq in candidates.into_iter().rev() {
            let matched: BTreeSet<NodeID> =
                matched_seq.iter().filter(|(_, s)| *s >= seq).map(|(id, _)| *id).collect();
            if self.membership.has_quorum(&matched) {
                quorum_read_seq = seq;
                break;
            }
        }

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

    fn maybe_send_append(&mut self, peer: NodeID, mut probe: bool) -> Result<()> {
        if !self.role.progress.contains_key(&peer) {
            return Ok(());
        }
        let (last_index, _) = self.log.get_last_index();
        let progress = self.role.progress.get_mut(&peer).unwrap();
        assert_ne!(progress.next_index, 0, "invalid next_index");
        assert!(progress.next_index > progress.match_index, "invalid next_index <= match_index");
        assert!(progress.match_index <= last_index, "invalid match_index > last_index");
        assert!(progress.next_index <= last_index + 1, "invalid next_index > last_index + 1");

        let first_index = self.log.get_first_index();
        if progress.next_index < first_index {
            let (last_included_index, last_included_term) = self.log.get_snapshot_meta();
            let data = self.state.snapshot()?;
            let membership = self.membership.active.clone();
            return self.send(
                peer,
                Message::InstallSnapshot {
                    last_included_index,
                    last_included_term,
                    data,
                    membership,
                },
            );
        }

        if progress.match_index == last_index {
            return Ok(());
        }
        probe = probe && progress.next_index > progress.match_index + 1;
        if progress.next_index > last_index && !probe {
            return Ok(());
        }

        let (base_index, base_term) = match progress.next_index {
            0 => panic!("next_index=0 for node {peer}"),
            1 => (0, 0),
            next => {
                self.log.get(next - 1)?.map(|e| (e.index, e.term)).expect("missing base entry")
            }
        };
        let entries = match probe {
            false => self
                .log
                .scan(progress.next_index..)
                .take(self.opts.max_append_entries)
                .try_collect()?,
            true => Vec::new(),
        };

        if let Some(last) = entries.last() {
            progress.next_index = last.index + 1;
        }

        debug!("Replicating {} entries with base {base_index} to {peer}", entries.len());
        self.send(peer, Message::Append { base_index, base_term, entries })
    }

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
            voters: self.membership.all_voters(),
        })
    }

    fn progress(&mut self, id: NodeID) -> &mut Progress {
        self.role.progress.get_mut(&id).expect("unknown node")
    }
}
