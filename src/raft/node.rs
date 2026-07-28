// 比较工具：截断/推进复制进度与拒绝索引时取上下界
use std::cmp::{max, min};
// 集合类型：票集、进度表、转发中请求、有序读队列等
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
// 选举超时随机区间类型
use std::ops::Range;

// 出站消息通道：节点经此向网络层投递 Envelope
use crossbeam::channel::Sender;
// 排序辅助：中止转发请求时按 id 有序回复，保证可复现
use itertools::Itertools as _;
// 协议路径日志：选举、复制、丢弃未知发送方等
use log::{debug, info, warn};
// 随机选举超时，降低同时超时引发的选票瓜分
use rand::RngExt as _;

// Raft 日志条目、索引与持久化日志抽象
use super::log::{Entry, Index, Log};
// 成员配置：Simple/Joint 与生效状态机
use super::membership::{Membership, MembershipEntry, MembershipState};
// RPC 信封与消息体：选举、复制、读写、快照、成员变更
use super::message::{Envelope, Message, ReadSequence, Request, RequestID, Response, Status};
// 状态机接口：apply/read/snapshot/restore
use super::state::State;
// 默认心跳间隔、选举超时范围与单次 Append 上限
use super::{ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL, MAX_APPEND_ENTRIES};
// 参数校验错误宏
use crate::errinput;
// 统一错误与 Result 类型
use crate::error::{Error, Result};

/// 节点 ID，在集群内唯一。启动时手动分配。
pub type NodeID = u8;

/// 领导者任期号。选举时单调递增。
pub type Term = u64;

/// 逻辑时钟间隔，以 tick 数量表示。
pub type Ticks = u8;

/// Raft 节点选项。
#[derive(Clone, Debug, PartialEq)]
// Raft 运行时可调参数集合
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
// 结束代码块
}

// 默认配置实现
impl Default for Options {
    // 构造与生产默认一致的 Options
    fn default() -> Self {
        // 填充各字段默认值
        Self {
            // 默认心跳间隔
            heartbeat_interval: HEARTBEAT_INTERVAL,
            // 默认选举超时随机区间
            election_timeout_range: ELECTION_TIMEOUT_RANGE,
            // 默认单次复制条目上限
            max_append_entries: MAX_APPEND_ENTRIES,
            // 默认开启 Pre-vote
            pre_vote: true,
            // 默认开启 CheckQuorum
            check_quorum: true,
            snapshot_threshold: 0, // 默认关闭，由节点配置开启
        // 结束代码块
        }
    // 结束代码块
    }
// 结束代码块
}

/// 具有动态角色的 Raft 节点。
pub enum Node {
    /// 候选人（含 Pre-vote 相位）。
    Candidate(RawNode<Candidate>),
    /// 跟随者。
    Follower(RawNode<Follower>),
    /// 领导者。
    Leader(RawNode<Leader>),
// 结束类型定义
}

// Node 对外统一入口：创建、step、tick、查询
impl Node {
    /// 创建新的 Raft 节点。`peers` 为除自身外的初始投票同伴。
    pub fn new(
        // 本节点 ID
        id: NodeID,
        // 初始同伴集合（不含自身）
        peers: HashSet<NodeID>,
        // 持久化日志
        log: Log,
        // 状态机实现
        state: Box<dyn State>,
        // 出站消息发送端
        tx: Sender<Envelope>,
        // 运行时选项
        opts: Options,
    // 可能因参数非法失败
    ) -> Result<Self> {
        // 以跟随者形态构造底层 RawNode
        let node = RawNode::new(id, peers, log, state, tx, opts)?;
        // 单节点集群无需等待他人选票
        if node.cluster_size() == 1 {
            // 单节点：跳过 pre-vote，直接竞选并当选。
            return Ok(node.into_candidate(false)?.into_leader()?.into());
        // 结束结构/枚举构造
        }
        // 多节点默认以跟随者启动
        Ok(node.into())
    // 结束结构/枚举构造
    }

    // 查询当前节点 ID（与角色无关）
    pub fn id(&self) -> NodeID {
        // 按角色分支取出 id
        match self {
            // 候选人 id
            Self::Candidate(node) => node.id,
            // 跟随者 id
            Self::Follower(node) => node.id,
            // 领导者 id
            Self::Leader(node) => node.id,
        // 结束 match 分支/块
        }
    // 结束 match 分支/块
    }

    // 查询当前任期
    pub fn term(&self) -> Term {
        // 按角色分支取出 term
        match self {
            // 候选人任期
            Self::Candidate(node) => node.term(),
            // 跟随者任期
            Self::Follower(node) => node.term(),
            // 领导者任期
            Self::Leader(node) => node.term(),
        // 结束 match 分支/块
        }
    // 结束 match 分支/块
    }

    /// 当前生效的投票成员（含自身）。
    pub fn voters(&self) -> BTreeSet<NodeID> {
        // 按角色读取成员配置
        match self {
            // 候选人侧投票成员
            Self::Candidate(node) => node.membership.all_voters(),
            // 跟随者侧投票成员
            Self::Follower(node) => node.membership.all_voters(),
            // 领导者侧投票成员
            Self::Leader(node) => node.membership.all_voters(),
        // 结束 match 分支/块
        }
    // 结束 match 分支/块
    }

    // 处理一条入站消息并可能发生角色转换
    pub fn step(self, msg: Envelope) -> Result<Self> {
        // 消息目标必须是本节点，防止串包
        assert_eq!(msg.to, self.id(), "message to other node: {msg:?}");

        // 允许来自当前（可能 joint）配置中的成员或自身；未知发送方丢弃。
        let known = match &self {
            // 候选人已知发送方？
            Self::Candidate(node) => node.is_known_sender(msg.from),
            // 跟随者已知发送方？
            Self::Follower(node) => node.is_known_sender(msg.from),
            // 领导者已知发送方？
            Self::Leader(node) => node.is_known_sender(msg.from),
        // 结束 match 分支/块
        };
        // 未知发送方：丢弃，防配置外干扰
        if !known {
            // 记录丢弃原因
            warn!("Dropping message from unknown sender: {msg:?}");
            // 保持当前角色不变
            return Ok(self);
        // 结束结构/枚举构造
        }
        // 进入角色专属 step
        debug!("Stepping {msg:?}");

        // 按当前角色分派消息处理
        match self {
            // 候选人处理消息（计票/落选等）
            Self::Candidate(node) => node.step(msg),
            // 跟随者处理消息（复制/投票/转发）
            Self::Follower(node) => node.step(msg),
            // 领导者处理消息（复制确认/客户端/成员变更）
            Self::Leader(node) => node.step(msg),
        // 结束 match 分支/块
        }
    // 结束 match 分支/块
    }

    // 逻辑时钟推进一拍，驱动超时与心跳
    pub fn tick(self) -> Result<Self> {
        // 按角色分派 tick
        match self {
            // 候选人选举计时
            Self::Candidate(node) => node.tick(),
            // 跟随者选举超时检测
            Self::Follower(node) => node.tick(),
            // 领导者心跳与 CheckQuorum
            Self::Leader(node) => node.tick(),
        // 结束 match 分支/块
        }
    // 结束 match 分支/块
    }
// 结束代码块
}

// Candidate RawNode 提升为 Node 枚举
impl From<RawNode<Candidate>> for Node {
    // 包装为 Candidate 变体
    fn from(node: RawNode<Candidate>) -> Self {
        // 构造 Candidate 节点
        Node::Candidate(node)
    // 结束函数
    }
// 结束函数
}

// Follower RawNode 提升为 Node 枚举
impl From<RawNode<Follower>> for Node {
    // 包装为 Follower 变体
    fn from(node: RawNode<Follower>) -> Self {
        // 构造 Follower 节点
        Node::Follower(node)
    // 结束函数
    }
// 结束函数
}

// Leader RawNode 提升为 Node 枚举
impl From<RawNode<Leader>> for Node {
    // 包装为 Leader 变体
    fn from(node: RawNode<Leader>) -> Self {
        // 构造 Leader 节点
        Node::Leader(node)
    // 结束函数
    }
// 结束函数
}

// 角色标记 trait：约束 RawNode 的泛型角色
pub trait Role {}

// 带具体角色状态的 Raft 节点内核
pub struct RawNode<R: Role> {
    // 本节点 ID
    id: NodeID,
    /// 当前生效的成员配置（日志中最新成员条目，追加后即生效）。
    membership: MembershipState,
    // Raft 日志
    log: Log,
    // 状态机
    state: Box<dyn State>,
    // 出站通道
    tx: Sender<Envelope>,
    // 运行选项
    opts: Options,
    // 角色专属状态（Follower/Candidate/Leader）
    role: R,
// 结束代码块
}

// 所有角色共享的通用方法
impl<R: Role> RawNode<R> {
    // 仅替换角色字段，保留日志/成员/状态机等
    fn into_role<T: Role>(self, role: T) -> RawNode<T> {
        // 重建同配置不同角色的 RawNode
        RawNode {
            // 保留节点 ID
            id: self.id,
            // 保留成员配置状态
            membership: self.membership,
            // 保留日志
            log: self.log,
            // 保留状态机
            state: self.state,
            // 保留发送通道
            tx: self.tx,
            // 保留选项
            opts: self.opts,
            // 换上新角色状态
            role,
        // 结束代码块
        }
    // 结束代码块
    }

    // 从持久化 term/vote 读取当前任期
    fn term(&self) -> Term {
        // term 存于日志元数据 (term, voted_for)
        self.log.get_term_vote().0
    // 结束函数
    }

    // 除自身外的同伴集合（用于广播）
    fn peers(&self) -> BTreeSet<NodeID> {
        // 由成员配置推导 peers
        self.membership.peers_of(self.id)
    // 结束函数
    }

    // 是否接受该发送方消息：自身或当前投票成员
    fn is_known_sender(&self, from: NodeID) -> bool {
        // 自身回环或配置内成员
        from == self.id || self.membership.all_voters().contains(&from)
    // 结束函数
    }

    /// 配置中的投票节点数（joint 时为并集大小，仅用于进度 map 等）。
    fn cluster_size(&self) -> usize {
        // 统计 all_voters 大小
        self.membership.all_voters().len()
    // 结束函数
    }

    // 在配置区间内随机选举超时，打散同时超时
    fn random_election_timeout(&self) -> Ticks {
        // 均匀采样 [start, end)
        rand::rng().random_range(self.opts.election_timeout_range.clone())
    // 结束函数
    }

    /// 选举超时上界，用于 check-quorum 窗口。
    fn election_timeout_max(&self) -> Ticks {
        // 取 range.end-1，至少为 1
        self.opts.election_timeout_range.end.saturating_sub(1).max(1)
    // 结束函数
    }

    // 向指定节点发送带本任期的消息
    fn send(&self, to: NodeID, message: Message) -> Result<()> {
        // 封装 Envelope 并经通道投递
        Self::send_via(&self.tx, Envelope { from: self.id, to, term: self.term(), message })
    // 结束函数
    }

    // 底层发送：写通道并记录调试日志
    fn send_via(tx: &Sender<Envelope>, msg: Envelope) -> Result<()> {
        // 记录出站消息
        debug!("Sending {msg:?}");
        // 通道错误上浮为 Result
        Ok(tx.send(msg)?)
    // 结束结构/枚举构造
    }

    // 向所有同伴广播同一消息（如心跳/拉票）
    fn broadcast(&self, message: Message) -> Result<()> {
        // 遍历当前 peers
        for id in self.peers() {
            // 逐个发送克隆后的消息体
            self.send(id, message.clone())?;
        // 结束循环
        }
        // 广播完成
        Ok(())
    // 结束结构/枚举构造
    }

    /// 日志追加后立即应用其中的成员配置。
    fn maybe_apply_membership_from_entries(&mut self, entries: &[Entry]) {
        // 扫描刚追加/拼接的条目
        for e in entries {
            // 仅处理带 membership 的配置条目
            if let Some(ref m) = e.membership {
                // 记录配置切换
                info!("Node {} adopting membership from log index {}: {m:?}", self.id, e.index);
                // 更新本地 MembershipState
                self.membership.apply_entry(m);
            // 结束条件分支
            }
        // 结束条件分支
        }
    // 结束条件分支
    }

    /// 从日志恢复最新成员配置（启动时）。
    fn restore_membership_from_log(&mut self) -> Result<()> {
        // 读取日志中最新 membership 条目
        if let Some((_idx, m)) = self.log.latest_membership()? {
            // 应用到内存配置
            self.membership.apply_entry(&m);
            // 若该条目已提交且为 Simple，清除 pending。
            let (commit, _) = self.log.get_commit_index();
            // 最新配置为已提交的 Simple 时清除 pending
            if let Some((idx, MembershipEntry::Simple(_))) = self.log.latest_membership()?
                // 条目索引不超过 commit 才算已提交
                && idx <= commit
            // 进入代码块
            {
                // 无在途成员变更
                self.membership.change_pending = false;
            // 结束条件分支
            }
        // 结束条件分支
        }
        // 恢复完成
        Ok(())
    // 结束结构/枚举构造
    }
// 结束结构/枚举构造
}

// =============================================================================
// Follower
// =============================================================================

// 跟随者角色状态
pub struct Follower {
    // 当前已知领导者；None 表示无主
    leader: Option<NodeID>,
    // 距上次收到领导者消息的 tick 计数
    leader_seen: Ticks,
    // 本任期随机选举超时阈值
    election_timeout: Ticks,
    // 已转发至领导者、等待回包的客户端请求 ID
    forwarded: HashSet<RequestID>,
// 结束代码块
}

// 跟随者构造辅助
impl Follower {
    // 创建跟随者状态：未见领导者、清空转发集
    fn new(leader: Option<NodeID>, election_timeout: Ticks) -> Self {
        // leader_seen 从 0 起算
        Self { leader, leader_seen: 0, election_timeout, forwarded: HashSet::new() }
    // 结束结构/枚举构造
    }
// 结束结构/枚举构造
}

// Follower 实现 Role 标记
impl Role for Follower {}

// 跟随者节点行为：选举超时、复制、投票、客户端代理
impl RawNode<Follower> {
    // 以跟随者身份初始化集群节点
    fn new(
        // 节点 ID
        id: NodeID,
        // 初始同伴
        peers: HashSet<NodeID>,
        // 日志
        log: Log,
        // 状态机
        state: Box<dyn State>,
        // 发送通道
        tx: Sender<Envelope>,
        // 选项
        opts: Options,
    // 初始化可能失败
    ) -> Result<Self> {
        // ID 不得出现在 peers 中
        if peers.contains(&id) {
            // 参数错误
            return errinput!("node ID {id} can't be in peers");
        // 结束条件分支
        }
        // 用自身与 peers 引导初始 Simple 配置
        let membership = MembershipState::bootstrap(id, peers.iter().copied());
        // 占位角色，稍后填随机超时
        let role = Follower::new(None, 0);
        // 组装 RawNode
        let mut node = Self { id, membership, log, state, tx, opts, role };
        // 为跟随者抽一次随机选举超时
        node.role.election_timeout = node.random_election_timeout();
        // 从持久化日志恢复成员配置
        node.restore_membership_from_log()?;
        // 补齐已提交但未 apply 的条目
        node.maybe_apply()?;
        // 返回就绪跟随者
        Ok(node)
    // 结束结构/枚举构造
    }

    /// `use_prevote`: true 时先 Pre-vote；false 时直接真选举（单节点或关闭 pre_vote）。
    fn into_candidate(mut self, use_prevote: bool) -> Result<RawNode<Candidate>> {
        // 角色切换前中止未完成的转发请求
        self.abort_forwarded()?;
        // 切换前尽量应用已提交日志
        self.maybe_apply()?;
        // 新角色使用新的随机选举超时
        let election_timeout = self.random_election_timeout();
        // 根据参数与配置决定是否走 Pre-vote
        let phase = if use_prevote && self.opts.pre_vote {
            // Pre-vote 相位：不抬升本地任期
            ElectionPhase::PreVote
        // 不走 Pre-vote 时直接真选举
        } else {
            // 真选举相位：立即 term+1 并持久化自投
            ElectionPhase::Election
        // 结束条件分支
        };
        // 切换为候选人角色
        let mut node = self.into_role(Candidate::new(election_timeout, phase));
        // 按相位发起预选或真选
        match node.role.phase {
            // 广播 PreCampaign
            ElectionPhase::PreVote => node.pre_campaign()?,
            // 广播 Campaign 并自投
            ElectionPhase::Election => node.campaign()?,
        // 结束 match 分支/块
        }
        // 返回候选人节点
        Ok(node)
    // 结束结构/枚举构造
    }

    // 转为跟随者：跟随已知领导或发现更高任期
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        // term 0 非法，协议从 1 起
        assert_ne!(term, 0, "can't become follower in term 0");
        // 切换前中止转发中的客户端请求
        self.abort_forwarded()?;

        // 已知领导者：同任期追随
        if let Some(leader) = leader {
            // 领导者必须在当前投票配置内
            assert!(self.membership.all_voters().contains(&leader), "leader is not a voter");
            // 同任期不应重复设置领导
            assert_eq!(self.role.leader, None, "already have leader in term");
            // 追随的领导任期须与本地一致
            assert_eq!(term, self.term(), "can't follow leader in different term");
            // 记录开始跟随
            info!("Following leader {leader} in term {term}");
            // 写入领导 ID，保留原选举超时
            self.role = Follower::new(Some(leader), self.role.election_timeout);
        // 无已知领导时的分支
        } else {
            // 无主跟随者：必须来自更高任期
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            // 记录发现新任期
            info!("Discovered new term {term}");
            // 持久化新任期并清空投票
            self.log.set_term_vote(term, None)?;
            // 无主并重置随机选举超时
            self.role = Follower::new(None, self.random_election_timeout());
        // 结束代码块
        }
        // 返回跟随者
        Ok(self)
    // 结束结构/枚举构造
    }

    // 跟随者消息处理主循环
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Pre-vote 消息：不因更高 term 而切换任期。
        if matches!(msg.message, Message::PreCampaign { .. } | Message::PreCampaignResponse { .. }) {
            // 委托 step_prevote
            return self.step_prevote(msg);
        // 结束条件分支
        }

        // 过期任期消息直接丢弃
        if msg.term < self.term() {
            // 调试记录
            debug!("Dropping message from past term: {msg:?}");
            // 保持跟随者
            return Ok(self.into());
        // 结束结构/枚举构造
        }
        // 更高任期：先无主降级再重放该消息
        if msg.term > self.term() {
            // 递归 step 以在新任期处理
            return self.into_follower(msg.term, None)?.step(msg);
        // 结束条件分支
        }

        // 来自当前领导者的任意消息重置见领导计时
        if Some(msg.from) == self.role.leader {
            // leader_seen 清零，推迟选举超时
            self.role.leader_seen = 0;
        // 结束条件分支
        }

        // 按消息类型分支
        match msg.message {
            // 心跳：同步 commit 并应答 match/read_seq
            Message::Heartbeat { last_index, commit_index, read_seq } => {
                // commit 不得越过领导者 last_index
                assert!(commit_index <= last_index, "commit_index after last_index");
                // 校验心跳来源是否为当前领导
                match self.role.leader {
                    // 非当前领导的心跳忽略
                    Some(leader) if msg.from != leader => {
                        // 告警双重领导迹象
                        warn!(
                            // 说明当前领导
                            "node {} ignoring Heartbeat from {} (current leader {})",
                            // 附带冲突方
                            self.id, msg.from, leader
                        // 业务逻辑
                        );
                        // 不处理冲突心跳
                        return Ok(self.into());
                    // 结束结构/枚举构造
                    }
                    // 已有正确领导：继续
                    Some(_) => {}
                    // 无主时通过心跳确立领导者
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                // 结束结构/枚举构造
                }
                // 日志匹配则回报 match，否则 0 触发探测
                let match_index = if self.log.has(last_index, msg.term)? { last_index } else { 0 };
                // 心跳应答携带 match_index 与读序列
                self.send(msg.from, Message::HeartbeatResponse { match_index, read_seq })?;
                // 仅当日志匹配时才可安全推进本地 commit
                if match_index != 0 && commit_index > self.log.get_commit_index().0 {
                    // 提交到领导者给出的 commit_index
                    self.log.commit(commit_index)?;
                    // 提交后应用到状态机
                    self.maybe_apply()?;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // 日志复制 AppendEntries
            Message::Append { base_index, base_term, entries } => {
                // 一致性检查：base 应对齐首条前一索引
                if let Some(first) = entries.first() {
                    // base_index 等于 first.index - 1
                    assert_eq!(base_index, first.index - 1, "base index mismatch");
                // 结束条件分支
                }
                // 校验 Append 是否来自当前领导
                match self.role.leader {
                    // 非领导 Append 忽略
                    Some(leader) if msg.from != leader => {
                        // 告警
                        warn!(
                            // 冲突来源
                            "node {} ignoring Append from {} (current leader {})",
                            // 当前领导
                            self.id, msg.from, leader
                        // 业务逻辑
                        );
                        // 丢弃
                        return Ok(self.into());
                    // 结束结构/枚举构造
                    }
                    // 已有领导
                    Some(_) => {}
                    // 无主时确立领导
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                // 结束结构/枚举构造
                }
                // prevLog 匹配（或 base=0）则接受拼接
                if base_index == 0 || self.log.has(base_index, base_term)? {
                    // 匹配点为最后一条新条目或 base
                    let match_index = entries.last().map(|e| e.index).unwrap_or(base_index);
                    // splice：截断冲突后缀并追加
                    self.log.splice(entries.clone())?;
                    // 配置在日志中出现后立即生效（论文联合共识）。
                    self.maybe_apply_membership_from_entries(&entries);
                    // 成功应答 match_index
                    self.send(msg.from, Message::AppendResponse { match_index, reject_index: 0 })?;
                // 普通业务条目（非成员变更）分支
                } else {
                    // 拒绝：回报可回退的 reject_index
                    let reject_index = min(base_index, self.log.get_last_index().0 + 1);
                    // match_index=0 表示拒绝
                    self.send(msg.from, Message::AppendResponse { reject_index, match_index: 0 })?;
                // 结束结构/枚举构造
                }
            // 结束结构/枚举构造
            }

            // 线性一致读探测：跟随者仅回显 seq
            Message::Read { seq } => {
                // 校验来源为当前领导
                match self.role.leader {
                    // 非领导 Read 忽略
                    Some(leader) if msg.from != leader => {
                        // 告警
                        warn!(
                            // 冲突说明
                            "node {} ignoring Read from {} (current leader {})",
                            // 当前领导
                            self.id, msg.from, leader
                        // 业务逻辑
                        );
                        // 丢弃
                        return Ok(self.into());
                    // 结束结构/枚举构造
                    }
                    // 已有领导
                    Some(_) => {}
                    // 无主时确立领导
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                // 结束结构/枚举构造
                }
                // 确认本节点存活，供领导者统计读多数
                self.send(msg.from, Message::ReadResponse { seq })?;
            // 结束结构/枚举构造
            }

            // 真选举投票请求 RequestVote
            Message::Campaign { last_index, last_term } => {
                // 本任期已投票给他人则拒绝
                if let (_, Some(vote)) = self.log.get_term_vote()
                    // 仅允许投给已记录的候选人
                    && msg.from != vote
                // 进入代码块
                {
                    // 拒绝票
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    // 结束处理
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 比较候选人日志新旧（term 优先，再 index）
                let (log_index, log_term) = self.log.get_last_index();
                // 本地日志更新则拒票，保证选主带最新日志
                if log_term > last_term || log_term == last_term && log_index > last_index {
                    // 拒绝
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 授予选票
                info!("Voting for {} in term {} election", msg.from, msg.term);
                // 持久化 term 与 voted_for
                self.log.set_term_vote(msg.term, Some(msg.from))?;
                // 赞成票
                self.send(msg.from, Message::CampaignResponse { vote: true })?;
            // 结束结构/枚举构造
            }

            // 客户端请求：跟随者代理转发领导者
            Message::ClientRequest { id, request: _ } => {
                // 仅接受本节点注入的客户端请求；其它 from 直接 Abort，避免 panic。
                if msg.from != self.id {
                    // 外来 ClientRequest 拒绝
                    warn!(
                        // 说明节点与来源
                        "node {} rejecting ClientRequest from foreign sender {}",
                        // 外来 from
                        self.id, msg.from
                    // 业务逻辑
                    );
                    // 回复 Abort
                    self.send(
                        // 目标为外来发送方
                        msg.from,
                        // 错误响应
                        Message::ClientResponse { id, response: Err(Error::Abort) },
                    // 完成发送并向上传播错误
                    )?;
                    // 结束
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 已知领导则转发
                if let Some(leader) = self.role.leader {
                    // 记录转发
                    debug!("Forwarding request to leader {leader}: {msg:?}");
                    // 跟踪请求 id 以便回包
                    self.role.forwarded.insert(id);
                    // 原样转给领导者
                    self.send(leader, msg.message)?;
                // 无已知领导时的分支
                } else {
                    // 无主：无法服务，Abort
                    self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
                // 结束结构/枚举构造
                }
            // 结束结构/枚举构造
            }

            // 领导者对转发请求的响应
            Message::ClientResponse { id, response } => {
                // 必须来自当前领导
                if Some(msg.from) != self.role.leader {
                    // 非领导响应忽略
                    warn!(
                        // 告警
                        "node {} ignoring ClientResponse from non-leader {}",
                        // 来源
                        self.id, msg.from
                    // 业务逻辑
                    );
                    // 丢弃
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 仅当仍在转发集合中才回传客户端
                if self.role.forwarded.remove(&id) {
                    // 回环给本节点客户端出口
                    self.send(self.id, Message::ClientResponse { id, response })?;
                // 结束结构/枚举构造
                }
            // 结束结构/枚举构造
            }

            // 跟随者不收集选票响应
            Message::CampaignResponse { .. } => {}

            // Pre-vote 已在入口处理
            Message::PreCampaign { .. } | Message::PreCampaignResponse { .. } => {
                // 不可达分支
                unreachable!("handled above")
            // 结束结构/枚举构造
            }

            // 安装快照：落后跟随者用快照追赶
            Message::InstallSnapshot {
                // 快照覆盖到的最后索引
                last_included_index,
                // 对应任期
                last_included_term,
                // 状态机快照字节
                data,
                // 快照中的成员配置
                membership,
            // 匹配该模式后进入处理体
            } => {
                // 校验来源为当前领导
                match self.role.leader {
                    // 非领导快照忽略
                    Some(leader) if msg.from != leader => {
                        // 告警
                        warn!(
                            // 冲突
                            "node {} ignoring InstallSnapshot from {} (current leader {})",
                            // 当前领导
                            self.id, msg.from, leader
                        // 业务逻辑
                        );
                        // 丢弃
                        return Ok(self.into());
                    // 结束结构/枚举构造
                    }
                    // 已有领导
                    Some(_) => {}
                    // 无主时确立领导
                    None => self = self.into_follower(msg.term, Some(msg.from))?,
                // 结束结构/枚举构造
                }
                // 忽略过期 / 重复快照，避免把状态机回滚到更旧点。
                let (snap_idx, _) = self.log.get_snapshot_meta();
                // 状态机已应用点
                let applied = self.state.get_applied_index();
                // 只接受不落后于本地快照/已应用的快照
                if last_included_index >= snap_idx && last_included_index >= applied {
                    // 恢复状态机到快照点
                    self.state.restore(&data, last_included_index)?;
                    // 持久化快照字节便于重启
                    self.log.engine.set(&super::log::Key::SnapshotData.encode(), data.clone())?;
                    // 日志丢弃快照点之前条目并更新元数据
                    self.log.reset_with_snapshot(last_included_index, last_included_term)?;
                    // 采用快照内成员配置
                    self.membership.apply_entry(&membership);
                    // 快照中的配置视为已提交生效。
                    if matches!(membership, MembershipEntry::Simple(_)) {
                        // 无在途变更
                        self.membership.change_pending = false;
                    // 结束条件分支
                    }
                // 结束条件分支
                }
                // 无论是否应用都应答，避免领导者卡住
                self.send(
                    // 回给领导者
                    msg.from,
                    // 携带 last_included_index 推进 match
                    Message::InstallSnapshotResponse { last_included_index },
                // 完成发送并向上传播错误
                )?;
            // 结束结构/枚举构造
            }

            // 跟随者不处理快照应答
            Message::InstallSnapshotResponse { .. } => {}

            // 领导者专属响应不应到达跟随者
            Message::HeartbeatResponse { .. }
            // Append 应答
            | Message::AppendResponse { .. }
            // 读确认应答
            | Message::ReadResponse { .. } => {
                // 协议错误：panic 暴露 bug
                panic!("follower received unexpected message {msg:?}")
            // 结束结构/枚举构造
            }
        // 结束结构/枚举构造
        };
        // 跟随者 step 完成，包装回 Node
        Ok(self.into())
    // 结束结构/枚举构造
    }

    // 处理 Pre-vote：不修改本地 term/vote
    fn step_prevote(self, msg: Envelope) -> Result<Node> {
        // 按 Pre-vote 消息类型
        match msg.message {
            // 预选票请求
            Message::PreCampaign { last_index, last_term } => {
                // 仍能联系当前领导则拒绝预选票。
                if self.role.leader.is_some() && self.role.leader_seen < self.role.election_timeout {
                    // 拒预选票
                    self.send(msg.from, Message::PreCampaignResponse { vote: false })?;
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 日志新旧检查，规则同真投票
                let (log_index, log_term) = self.log.get_last_index();
                // log_ok：候选人不落后
                let log_ok =
                    // 本地更新则 false
                    !(log_term > last_term || log_term == last_term && log_index > last_index);
                // 预选票请求的 envelope.term 为 intended term（current+1），不更新本地 term。
                let term_ok = msg.term > self.term();
                // 同时满足日志与任期条件才授预选票
                let vote = log_ok && term_ok;
                // 调试预投票结果
                debug!(
                    // 候选人与 intended term
                    "Pre-vote for {} (term {}): vote={vote} log_ok={log_ok} term_ok={term_ok}",
                    // 来源与任期
                    msg.from, msg.term
                // 业务逻辑
                );
                // 发送预选票结果（不持久化）
                self.send(msg.from, Message::PreCampaignResponse { vote })?;
            // 结束结构/枚举构造
            }
            // 跟随者不累计预选票
            Message::PreCampaignResponse { .. } => {
                // 忽略
                // 跟随者不收集预选票。
            }
            // 其它消息忽略
            _ => {}
        // 结束结构/枚举构造
        }
        // Pre-vote 处理结束
        Ok(self.into())
    // 结束结构/枚举构造
    }

    // 跟随者时钟：累计未见领导时间
    fn tick(mut self) -> Result<Node> {
        // 每个 tick 增加 leader_seen
        self.role.leader_seen += 1;
        // 达到选举超时则发起竞选（优先 Pre-vote）
        if self.role.leader_seen >= self.role.election_timeout {
            // 转为候选人并进入选举流程
            return Ok(self.into_candidate(true)?.into());
        // 结束结构/枚举构造
        }
        // 未超时：保持跟随者
        Ok(self.into())
    // 结束结构/枚举构造
    }

    // 角色切换时中止所有在途转发请求
    fn abort_forwarded(&mut self) -> Result<()> {
        // 取出并按 id 排序，保证回复顺序稳定
        for id in std::mem::take(&mut self.role.forwarded).into_iter().sorted() {
            // 调试中止
            debug!("Aborting forwarded request {id}");
            // 向本节点客户端回 Abort
            self.send(self.id, Message::ClientResponse { id, response: Err(Error::Abort) })?;
        // 结束结构/枚举构造
        }
        // 清理完成
        Ok(())
    // 结束结构/枚举构造
    }

    // 将已提交未应用的日志应用到状态机
    fn maybe_apply(&mut self) -> Result<()> {
        // 从 applied_index 之后扫描
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        // 逐条取出
        while let Some(entry) = iter.next().transpose()? {
            // 调试 apply
            debug!("Applying {entry:?}");
            // 成员条目提交时更新 pending 等
            if let Some(ref m) = entry.membership {
                // on_commit 处理配置提交语义
                self.membership.on_commit(m);
            // 结束条件分支
            }
            // 成员变更条目：状态机收到 noop 式 command=None。
            _ = self.state.apply(entry);
        // 结束条件分支
        }
        // apply 完成
        Ok(())
    // 结束结构/枚举构造
    }
// 结束结构/枚举构造
}

// =============================================================================
// Candidate (+ Pre-vote phase)
// =============================================================================

// 选举相位为轻量可拷贝枚举，便于匹配与日志
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// 选举相位：预选或真选
enum ElectionPhase {
    // Pre-vote：试探多数，不抬升任期
    PreVote,
    // 真选举：term+1 并持久化自投
    Election,
// 结束类型定义
}

// 候选人角色状态
pub struct Candidate {
    // 已获（预）选票的节点集合
    votes: HashSet<NodeID>,
    // 本轮选举已进行的 tick
    election_duration: Ticks,
    // 本轮选举超时阈值
    election_timeout: Ticks,
    // 当前处于 PreVote 还是 Election
    phase: ElectionPhase,
// 结束代码块
}

// 构造候选人状态
impl Candidate {
    // 空票集、计时归零
    fn new(election_timeout: Ticks, phase: ElectionPhase) -> Self {
        // 初始化各字段
        Self { votes: HashSet::new(), election_duration: 0, election_timeout, phase }
    // 结束结构/枚举构造
    }
// 结束结构/枚举构造
}

// Candidate 实现 Role
impl Role for Candidate {}

// 候选人行为：预选、拉票、计票、当选/落选
impl RawNode<Candidate> {
    // 落选或发现更高任期，转为跟随者
    fn into_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RawNode<Follower>> {
        // 新跟随者使用新随机超时
        let election_timeout = self.random_election_timeout();
        // 跟随已知领导（同任期）
        if let Some(leader) = leader {
            // 任期必须一致
            assert_eq!(term, self.term(), "can't follow leader in different term");
            // 记录落选跟随
            info!("Lost election, following leader {leader} in term {term}");
            // 切换角色并带上领导 ID
            Ok(self.into_role(Follower::new(Some(leader), election_timeout)))
        // 无已知领导时的分支
        } else {
            // 无主：必须更高任期
            assert_ne!(term, self.term(), "can't become leaderless follower in current term");
            // 记录新任期
            info!("Discovered new term {term}");
            // 持久化新任期、清空投票
            self.log.set_term_vote(term, None)?;
            // 无主跟随者
            Ok(self.into_role(Follower::new(None, election_timeout)))
        // 结束结构/枚举构造
        }
    // 结束结构/枚举构造
    }

    // 获得多数真选票后转领导者
    fn into_leader(self) -> Result<RawNode<Leader>> {
        // 校验持久化投票状态
        let (term, vote) = self.log.get_term_vote();
        // 领导任期不可为 0
        assert_ne!(term, 0, "leaders can't have term 0");
        // 当选者必须已自投
        assert_eq!(vote, Some(self.id), "leader did not vote for self");
        // 仅真选举胜出可当领导（Pre-vote 不够）
        assert_eq!(self.role.phase, ElectionPhase::Election, "must win real election");

        // 记录当选
        info!("Won election for term {term}, becoming leader");
        // 当前同伴集
        let peers = self.peers();
        // 日志末端作为初始 next_index 基准
        let (last_index, _) = self.log.get_last_index();
        // 构造领导者：各 peer 进度从 last+1 起
        let mut node = self.into_role(Leader::new(peers, last_index));
        // 空条目（no-op）确立本任期领导权并推进提交
        node.propose(None)?;
        // 尝试提交并应用（单节点立即提交）
        let _ = node.maybe_commit_and_apply()?;
        // 立即广播心跳宣告领导
        node.heartbeat()?;
        // 返回领导者
        Ok(node)
    // 结束结构/枚举构造
    }

    // 候选人消息处理
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // Pre-vote：不因更高 term 切换；可向日志足够新的节点授予预选票。
        if let Message::PreCampaign { last_index, last_term } = msg.message {
            // 本地日志末端
            let (log_index, log_term) = self.log.get_last_index();
            // 日志是否不比候选人新
            let log_ok =
                // term/index 比较
                !(log_term > last_term || log_term == last_term && log_index > last_index);
            // intended term 应大于本地 term；同 term 的预选也允许（大家都在 term T 抢 T+1）。
            let term_ok = msg.term > self.term();
            // 日志与任期都通过才投预选票
            let vote = log_ok && term_ok;
            // 回复预选票
            self.send(msg.from, Message::PreCampaignResponse { vote })?;
            // 保持候选人
            return Ok(self.into());
        // 结束结构/枚举构造
        }
        // 收集预选票响应
        if let Message::PreCampaignResponse { vote } = msg.message {
            // 仅 PreVote 相位且赞成票有效
            if self.role.phase == ElectionPhase::PreVote && vote {
                // 发送方须在投票配置内
                if self.membership.all_voters().contains(&msg.from) {
                    // 记入票集
                    self.role.votes.insert(msg.from);
                // 结束条件分支
                }
                // 预选票达多数则启动真选举
                if self.quorum_reached(&self.role.votes) {
                    // 记录 Pre-vote 成功
                    info!("Pre-vote won, starting real election");
                    // 抬升任期、自投、广播 Campaign
                    self.campaign()?;
                    // 单节点或已够多数时立即当选。
                    if self.quorum_reached(&self.role.votes)
                        // 相位已切到 Election
                        && self.role.phase == ElectionPhase::Election
                    // 进入代码块
                    {
                        // 转为领导者
                        return Ok(self.into_leader()?.into());
                    // 结束结构/枚举构造
                    }
                // 结束结构/枚举构造
                }
            // 结束结构/枚举构造
            }
            // 预选票未达多数：继续等待
            return Ok(self.into());
        // 结束结构/枚举构造
        }

        // 过期任期消息丢弃
        if msg.term < self.term() {
            // 调试
            debug!("Dropping message from past term: {msg:?}");
            // 保持
            return Ok(self.into());
        // 结束结构/枚举构造
        }
        // 更高任期：降为无主跟随者并重放
        if msg.term > self.term() {
            // 递归处理
            return self.into_follower(msg.term, None)?.step(msg);
        // 结束条件分支
        }

        // 真选举相关消息
        match msg.message {
            // 获得赞成票
            Message::CampaignResponse { vote: true } => {
                // 仅在真选举相位计票
                if self.role.phase == ElectionPhase::Election {
                    // 投票者须在配置内
                    if self.membership.all_voters().contains(&msg.from) {
                        // 记票
                        self.role.votes.insert(msg.from);
                    // 结束条件分支
                    }
                    // 达多数则当选
                    if self.quorum_reached(&self.role.votes) {
                        // 进入领导者
                        return Ok(self.into_leader()?.into());
                    // 结束结构/枚举构造
                    }
                // 结束结构/枚举构造
                }
            // 结束结构/枚举构造
            }
            // 反对票：忽略，等超时再选
            Message::CampaignResponse { vote: false } => {}
            // 同任期他人拉票：候选人已自投，拒票
            Message::Campaign { .. } => {
                // 明确拒绝
                self.send(msg.from, Message::CampaignResponse { vote: false })?;
            // 结束结构/枚举构造
            }
            // 收到领导类消息：说明已有领导，立即追随
            Message::Heartbeat { .. }
            // Append
            | Message::Append { .. }
            // Read 探测
            | Message::Read { .. }
            // 快照
            | Message::InstallSnapshot { .. } => {
                // 降为该发送方的跟随者并重放消息
                return self.into_follower(msg.term, Some(msg.from))?.step(msg);
            // 结束结构/枚举构造
            }
            // 选举期间拒绝客户端请求
            Message::ClientRequest { id, request: _ } => {
                // Abort
                self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
            // 结束结构/枚举构造
            }
            // 忽略快照应答
            Message::InstallSnapshotResponse { .. } => {}
            // 不应出现的领导侧响应
            Message::HeartbeatResponse { .. }
            // Append 应答
            | Message::AppendResponse { .. }
            // 读应答
            | Message::ReadResponse { .. }
            // 客户端应答
            | Message::ClientResponse { .. }
            // PreCampaign 已处理
            | Message::PreCampaign { .. }
            // PreCampaignResponse 已处理
            | Message::PreCampaignResponse { .. } => {
                // 异常消息 panic
                panic!("unexpected message {msg:?}")
            // 结束结构/枚举构造
            }
        // 结束结构/枚举构造
        }
        // 候选人 step 结束
        Ok(self.into())
    // 结束结构/枚举构造
    }

    // 候选人时钟：选举超时则重启一轮
    fn tick(mut self) -> Result<Node> {
        // 累计本轮时长
        self.role.election_duration += 1;
        // 超时
        if self.role.election_duration >= self.role.election_timeout {
            // 超时后重新从 Pre-vote 开始（若启用），避免 term 风暴。
            if self.opts.pre_vote {
                // 重置为 PreVote 并广播
                self.pre_campaign()?;
            // 不走 Pre-vote 时直接真选举
            } else {
                // 关闭 Pre-vote：直接真选举
                self.campaign()?;
            // 结束条件分支
            }
        // 结束条件分支
        }
        // 未超时或重启后继续候选
        Ok(self.into())
    // 结束结构/枚举构造
    }

    /// 当前票集是否达到（联合）法定人数。候选人总是含自己。
    fn quorum_reached(&self, votes: &HashSet<NodeID>) -> bool {
        // 转为有序集合供 has_quorum
        let matched: BTreeSet<NodeID> = votes.iter().copied().collect();
        // 委托成员配置的多数判断（joint 需双多数）
        self.membership.has_quorum(&matched)
    // 结束函数
    }

    // 发起 Pre-vote 轮次
    fn pre_campaign(&mut self) -> Result<()> {
        // 新随机超时
        let timeout = self.random_election_timeout();
        // 重置角色为 PreVote 空票集
        self.role = Candidate::new(timeout, ElectionPhase::PreVote);
        // 预选票先计入自己
        self.role.votes.insert(self.id);
        // 单节点：直接进入真选举。
        if self.cluster_size() == 1 {
            // campaign
            return self.campaign();
        // 结束条件分支
        }
        // 带上日志末端供他人比较新旧
        let (last_index, last_term) = self.log.get_last_index();
        // Pre-vote 使用 intended term = current+1，接收方不持久化。
        let intended = self.term() + 1;
        // 记录预选开始
        info!("Starting pre-vote for intended term {intended}");
        // 向每个同伴发 PreCampaign
        for id in self.peers() {
            // 直接构造 Envelope 以写入 intended term
            Self::send_via(
                // 使用本节点通道
                &self.tx,
                // 信封
                Envelope {
                    // 发送方
                    from: self.id,
                    // 目标同伴
                    to: id,
                    // intended term 而非本地 term
                    term: intended,
                    // 预选消息体
                    message: Message::PreCampaign { last_index, last_term },
                // 业务逻辑
                },
            // 完成发送并向上传播错误
            )?;
        // 结束结构/枚举构造
        }
        // 已有自己一票；若自己已构成多数（不应发生在多节点）则直接竞选。
        if self.quorum_reached(&self.role.votes) {
            // 进入 campaign
            self.campaign()?;
        // 结束条件分支
        }
        // 预选发起完成
        Ok(())
    // 结束结构/枚举构造
    }

    // 发起真选举：term+1、自投、广播 RequestVote
    fn campaign(&mut self) -> Result<()> {
        // 新任期
        let term = self.term() + 1;
        // 记录
        info!("Starting new election for term {term}");
        // 新超时
        let timeout = self.random_election_timeout();
        // 重置为 Election 相位
        self.role = Candidate::new(timeout, ElectionPhase::Election);
        // 自投票
        self.role.votes.insert(self.id);
        // 持久化新任期与 voted_for=self
        self.log.set_term_vote(term, Some(self.id))?;
        // 日志位置用于投票比较
        let (last_index, last_term) = self.log.get_last_index();
        // 广播 Campaign
        self.broadcast(Message::Campaign { last_index, last_term })?;
        // 单节点等已达多数：由调用方 into_leader
        if self.quorum_reached(&self.role.votes) {
            // 单节点等情况：立即当选由调用方 into_leader；此处仅标记。
            // 实际 into_leader 在 step/tick 路径；对单节点 Node::new 会链式调用。
        }
        // 真选举发起完成
        Ok(())
    // 结束结构/枚举构造
    }
// 结束结构/枚举构造
}

// =============================================================================
// Leader
// =============================================================================

// 领导者角色状态：复制进度、读写队列、心跳与活跃性
pub struct Leader {
    // 每个跟随者的 match/next/read_seq 进度
    progress: HashMap<NodeID, Progress>,
    // 在途写请求：日志索引到客户端
    writes: HashMap<Index, Write>,
    /// 在途成员变更客户端请求（joint 条目索引）。
    membership_writes: HashMap<Index, Write>,
    // 有序读请求队列（按 read_seq）
    reads: VecDeque<Read>,
    // 领导者发出的读序列号，单调递增
    read_seq: ReadSequence,
    // 距上次心跳的 tick
    since_heartbeat: Ticks,
    /// 自上次收到该 peer 有效响应以来的 tick；自身不计入。
    peer_seen: HashMap<NodeID, Ticks>,
// 结束代码块
}

// 单跟随者复制与读确认进度
struct Progress {
    // 已确认匹配的最高日志索引
    match_index: Index,
    // 下一条待发送的日志索引
    next_index: Index,
    // 该跟随者已确认的最大读序列
    read_seq: ReadSequence,
// 结束类型定义
}

// 进度推进辅助
impl Progress {
    // 跟随者日志匹配点前进
    fn advance(&mut self, match_index: Index) -> bool {
        // 不前进则忽略陈旧应答
        if match_index <= self.match_index {
            // 返回 false
            return false;
        // 结束条件分支
        }
        // 更新 match_index
        self.match_index = match_index;
        // next 至少为 match+1
        self.next_index = max(self.next_index, match_index + 1);
        // 发生了有效推进
        true
    // 结束代码块
    }

    // 读序列确认前进
    fn advance_read(&mut self, read_seq: ReadSequence) -> bool {
        // 陈旧 read_seq 忽略
        if read_seq <= self.read_seq {
            // false
            return false;
        // 结束条件分支
        }
        // 更新
        self.read_seq = read_seq;
        // 有效推进
        true
    // 结束条件分支
    }

    // Append 被拒后回退 next_index
    fn regress_next(&mut self, next_index: Index) -> bool {
        // 不能回退到已匹配之后或无效位置
        if next_index >= self.next_index || self.next_index <= self.match_index + 1 {
            // 无需回退
            return false;
        // 结束条件分支
        }
        // next 不低于 match+1
        self.next_index = max(next_index, self.match_index + 1);
        // 发生回退
        true
    // 结束条件分支
    }
// 结束代码块
}

// 在途写：记录客户端来源与请求 id
struct Write {
    // 客户端节点（常为本节点）
    from: NodeID,
    // 请求关联 id
    id: RequestID,
// 结束类型定义
}

// 在途线性一致读
struct Read {
    // 对应 read_seq
    seq: ReadSequence,
    // 客户端来源
    from: NodeID,
    // 请求 id
    id: RequestID,
    // 只读命令字节
    command: Vec<u8>,
// 结束代码块
}

// 领导者状态构造
impl Leader {
    // 按同伴初始化进度与活跃计时
    fn new(peers: BTreeSet<NodeID>, last_index: Index) -> Self {
        // 乐观假设同伴缺最后一条，从 last+1 开始
        let next_index = last_index + 1;
        // 为每个 peer 建 Progress
        let progress = peers
            // 遍历同伴
            .iter()
            // 复制
            .copied()
            // match=0, read_seq=0, next=last+1
            .map(|p| (p, Progress { next_index, match_index: 0, read_seq: 0 }))
            // 聚合为最终集合/映射
            .collect();
        // peer_seen 初始 0 表示刚当选视为刚联系过
        let peer_seen = peers.iter().copied().map(|p| (p, 0)).collect();
        // 组装 Leader 状态
        Self {
            // 进度表
            progress,
            // 无在途写
            writes: HashMap::new(),
            // 无在途成员变更写
            membership_writes: HashMap::new(),
            // 空读队列
            reads: VecDeque::new(),
            // 读序列从 0 起
            read_seq: 0,
            // 立即允许发心跳
            since_heartbeat: 0,
            // 活跃性表
            peer_seen,
        // 结束代码块
        }
    // 结束代码块
    }
// 结束代码块
}

// Leader 实现 Role
impl Role for Leader {}

// 领导者行为：心跳、复制、提交、读写、成员变更、CheckQuorum
impl RawNode<Leader> {
    /// 因更高任期或 check-quorum 下台。
    fn into_follower(mut self, term: Term) -> Result<RawNode<Follower>> {
        // check-quorum 可能同任期下台。
        if term > self.term() {
            // 记录
            info!("Discovered new term {term}");
            // set_term_vote
            self.log.set_term_vote(term, None)?;
        // 同任期下台或无更高任期时的分支
        } else {
            // 同任期 step-down（CheckQuorum/移出配置）
            info!("Leader stepping down in term {}", self.term());
        // 结束条件分支
        }

        // 中止所有在途写请求
        for write in std::mem::take(&mut self.role.writes).into_values().sorted_by_key(|w| w.id) {
            // 客户端 Abort
            self.send(write.from, Message::ClientResponse { id: write.id, response: Err(Error::Abort) })?;
        // 结束结构/枚举构造
        }
        // 中止在途成员变更请求
        for write in
            // 按 id 排序
            std::mem::take(&mut self.role.membership_writes).into_values().sorted_by_key(|w| w.id)
        // 遍历
        {
            // Abort
            self.send(write.from, Message::ClientResponse { id: write.id, response: Err(Error::Abort) })?;
        // 结束结构/枚举构造
        }
        // 中止在途读请求
        for read in std::mem::take(&mut self.role.reads).into_iter().sorted_by_key(|r| r.id) {
            // Abort
            self.send(read.from, Message::ClientResponse { id: read.id, response: Err(Error::Abort) })?;
        // 结束结构/枚举构造
        }

        // 下台后使用新随机选举超时
        let election_timeout = self.random_election_timeout();
        // 无主跟随者
        Ok(self.into_role(Follower::new(None, election_timeout)))
    // 结束结构/枚举构造
    }

    // 记录 peer 刚有有效响应，供 CheckQuorum
    fn note_peer_response(&mut self, from: NodeID) {
        // 存在则清零 seen
        if let Some(seen) = self.role.peer_seen.get_mut(&from) {
            // 重置活跃计时
            *seen = 0;
        // 结束条件分支
        }
    // 结束条件分支
    }

    // 成员变更后同步 progress/peer_seen 与 peers 集合
    fn sync_progress_with_membership(&mut self) {
        // 当前同伴
        let peers = self.peers();
        // 日志末端
        let (last_index, _) = self.log.get_last_index();
        // 新同伴从 last+1 开始追赶
        let next_index = last_index + 1;
        // 添加新同伴。
        for p in &peers {
            // or_insert 避免覆盖已有 match
            self.role.progress.entry(*p).or_insert(Progress {
                // 初始 next
                next_index,
                // 尚未匹配
                match_index: 0,
                // 读序列 0
                read_seq: 0,
            // 结束映射/闭包构造
            });
            // 新同伴活跃计时
            self.role.peer_seen.entry(*p).or_insert(0);
        // 结束代码块
        }
        // 移除旧同伴。
        self.role.progress.retain(|id, _| peers.contains(id));
        // 同步 peer_seen
        self.role.peer_seen.retain(|id, _| peers.contains(id));
    // 结束代码块
    }

    // 领导者消息处理主循环
    fn step(mut self, msg: Envelope) -> Result<Node> {
        // 成员变更提交后可能需要 step_down
        let mut step_down = false;
        // 领导者拒绝预选票：表明自己仍存活
        if matches!(msg.message, Message::PreCampaign { .. }) {
            // 领导者拒绝预选票（自己仍活着）。
            self.send(msg.from, Message::PreCampaignResponse { vote: false })?;
            // 结束
            return Ok(self.into());
        // 结束结构/枚举构造
        }
        // 领导者不收集预选票
        if matches!(msg.message, Message::PreCampaignResponse { .. }) {
            // 忽略
            return Ok(self.into());
        // 结束结构/枚举构造
        }

        // 过期任期丢弃
        if msg.term < self.term() {
            // 调试
            debug!("Dropping message from past term: {msg:?}");
            // 保持领导
            return Ok(self.into());
        // 结束结构/枚举构造
        }
        // 更高任期：下台并重放
        if msg.term > self.term() {
            // into_follower 后 step
            return self.into_follower(msg.term)?.step(msg);
        // 结束条件分支
        }

        // 按消息类型处理
        match msg.message {
            // 心跳应答：更新读确认、探测落后、推进提交
            Message::HeartbeatResponse { match_index, read_seq } => {
                // 已移除的 peer 忽略
                if !self.role.progress.contains_key(&msg.from) {
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 标记 peer 活跃
                self.note_peer_response(msg.from);
                // 本地日志末端
                let (last_index, _) = self.log.get_last_index();
                // match 不能超过本地 last
                assert!(match_index <= last_index, "future match index");
                // read_seq 不能超过领导者当前值
                assert!(read_seq <= self.role.read_seq, "future read sequence number");

                // 读序列前进则尝试放行读
                if self.progress(msg.from).advance_read(read_seq) {
                    // maybe_read
                    self.maybe_read()?;
                // 结束条件分支
                }
                // match=0 表示日志不匹配，回退并探测
                if match_index == 0 {
                    // 将 next 拉向 last 以便 probe
                    self.progress(msg.from).regress_next(last_index);
                    // 强制 probe Append
                    self.maybe_send_append(msg.from, true)?;
                // 结束条件分支
                }
                // match 前进则尝试提交
                if self.progress(msg.from).advance(match_index) {
                    // 可能因移出配置需下台
                    step_down |= self.maybe_commit_and_apply()?.1;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // 成功的 Append 应答
            Message::AppendResponse { match_index, reject_index: 0 } if match_index > 0 => {
                // 未知 peer 忽略
                if !self.role.progress.contains_key(&msg.from) {
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 活跃
                self.note_peer_response(msg.from);
                // 末端
                let (last_index, _) = self.log.get_last_index();
                // 校验
                assert!(match_index <= last_index, "future match index");
                // 推进 match
                if self.progress(msg.from).advance(match_index) {
                    // 尝试提交/应用
                    step_down |= self.maybe_commit_and_apply()?.1;
                // 结束条件分支
                }
                // 流水线继续发送后续条目
                self.maybe_send_append(msg.from, false)?;
            // 结束条件分支
            }

            // 独立读确认应答
            Message::ReadResponse { seq } => {
                // 未知 peer
                if !self.role.progress.contains_key(&msg.from) {
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 活跃
                self.note_peer_response(msg.from);
                // 推进 read_seq
                if self.progress(msg.from).advance_read(seq) {
                    // 尝试完成读
                    self.maybe_read()?;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // Append 被拒绝：回退 next 并重发
            Message::AppendResponse { reject_index, match_index: 0 } if reject_index > 0 => {
                // 未知 peer
                if !self.role.progress.contains_key(&msg.from) {
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 活跃
                self.note_peer_response(msg.from);
                // 末端
                let (last_index, _) = self.log.get_last_index();
                // reject 不得超 last
                assert!(reject_index <= last_index, "future reject index");
                // 拒绝点不大于已 match：陈旧应答
                if reject_index <= self.progress(msg.from).match_index {
                    // 忽略
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 回退 next_index
                if self.progress(msg.from).regress_next(reject_index) {
                    // probe 重发
                    self.maybe_send_append(msg.from, true)?;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // 非法 AppendResponse 形态
            Message::AppendResponse { .. } => panic!("invalid message {msg:?}"),

            // 客户端写：追加日志并登记回调
            Message::ClientRequest { id, request: Request::Write(command) } => {
                // propose 复制 command
                let index = self.propose(Some(command))?;
                // 记录写等待提交
                self.role.writes.insert(index, Write { from: msg.from, id });
                // 单节点立即提交
                if self.cluster_size() == 1 {
                    // 提交/应用，检查 step_down
                    step_down |= self.maybe_commit_and_apply()?.1;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // 带会话的幂等写
            Message::ClientRequest {
                // 请求 id
                id,
                // 会话字段
                request: Request::WriteSession { client_id, seq, command },
            // 处理
            } => {
                // 编码为状态机可识别的会话包装
                let wrapped = super::session::encode_session(client_id, seq, command);
                // 作为普通写提出
                let index = self.propose(Some(wrapped))?;
                // 登记回调
                self.role.writes.insert(index, Write { from: msg.from, id });
                // 单节点快路径
                if self.cluster_size() == 1 {
                    // 提交
                    step_down |= self.maybe_commit_and_apply()?.1;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // 线性一致读：分配 read_seq 并广播确认
            Message::ClientRequest { id, request: Request::Read(command) } => {
                // 递增全局读序列
                self.role.read_seq += 1;
                // 入队读请求
                let read = Read { seq: self.role.read_seq, from: msg.from, id, command };
                // 保持 FIFO
                self.role.reads.push_back(read);
                // 广播 Read 让跟随者确认领导仍在
                self.broadcast(Message::Read { seq: self.role.read_seq })?;
                // 单节点无需多数确认
                if self.cluster_size() == 1 {
                    // 立即 maybe_read
                    self.maybe_read()?;
                // 结束条件分支
                }
            // 结束条件分支
            }

            // 集群状态查询：本地即可回答
            Message::ClientRequest { id, request: Request::Status } => {
                // 组装 Status
                let response = self.status().map(Response::Status);
                // 直接响应客户端
                self.send(msg.from, Message::ClientResponse { id, response })?;
            // 结束结构/枚举构造
            }

            // 成员变更请求
            Message::ClientRequest {
                // 请求 id
                id,
                // 新投票集合
                request: Request::ChangeMembership { voters },
            // 处理
            } => {
                // 提出 joint 配置
                let result = self.propose_membership_change(voters);
                // 成功或立即失败
                match result {
                    // 成功：登记在途成员变更写
                    Ok(index) => {
                        // 等待 joint 提交后回复
                        self.role.membership_writes.insert(index, Write { from: msg.from, id });
                        // 单节点提交
                        if self.cluster_size() == 1 {
                            // 检查 step_down
                            step_down |= self.maybe_commit_and_apply()?.1;
                        // 结束条件分支
                        }
                    // 结束条件分支
                    }
                    // 失败：立即错误响应（如已有在途变更）
                    Err(e) => {
                        // 发送
                        self.send(
                            // 客户端
                            msg.from,
                            // 错误
                            Message::ClientResponse { id, response: Err(e) },
                        // 完成发送并向上传播错误
                        )?;
                    // 结束结构/枚举构造
                    }
                // 结束结构/枚举构造
                }
            // 结束结构/枚举构造
            }

            // 同任期竞选：领导者拒票
            Message::Campaign { .. } => {
                // vote=false
                self.send(msg.from, Message::CampaignResponse { vote: false })?;
            // 结束结构/枚举构造
            }
            // 忽略选票响应
            Message::CampaignResponse { .. } => {}
            // 同任期不应出现另一领导的复制/读消息
            Message::Heartbeat { .. } | Message::Append { .. } | Message::Read { .. } => {
                // 同任期不应出现另一领导；丢弃以免陈旧/异常消息拖垮进程。
                warn!(
                    // 说明
                    "leader {} ignoring peer-leader message from {} in term {}",
                    // 来源与任期
                    self.id, msg.from, msg.term
                // 业务逻辑
                );
            // 结束结构/枚举构造
            }
            // 快照安装成功应答：推进 match
            Message::InstallSnapshotResponse { last_included_index } => {
                // 未知 peer
                if !self.role.progress.contains_key(&msg.from) {
                    // 返回
                    return Ok(self.into());
                // 结束结构/枚举构造
                }
                // 活跃
                self.note_peer_response(msg.from);
                // 用 last_included 作为 match
                if self.progress(msg.from).advance(last_included_index) {
                    // 尝试提交
                    step_down |= self.maybe_commit_and_apply()?.1;
                // 结束条件分支
                }
                // 继续追加后续日志
                self.maybe_send_append(msg.from, false)?;
            // 结束条件分支
            }
            // 领导者不应安装他人快照
            Message::InstallSnapshot { .. } => {
                // 告警忽略
                warn!("leader {} ignoring InstallSnapshot from {}", self.id, msg.from);
            // 结束结构/枚举构造
            }
            // 异常消息
            Message::ClientResponse { .. }
            // PreCampaign 已处理
            | Message::PreCampaign { .. }
            // panic
            | Message::PreCampaignResponse { .. } => panic!("unexpected message {msg:?}"),
        // 结束结构/枚举构造
        }

        // 本步处理中触发领导转移/移出配置
        if step_down {
            // 当前任期下台
            let term = self.term();
            // 转为无主跟随者
            return Ok(self.into_follower(term)?.into());
        // 结束结构/枚举构造
        }
        // 保持领导者
        Ok(self.into())
    // 结束结构/枚举构造
    }

    // 领导者时钟：心跳与 CheckQuorum
    fn tick(mut self) -> Result<Node> {
        // 推进 peer_seen。
        for seen in self.role.peer_seen.values_mut() {
            // 饱和递增防溢出
            *seen = seen.saturating_add(1);
        // 结束循环
        }

        // 心跳间隔计时
        self.role.since_heartbeat += 1;
        // 到达心跳周期
        if self.role.since_heartbeat >= self.opts.heartbeat_interval {
            // 广播 Heartbeat（含 commit 与 read_seq）
            self.heartbeat()?;
        // 结束条件分支
        }

        // CheckQuorum：窗口内活跃节点（含自己）是否构成多数。
        if self.opts.check_quorum && self.cluster_size() > 1 {
            // 活跃窗口约等于选举超时上界
            let window = self.election_timeout_max();
            // 活跃集合
            let mut matched: BTreeSet<NodeID> = BTreeSet::new();
            // 领导者自身算活跃
            matched.insert(self.id);
            // 检查每个 peer
            for (peer, seen) in &self.role.peer_seen {
                // 窗口内有过响应
                if *seen < window {
                    // 计入活跃
                    matched.insert(*peer);
                // 结束条件分支
                }
            // 结束条件分支
            }
            // 不构成（联合）多数则主动下台
            if !self.membership.has_quorum(&matched) {
                // 告警
                warn!(
                    // 失联多数
                    "CheckQuorum: leader {} lost majority contact, stepping down",
                    // 本节点
                    self.id
                // 业务逻辑
                );
                // 同任期下台
                let term = self.term();
                // 转为跟随者
                return Ok(self.into_follower(term)?.into());
            // 结束结构/枚举构造
            }
        // 结束结构/枚举构造
        }

        // 通过 CheckQuorum，保持领导
        Ok(self.into())
    // 结束结构/枚举构造
    }

    // 广播心跳：宣告存活并捎带 commit/read_seq
    fn heartbeat(&mut self) -> Result<()> {
        // 领导者日志末端
        let (last_index, last_term) = self.log.get_last_index();
        // 当前提交点
        let (commit_index, _) = self.log.get_commit_index();
        // 当前读序列
        let read_seq = self.role.read_seq;
        // 领导者 last 条目必须属本任期（no-op 保证）
        assert_eq!(last_term, self.term(), "leader's last_term not in current term");
        // 重置心跳计时
        self.role.since_heartbeat = 0;
        // 向所有同伴发 Heartbeat
        self.broadcast(Message::Heartbeat { last_index, commit_index, read_seq })
    // 结束结构/枚举构造
    }

    // 领导者提出新日志条目并触发复制
    fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        // 追加到本地日志，返回索引
        let index = self.log.append(command)?;
        // 对进度刚好指向该索引的 peer 立即发送
        for peer in self.peers() {
            // 避免无谓空转
            if index == self.progress(peer).next_index {
                // 发送 Append
                self.maybe_send_append(peer, false)?;
            // 结束条件分支
            }
        // 结束条件分支
        }
        // 返回新条目索引
        Ok(index)
    // 结束结构/枚举构造
    }

    // 提出成员变更：写入 Joint(old,new) 并即时生效
    fn propose_membership_change(&mut self, voters: HashSet<NodeID>) -> Result<Index> {
        // 禁止重叠的成员变更
        if self.membership.change_pending {
            // 错误
            return Err(Error::InvalidInput("membership change already in progress".into()));
        // 结束条件分支
        }
        // 空投票集非法
        if voters.is_empty() {
            // 错误
            return Err(Error::InvalidInput("voters must not be empty".into()));
        // 结束条件分支
        }
        // 允许移除当前领导：Simple 配置提交后领导 step down（领导转移）。
        let old = match &self.membership.active {
            // 取出旧配置
            MembershipEntry::Simple(m) => m.clone(),
            // 已在 joint 中禁止嵌套
            MembershipEntry::Joint { .. } => {
                // 错误
                return Err(Error::InvalidInput("already in joint consensus".into()));
            // 结束 match 分支/块
            }
        // 结束 match 分支/块
        };
        // 由目标 voters 构造新 Membership
        let new = Membership::from_iter(voters);
        // 无变化则拒绝
        if old == new {
            // 错误
            return Err(Error::InvalidInput("membership unchanged".into()));
        // 结束条件分支
        }

        // 构造 Joint 条目
        let joint = MembershipEntry::Joint { old, new: new.clone() };
        // 记录
        info!("Proposing joint membership: {joint:?}");
        // 追加成员日志
        let index = self.log.append_membership(joint.clone())?;
        // 立即采用 joint（两侧都需多数提交）
        self.membership.apply_entry(&joint);
        // 为新节点建 progress
        self.sync_progress_with_membership();

        // 向需要的 peer 推送该配置条目
        for peer in self.peers() {
            // 进度对齐时发送
            if index == self.progress(peer).next_index {
                // Append
                self.maybe_send_append(peer, false)?;
            // 结束条件分支
            }
        // 结束条件分支
        }
        // 返回 joint 日志索引
        Ok(index)
    // 结束结构/枚举构造
    }

    /// 提交 joint 后自动提出 Simple(C_new)。
    fn maybe_propose_simple_after_joint(&mut self, committed: &MembershipEntry) -> Result<()> {
        // 仅对 Joint 生效
        if let MembershipEntry::Joint { new, .. } = committed {
            // 目标配置的 Simple 形态
            let simple = MembershipEntry::Simple(new.clone());
            // 记录
            info!("Joint committed, proposing simple membership: {simple:?}");
            // 追加 Simple 成员条目
            let index = self.log.append_membership(simple.clone())?;
            // 立即采用 Simple
            self.membership.apply_entry(&simple);
            // 同步进度（可能移除旧节点）
            self.sync_progress_with_membership();
            // 推送新配置
            for peer in self.peers() {
                // 对齐则发
                if index == self.progress(peer).next_index {
                    // Append
                    self.maybe_send_append(peer, false)?;
                // 结束条件分支
                }
            // 结束条件分支
            }
        // 结束条件分支
        }
        // 非 Joint 无操作
        Ok(())
    // 结束结构/枚举构造
    }

    // 根据多数 match 推进 commit，并 apply、回复客户端
    fn maybe_commit_and_apply(&mut self) -> Result<(Index, bool)> {
        // 日志末端
        let (last_index, _) = self.log.get_last_index();

        // 基于当前（可能 joint）配置计算可提交索引：
        // 从 last_index 向下找第一个被多数复制的本任期索引。
        let mut commit_index = self.log.get_commit_index().0;
        // 自高向低找第一个可提交索引
        for idx in (commit_index + 1..=last_index).rev() {
            // 读取条目
            let Some(entry) = self.log.get(idx)? else { continue };
            // 只提交本任期条目（Raft 安全规则）
            if entry.term != self.term() {
                // 跳过其它任期
                continue;
            // 结束条件分支
            }
            // 统计复制到该索引的节点
            let mut matched: BTreeSet<NodeID> = BTreeSet::new();
            matched.insert(self.id); // 领导者已拥有
            // 遍历 progress
            for (peer, p) in &self.role.progress {
                // match 达到 idx
                if p.match_index >= idx {
                    // 计入
                    matched.insert(*peer);
                // 结束条件分支
                }
            // 结束条件分支
            }
            // 达到（联合）多数则选定该 commit
            if self.membership.has_quorum(&matched) {
                // 更新候选 commit_index
                commit_index = idx;
                // 找到最高可提交即停
                break;
            // 结束条件分支
            }
        // 结束条件分支
        }

        // 读取旧提交点
        let (old_index, old_term) = self.log.get_commit_index();
        // 无前进则返回
        if commit_index <= old_index {
            // step_down=false
            return Ok((old_index, false));
        // 结束结构/枚举构造
        }

        // 再次确认提交点属本任期
        match self.log.get(commit_index)? {
            // 本任期 OK
            Some(entry) if entry.term == self.term() => {}
            // 非本任期不可提交
            Some(_) => return Ok((old_index, false)),
            // 缺失条目严重错误
            None => panic!("commit index {commit_index} missing"),
        // 结束结构/枚举构造
        }

        // 持久化新 commit_index
        self.log.commit(commit_index)?;

        // 应用时发送响应需带当前 term
        let term = self.term();
        // 从已应用点扫描到 commit
        let mut iter = self.log.scan_apply(self.state.get_applied_index());
        // 收集本批提交的成员条目，稍后处理 joint 到 simple
        let mut committed_membership = Vec::new();
        // 逐条 apply
        while let Some(entry) = iter.next().transpose()? {
            // 调试
            debug!("Applying {entry:?}");
            // 成员条目
            if let Some(ref m) = entry.membership {
                // 提交语义：如清除 pending
                self.membership.on_commit(m);
                // 记录以便后续 propose simple
                committed_membership.push(m.clone());
            // 结束条件分支
            }

            // 取出对应在途写
            let write = self.role.writes.remove(&entry.index);
            // 取出在途成员变更写
            let mwrite = self.role.membership_writes.remove(&entry.index);
            // 条目索引供响应
            let entry_index = entry.index;

            // 成员变更条目：状态机 noop
            if entry.membership.is_some() {
                // 成员变更：状态机按 noop 应用，推进 applied_index。
                let _ = self.state.apply(Entry {
                    // 索引
                    index: entry.index,
                    // 任期
                    term: entry.term,
                    // 无业务命令
                    command: None,
                    // membership 不传给状态机
                    membership: None,
                // 结束映射/闭包构造
                });
                // 若有客户端等待成员变更结果则回复
                if let Some(Write { id, from: to }) = mwrite {
                    // 经通道发送
                    Self::send_via(
                        // tx
                        &self.tx,
                        // 信封
                        Envelope {
                            // 领导者 id
                            from: self.id,
                            // 任期
                            term,
                            // 客户端
                            to,
                            // 成功响应
                            message: Message::ClientResponse {
                                // 原请求 id
                                id,
                                // 返回配置条目索引
                                response: Ok(Response::ChangeMembership { index: entry_index }),
                            // 业务逻辑
                            },
                        // 业务逻辑
                        },
                    // 完成发送并向上传播错误
                    )?;
                // 结束结构/枚举构造
                }
            // 普通业务条目（非成员变更）分支
            } else {
                // 普通业务条目：apply 并回复写结果
                let result = self.state.apply(entry);
                // 有等待的写则回包
                if let Some(Write { id, from: to }) = write {
                    // 发送
                    Self::send_via(
                        // tx
                        &self.tx,
                        // 信封
                        Envelope {
                            // from
                            from: self.id,
                            // term
                            term,
                            // to 客户端
                            to,
                            // 写响应
                            message: Message::ClientResponse {
                                // id
                                id,
                                // 状态机结果映射为 Write
                                response: result.map(Response::Write),
                            // 业务逻辑
                            },
                        // 业务逻辑
                        },
                    // 完成发送并向上传播错误
                    )?;
                // 结束结构/枚举构造
                }
            // 结束代码块
            }
        // 结束代码块
        }
        // 释放日志扫描迭代器，避免与后续日志操作冲突
        drop(iter);

        // 是否因移出配置而下台
        let mut step_down = false;
        // 处理本批提交的成员配置
        for m in committed_membership {
            // joint 提交则提出 Simple
            self.maybe_propose_simple_after_joint(&m)?;
            // 配置变化后同步进度
            self.sync_progress_with_membership();
            // Simple 配置已生效且自身不在投票集合 → 领导转移完成，下台。
            if matches!(self.membership.active, MembershipEntry::Simple(_))
                // 检查是否仍为 voter
                && !self.membership.all_voters().contains(&self.id)
            // 进入代码块
            {
                // 记录即将下台
                info!(
                    // 节点 id
                    "Leader {} removed from membership, will step down",
                    // 说明
                    self.id
                // 业务逻辑
                );
                // 标记 step_down
                step_down = true;
            // 结束代码块
            }
        // 结束代码块
        }

        // 本任期首次提交后可放行读（领导权确认）
        if old_term != self.term() {
            // maybe_read
            self.maybe_read()?;
        // 结束条件分支
        }

        // 不下台时考虑本地快照压缩
        if !step_down {
            // maybe_snapshot
            self.maybe_snapshot()?;
        // 结束条件分支
        }

        // 返回新 commit 与是否下台
        Ok((commit_index, step_down))
    // 结束结构/枚举构造
    }

    // 达到阈值则对已应用前缀做本地快照并压缩日志
    fn maybe_snapshot(&mut self) -> Result<()> {
        // 读取阈值
        let threshold = self.opts.snapshot_threshold;
        // 0 表示关闭
        if threshold == 0 {
            // 直接返回
            return Ok(());
        // 结束结构/枚举构造
        }
        // 状态机已应用索引
        let applied = self.state.get_applied_index();
        // 当前快照点
        let (snap_idx, _) = self.log.get_snapshot_meta();
        // 增量不足阈值则跳过
        if applied <= snap_idx || applied - snap_idx < threshold {
            // 返回
            return Ok(());
        // 结束结构/枚举构造
        }
        // 提交点
        let (commit, commit_term) = self.log.get_commit_index();
        // 未应用到 commit 前不做快照
        if applied < commit {
            // 返回
            return Ok(());
        // 结束结构/枚举构造
        }
        // 确定 applied 条目的 term
        let term = match self.log.get(applied)? {
            // 正常从日志取
            Some(e) => e.term,
            // 恰为快照点则无需
            None if applied == snap_idx => return Ok(()),
            // 缺失时用 commit_term 兜底
            None => commit_term,
        // 结束条件分支
        };
        // 状态机导出快照字节
        let data = self.state.snapshot()?;
        // 将快照字节存入引擎，便于重启恢复状态机。
        self.log.engine.set(&super::log::Key::SnapshotData.encode(), data)?;
        // 记录压缩
        info!("Compacting log through index {applied} term {term}");
        // 丢弃 applied 之前日志
        self.log.compact_to(applied, term)?;
        // 完成
        Ok(())
    // 结束结构/枚举构造
    }

    // 在领导权与读多数确认后执行只读并回复
    fn maybe_read(&mut self) -> Result<()> {
        // 无在途读
        if self.role.reads.is_empty() {
            // 返回
            return Ok(());
        // 结束结构/枚举构造
        }
        // 当前提交点与任期
        let (commit_index, commit_term) = self.log.get_commit_index();
        // 已应用点
        let applied_index = self.state.get_applied_index();
        // 须本任期已提交且状态机追上 commit 才安全读
        if commit_term < self.term() || applied_index < commit_index {
            // 等待
            return Ok(());
        // 结束结构/枚举构造
        }

        // 读确认：read_seq 被多数确认。
        // 构造每个 voter 的 read_seq（自己为 role.read_seq）。
        let mut matched_seq: Vec<(NodeID, ReadSequence)> = Vec::new();
        // 领导者自身视为已确认当前 read_seq
        matched_seq.push((self.id, self.role.read_seq));
        // 跟随者进度中的 read_seq
        for (peer, p) in &self.role.progress {
            // 加入列表
            matched_seq.push((*peer, p.read_seq));
        // 结束循环
        }

        // 找最大的 seq，使得拥有 >= seq 的节点构成多数。
        let mut quorum_read_seq = 0;
        // 所有出现过的 seq 作为候选
        let candidates: BTreeSet<ReadSequence> = matched_seq.iter().map(|(_, s)| *s).collect();
        // 从大到小找第一个获多数的 seq
        for seq in candidates.into_iter().rev() {
            // 拥有 >= seq 的节点集
            let matched: BTreeSet<NodeID> =
                // 过滤
                matched_seq.iter().filter(|(_, s)| *s >= seq).map(|(id, _)| *id).collect();
            // 多数确认
            if self.membership.has_quorum(&matched) {
                // 记录 quorum_read_seq
                quorum_read_seq = seq;
                // 找到即停
                break;
            // 结束条件分支
            }
        // 结束条件分支
        }

        // 按队列顺序放行 seq 已确认的读
        while let Some(read) = self.role.reads.front() {
            // 后续读尚未获多数
            if read.seq > quorum_read_seq {
                // 停止
                break;
            // 结束条件分支
            }
            // 出队
            let read = self.role.reads.pop_front().unwrap();
            // 状态机只读
            let response = self.state.read(read.command).map(Response::Read);
            // 回复客户端
            self.send(read.from, Message::ClientResponse { id: read.id, response })?;
        // 结束结构/枚举构造
        }
        // 读处理完成
        Ok(())
    // 结束结构/枚举构造
    }

    // 向指定跟随者发送 Append 或快照以追赶日志
    fn maybe_send_append(&mut self, peer: NodeID, mut probe: bool) -> Result<()> {
        // peer 已不在进度表则跳过
        if !self.role.progress.contains_key(&peer) {
            // 返回
            return Ok(());
        // 结束结构/枚举构造
        }
        // 本地 last
        let (last_index, _) = self.log.get_last_index();
        // 取可变进度
        let progress = self.role.progress.get_mut(&peer).unwrap();
        // next 不得为 0
        assert_ne!(progress.next_index, 0, "invalid next_index");
        // next 必须在 match 之后
        assert!(progress.next_index > progress.match_index, "invalid next_index <= match_index");
        // match 不超过 last
        assert!(progress.match_index <= last_index, "invalid match_index > last_index");
        // next 最多 last+1
        assert!(progress.next_index <= last_index + 1, "invalid next_index > last_index + 1");

        // 日志中仍保留的最早索引（快照之后）
        let first_index = self.log.get_first_index();
        // next 落在快照之前：只能发 InstallSnapshot
        if progress.next_index < first_index {
            // 快照元数据
            let (last_included_index, last_included_term) = self.log.get_snapshot_meta();
            // 当前状态机快照
            let data = self.state.snapshot()?;
            // 当前生效成员配置随快照发送
            let membership = self.membership.active.clone();
            // 发送快照消息
            return self.send(
                // 目标 peer
                peer,
                // InstallSnapshot
                Message::InstallSnapshot {
                    // 覆盖索引
                    last_included_index,
                    // 覆盖任期
                    last_included_term,
                    // 数据
                    data,
                    // 配置
                    membership,
                // 业务逻辑
                },
            // 业务逻辑
            );
        // 结束代码块
        }

        // 已完全追平则无需发送
        if progress.match_index == last_index {
            // 返回
            return Ok(());
        // 结束结构/枚举构造
        }
        // probe 模式：next 与 match 有空洞时发空 Append 探测
        probe = probe && progress.next_index > progress.match_index + 1;
        // next 已超 last 且非 probe：无数据可发
        if progress.next_index > last_index && !probe {
            // 返回
            return Ok(());
        // 结束结构/枚举构造
        }

        // 确定 prevLogIndex/prevLogTerm
        let (base_index, base_term) = match progress.next_index {
            // next=0 非法
            0 => panic!("next_index=0 for node {peer}"),
            // 从日志起点复制：base=0
            1 => (0, 0),
            // 否则取 next-1 条目的 index/term
            next => {
                // 缺失 base 条目则严重错误
                self.log.get(next - 1)?.map(|e| (e.index, e.term)).expect("missing base entry")
            // 结束循环
            }
        // 结束循环
        };
        // probe 发空，否则批量取最多 max_append_entries 条
        let entries = match probe {
            // 正常复制
            false => self
                // 从 next 扫描
                .log
                // 范围
                .scan(progress.next_index..)
                // 限制批量大小
                .take(self.opts.max_append_entries)
                // 收集为 Vec
                .try_collect()?,
            // 探测：空 entries
            true => Vec::new(),
        // 结束代码块
        };

        // 乐观推进 next，等待应答确认 match
        if let Some(last) = entries.last() {
            // 发到 last+1
            progress.next_index = last.index + 1;
        // 结束条件分支
        }

        // 调试复制规模
        debug!("Replicating {} entries with base {base_index} to {peer}", entries.len());
        // 发送 AppendEntries
        self.send(peer, Message::Append { base_index, base_term, entries })
    // 结束结构/枚举构造
    }

    // 汇总集群状态供客户端 Status 请求
    fn status(&mut self) -> Result<Status> {
        // 构造 Status
        Ok(Status {
            // 当前领导为自己
            leader: self.id,
            // 当前任期
            term: self.term(),
            // 各节点 match_index 视图
            match_index: self
                // 从 progress
                .role
                // 迭代
                .progress
                // peer 与 match
                .iter()
                // 映射
                .map(|(id, p)| (*id, p.match_index))
                // 领导者自身 match=last_index
                .chain(std::iter::once((self.id, self.log.get_last_index().0)))
                // 收集
                .collect(),
            // 提交点
            commit_index: self.log.get_commit_index().0,
            // 状态机应用点
            applied_index: self.state.get_applied_index(),
            // 存储引擎状态
            storage: self.log.status()?,
            // 当前投票成员
            voters: self.membership.all_voters(),
        // 结束 Status/结构构造表达式
        })
    // 结束代码块
    }

    // 按 id 取可变复制进度，缺失则协议 bug
    fn progress(&mut self, id: NodeID) -> &mut Progress {
        // expect 未知节点
        self.role.progress.get_mut(&id).expect("unknown node")
    // 结束函数
    }
// 结束函数
}
