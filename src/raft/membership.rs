//! Raft 集群成员配置（含联合共识 Joint Consensus）。

// 有序集合：确定性序列化与 quorum 计算
use std::collections::BTreeSet;

// 成员配置写入日志，需可序列化
use serde::{Deserialize, Serialize};

// 投票节点标识
use super::NodeID;

/// 一组投票节点（有序以保证确定性与可序列化比较）。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
// 定义数据结构
pub struct Membership {
    // 当前配置下的投票成员
    pub voters: BTreeSet<NodeID>,
// 结束当前作用域
}

// 为类型实现方法
impl Membership {
    // 从任意迭代器构造成员集合
    pub fn from_iter(iter: impl IntoIterator<Item = NodeID>) -> Self {
        // 收集为 BTreeSet，自动去重排序
        Self { voters: iter.into_iter().collect() }
    // 结束当前作用域
    }

    // 判断某节点是否在投票集合中
    pub fn contains(&self, id: NodeID) -> bool {
        // 业务逻辑步骤
        self.voters.contains(&id)
    // 结束当前作用域
    }

    // 投票成员数量
    pub fn len(&self) -> usize {
        // 业务逻辑步骤
        self.voters.len()
    // 结束当前作用域
    }

    // 是否无投票成员（异常配置）
    pub fn is_empty(&self) -> bool {
        // 业务逻辑步骤
        self.voters.is_empty()
    // 结束当前作用域
    }

    /// 严格多数（> 50%）。
    pub fn quorum_size(&self) -> usize {
        // floor(n/2)+1，与 Raft 论文一致
        self.len() / 2 + 1
    // 结束当前作用域
    }

    /// `matched` 中属于本配置的节点数是否达到法定人数。
    pub fn has_quorum(&self, matched: &BTreeSet<NodeID>) -> bool {
        // 取交集计数，避免把非本配置节点算进多数
        self.voters.intersection(matched).count() >= self.quorum_size()
    // 结束当前作用域
    }
// 结束当前作用域
}

/// 日志中的成员变更条目。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// 定义枚举
pub enum MembershipEntry {
    /// 联合共识：提交需同时满足 old 与 new 的多数。
    Joint { old: Membership, new: Membership },
    /// 单一配置。
    Simple(Membership),
// 结束当前作用域
}

// 为类型实现方法
impl MembershipEntry {
    /// 当前配置下全部可能的投票人（joint 时为并集）。
    pub fn all_voters(&self) -> BTreeSet<NodeID> {
        // Simple 直接克隆；Joint 取 old∪new
        match self {
            // 列表或字段续项
            Self::Simple(m) => m.voters.clone(),
            // 并集：复制与心跳需要触达双方成员
            Self::Joint { old, new } => old.voters.union(&new.voters).copied().collect(),
        // 结束当前作用域
        }
    // 结束当前作用域
    }

    /// 最终目标配置（joint 的 new，或 simple 自身）。
    pub fn target(&self) -> &Membership {
        // Joint 完成后会再提 Simple(new)
        match self {
            // 列表或字段续项
            Self::Simple(m) => m,
            // 列表或字段续项
            Self::Joint { new, .. } => new,
        // 结束当前作用域
        }
    // 结束当前作用域
    }

    // 是否处于联合共识阶段
    pub fn is_joint(&self) -> bool {
        // 业务逻辑步骤
        matches!(self, Self::Joint { .. })
    // 结束当前作用域
    }

    /// `matched` 是否在本配置下构成法定人数。
    pub fn has_quorum(&self, matched: &BTreeSet<NodeID>) -> bool {
        // Simple：单配置多数；Joint：old 与 new 同时多数
        match self {
            // 列表或字段续项
            Self::Simple(m) => m.has_quorum(matched),
            // 联合共识双重多数，防止成员切换时脑裂
            Self::Joint { old, new } => old.has_quorum(matched) && new.has_quorum(matched),
        // 结束当前作用域
        }
    // 结束当前作用域
    }
// 结束当前作用域
}

/// 节点当前生效的成员配置（最新一条成员日志，追加后即生效）。
// 不序列化到消息；由日志回放重建
#[derive(Clone, Debug, PartialEq, Eq)]
// 定义数据结构
pub struct MembershipState {
    /// 当前生效配置。
    pub active: MembershipEntry,
    /// 是否已有未提交的成员变更（一次只允许一个在途变更）。
    pub change_pending: bool,
// 结束当前作用域
}

// 为类型实现方法
impl MembershipState {
    // 启动时用本节点 + 初始 peers 构造 Simple 配置
    pub fn bootstrap(id: NodeID, peers: impl IntoIterator<Item = NodeID>) -> Self {
        // 收集投票人
        let mut voters = BTreeSet::new();
        // 始终包含自己
        voters.insert(id);
        // 加入配置中的同伴
        voters.extend(peers);
        // 初始无在途变更
        Self { active: MembershipEntry::Simple(Membership { voters }), change_pending: false }
    // 结束当前作用域
    }

    // 当前全部投票人（含 joint 并集）
    pub fn all_voters(&self) -> BTreeSet<NodeID> {
        // 业务逻辑步骤
        self.active.all_voters()
    // 结束当前作用域
    }

    // 除自身外的同伴，用于出站复制/心跳
    pub fn peers_of(&self, id: NodeID) -> BTreeSet<NodeID> {
        // 先取全集再移除自己
        let mut all = self.all_voters();
        // 业务逻辑步骤
        all.remove(&id);
        // 语法续行
        all
    // 结束当前作用域
    }

    // 委托 active 配置判断 quorum
    pub fn has_quorum(&self, matched: &BTreeSet<NodeID>) -> bool {
        // 业务逻辑步骤
        self.active.has_quorum(matched)
    // 结束当前作用域
    }

    /// 应用一条成员日志（追加到本地日志后立即调用）。
    pub fn apply_entry(&mut self, entry: &MembershipEntry) {
        // 追加即生效（C_old,new 或 C_new）
        self.active = entry.clone();
        // 标记有在途变更，阻止并发第二次变更
        self.change_pending = true;
    // 结束当前作用域
    }

    /// 成员条目提交后：若刚提交的是 Joint，调用方应再提出 Simple；
    /// 若是 Simple，清除 pending。
    pub fn on_commit(&mut self, entry: &MembershipEntry) {
        // 按条目类型更新 pending 标志
        match entry {
            // 业务逻辑步骤
            MembershipEntry::Joint { .. } => {
                // 仍处于变更流程，等待后续 Simple 提交。
                self.change_pending = true;
            // 结束当前作用域
            }
            // 业务逻辑步骤
            MembershipEntry::Simple(_) => {
                // Simple 提交后变更流程结束
                self.change_pending = false;
            // 结束当前作用域
            }
        // 结束当前作用域
        }
    // 结束当前作用域
    }
// 结束当前作用域
}
