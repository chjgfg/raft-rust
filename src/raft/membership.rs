//! Raft 集群成员配置（含联合共识 Joint Consensus）。

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::NodeID;

/// 一组投票节点（有序以保证确定性与可序列化比较）。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub voters: BTreeSet<NodeID>,
}

impl Membership {
    pub fn from_iter(iter: impl IntoIterator<Item = NodeID>) -> Self {
        Self { voters: iter.into_iter().collect() }
    }

    pub fn contains(&self, id: NodeID) -> bool {
        self.voters.contains(&id)
    }

    pub fn len(&self) -> usize {
        self.voters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    /// 严格多数（> 50%）。
    pub fn quorum_size(&self) -> usize {
        self.len() / 2 + 1
    }

    /// `matched` 中属于本配置的节点数是否达到法定人数。
    pub fn has_quorum(&self, matched: &BTreeSet<NodeID>) -> bool {
        self.voters.intersection(matched).count() >= self.quorum_size()
    }
}

/// 日志中的成员变更条目。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipEntry {
    /// 联合共识：提交需同时满足 old 与 new 的多数。
    Joint { old: Membership, new: Membership },
    /// 单一配置。
    Simple(Membership),
}

impl MembershipEntry {
    /// 当前配置下全部可能的投票人（joint 时为并集）。
    pub fn all_voters(&self) -> BTreeSet<NodeID> {
        match self {
            Self::Simple(m) => m.voters.clone(),
            Self::Joint { old, new } => old.voters.union(&new.voters).copied().collect(),
        }
    }

    /// 最终目标配置（joint 的 new，或 simple 自身）。
    pub fn target(&self) -> &Membership {
        match self {
            Self::Simple(m) => m,
            Self::Joint { new, .. } => new,
        }
    }

    pub fn is_joint(&self) -> bool {
        matches!(self, Self::Joint { .. })
    }

    /// `matched` 是否在本配置下构成法定人数。
    pub fn has_quorum(&self, matched: &BTreeSet<NodeID>) -> bool {
        match self {
            Self::Simple(m) => m.has_quorum(matched),
            Self::Joint { old, new } => old.has_quorum(matched) && new.has_quorum(matched),
        }
    }
}

/// 节点当前生效的成员配置（最新一条成员日志，追加后即生效）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipState {
    /// 当前生效配置。
    pub active: MembershipEntry,
    /// 是否已有未提交的成员变更（一次只允许一个在途变更）。
    pub change_pending: bool,
}

impl MembershipState {
    pub fn bootstrap(id: NodeID, peers: impl IntoIterator<Item = NodeID>) -> Self {
        let mut voters = BTreeSet::new();
        voters.insert(id);
        voters.extend(peers);
        Self { active: MembershipEntry::Simple(Membership { voters }), change_pending: false }
    }

    pub fn all_voters(&self) -> BTreeSet<NodeID> {
        self.active.all_voters()
    }

    pub fn peers_of(&self, id: NodeID) -> BTreeSet<NodeID> {
        let mut all = self.all_voters();
        all.remove(&id);
        all
    }

    pub fn has_quorum(&self, matched: &BTreeSet<NodeID>) -> bool {
        self.active.has_quorum(matched)
    }

    /// 应用一条成员日志（追加到本地日志后立即调用）。
    pub fn apply_entry(&mut self, entry: &MembershipEntry) {
        self.active = entry.clone();
        self.change_pending = true;
    }

    /// 成员条目提交后：若刚提交的是 Joint，调用方应再提出 Simple；
    /// 若是 Simple，清除 pending。
    pub fn on_commit(&mut self, entry: &MembershipEntry) {
        match entry {
            MembershipEntry::Joint { .. } => {
                // 仍处于变更流程，等待后续 Simple 提交。
                self.change_pending = true;
            }
            MembershipEntry::Simple(_) => {
                self.change_pending = false;
            }
        }
    }
}
