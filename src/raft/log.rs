use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

use super::membership::MembershipEntry;
use super::{NodeID, Term};
use crate::error::Result;
use crate::storage;

/// 日志索引（条目位置）。从 1 开始；0 表示无索引。
pub type Index = u64;

/// Bincode 标准配置，用于日志条目与元数据的序列化。
const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 用 bincode 序列化值。
fn encode_value<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(value, BINCODE).expect("value must be serializable")
}

/// 用 bincode 反序列化值。
fn decode_value<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)?.0)
}

/// 包含状态机命令的日志条目。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// 条目索引。
    ///
    /// 编码值里其实可以省略索引（键里也有），但为简单起见仍保留。
    pub index: Index,
    /// 条目被加入时的任期。
    pub term: Term,
    /// 状态机命令。None（noop）用于领导者选举时提交旧条目，见 Raft 论文 5.4.2 节。
    /// 与 [`Self::membership`] 互斥：成员变更条目的 command 应为 None。
    pub command: Option<Vec<u8>>,
    /// 集群成员配置变更（联合共识 / 单一配置）。追加到日志后立即生效。
    #[serde(default)]
    pub membership: Option<MembershipEntry>,
}

impl Entry {
    fn encode(&self) -> Vec<u8> {
        encode_value(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_value(bytes)
    }
}

/// 日志存储键。
///
/// 编码为固定前缀 + 可选的大端索引，保证 `Entry(i)` 按索引字典序排列，
/// 且全部 Entry 键排在元数据键之前：
/// * `Entry(index)` → `[0x00] ‖ index.to_be_bytes()`
/// * `TermVote`     → `[0x01]`
/// * `CommitIndex`  → `[0x02]`
/// * `SnapshotMeta` → `[0x03]`  (last_included_index, last_included_term)
/// * `SnapshotData` → `[0x04]`  状态机快照字节
#[derive(Clone, Debug, PartialEq)]
pub enum Key {
    /// 日志条目，保存任期与命令。
    Entry(Index),
    /// 保存当前任期与投票（若有）。
    TermVote,
    /// 保存当前 commit 索引（若有）。
    CommitIndex,
    /// 快照元数据：`(last_included_index, last_included_term)`。
    SnapshotMeta,
    /// 状态机快照原始字节。
    SnapshotData,
}

impl Key {
    /// 编码为有序字节键。
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Key::Entry(index) => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(0x00);
                buf.extend_from_slice(&index.to_be_bytes());
                buf
            }
            Key::TermVote => vec![0x01],
            Key::CommitIndex => vec![0x02],
            Key::SnapshotMeta => vec![0x03],
            Key::SnapshotData => vec![0x04],
        }
    }
}

/// Raft 日志保存一系列任意命令（通常是写操作），在节点间复制，并顺序应用到本地状态机。
/// 每条日志含索引、命令，以及领导者提出它时的任期。命令可为 noop（None），
/// 在选出领导者时追加（见论文 5.4.2 节）。示例：
///
/// Index | Term | Command
/// ------|------|------------------------------------------------------
///   1   |   1  | None
///   2   |   1  | CREATE TABLE table (id INT PRIMARY KEY, value STRING)
///   3   |   1  | INSERT INTO table VALUES (1, 'foo')
///   4   |   2  | None
///   5   |   2  | UPDATE table SET value = 'bar' WHERE id = 1
///   6   |   2  | DELETE FROM table WHERE id = 1
///
/// 注意这只是示意；实际命令不必是 SQL，而是任意底层写操作。
///
/// 使用键值存储按索引落盘日志条目，以及若干元数据键（例如本任期投票给了谁）。
///
/// 稳态下日志只追加：客户端提交命令后，领导者经 [`Log::append`] 写入本地日志，
/// 再复制给跟随者，跟随者经 [`Log::splice`] 追加。当某索引已复制到多数节点时
/// 即变为已提交，此前日志不可变，并保证最终所有节点都会拥有它。
/// 节点通过 [`Log::commit`] 跟踪 commit 索引，并将已提交命令应用到状态机。
///
/// 但未提交条目可被替换或删除。领导者可能已追加却无法达成共识
///（例如无法与多数节点通信）。若另选新领导者并在相同索引写入不同命令，
/// 旧领导者或跟随者发现后，会用新领导者的条目替换未提交部分。
///
/// Raft 日志不变量：
///
/// * 条目索引从 1 起连续（无空洞）。
/// * 条目任期相对前一条从不下降。
/// * 条目任期不超过当前任期。
/// * 追加的条目是持久的（刷盘）。
/// * 追加的条目使用当前任期。
/// * 已提交条目在快照后可截断前缀（见 [`Log::compact_to`]）。
/// * 已提交条目最终会复制到所有节点。
/// * 相同索引/任期的条目含相同命令。
/// * 若两份日志在某索引/任期匹配，则此前所有条目相同（见论文 5.3 节）。
pub struct Log {
    /// 底层存储引擎。使用 trait 对象而非泛型，以便运行时选择引擎，
    /// 并避免把泛型参数传遍整个 Raft。
    pub engine: Box<dyn storage::Engine>,
    /// 当前任期。
    term: Term,
    /// 本任期的领导者投票（若有）。
    vote: Option<NodeID>,
    /// 日志中仍保留的第一条条目索引（截断后 > 1）。
    first_index: Index,
    /// 快照最后包含的任期（first_index-1 对应的 term；无快照时为 0）。
    snapshot_term: Term,
    /// 最后一条已存条目的索引。
    last_index: Index,
    /// 最后一条已存条目的任期。
    last_term: Term,
    /// 最后一条已提交条目的索引。
    commit_index: Index,
    /// 最后一条已提交条目的任期。
    commit_term: Term,
    /// 为 true 时，追加后 fsync 到磁盘。这是 Raft 要求的，但有明显性能代价
    ///（尤其是未做批量 fsync 优化时）。关闭可大幅提升写性能，但崩溃可能丢数据，
    /// 某些场景下会导致日志“未提交”与状态机分叉。
    fsync: bool,
}

impl Log {
    /// 使用给定存储引擎初始化日志。
    pub fn new(mut engine: Box<dyn storage::Engine>) -> Result<Self> {
        // 从磁盘加载初始内存状态。
        let (term, vote) = engine
            .get(&Key::TermVote.encode())?
            .as_deref()
            .map(decode_value)
            .transpose()?
            .unwrap_or((0, None));
        let (mut last_index, mut last_term) = engine
            .scan_dyn((
                Bound::Included(Key::Entry(0).encode()),
                Bound::Included(Key::Entry(u64::MAX).encode()),
            ))
            .last()
            .transpose()?
            .map(|(_, v)| Entry::decode(&v))
            .transpose()?
            .map(|e| (e.index, e.term))
            .unwrap_or((0, 0));
        let (mut commit_index, mut commit_term) = engine
            .get(&Key::CommitIndex.encode())?
            .as_deref()
            .map(decode_value)
            .transpose()?
            .unwrap_or((0, 0));
        let (snap_index, snapshot_term) = engine
            .get(&Key::SnapshotMeta.encode())?
            .as_deref()
            .map(decode_value)
            .transpose()?
            .unwrap_or((0, 0));
        // first_index = 快照之后下一条；无快照且无日志时为 1。
        let mut first_index = if snap_index > 0 {
            snap_index + 1
        } else if last_index == 0 {
            1
        } else {
            // 扫描最小 entry 索引
            engine
                .scan_dyn((
                    Bound::Included(Key::Entry(0).encode()),
                    Bound::Included(Key::Entry(u64::MAX).encode()),
                ))
                .next()
                .transpose()?
                .map(|(_, v)| Entry::decode(&v))
                .transpose()?
                .map(|e| e.index)
                .unwrap_or(1)
        };

        // 快照之后若无剩余日志条目，last/commit 至少要覆盖快照基座。
        if snap_index > 0 {
            if last_index < snap_index {
                last_index = snap_index;
                last_term = snapshot_term;
            }
            if commit_index < snap_index {
                commit_index = snap_index;
                commit_term = snapshot_term;
            }
            if first_index != snap_index + 1 {
                first_index = snap_index + 1;
            }
        }

        let fsync = true; // 默认开启 fsync
        Ok(Self {
            engine,
            term,
            vote,
            first_index,
            snapshot_term,
            last_index,
            last_term,
            commit_index,
            commit_term,
            fsync,
        })
    }

    /// 日志中仍保留的第一条索引。
    pub fn get_first_index(&self) -> Index {
        self.first_index
    }

    /// 快照基座：`(last_included_index, last_included_term)`；无快照时 `(0,0)`。
    pub fn get_snapshot_meta(&self) -> (Index, Term) {
        if self.first_index <= 1 {
            (0, 0)
        } else {
            (self.first_index - 1, self.snapshot_term)
        }
    }

    /// 截断并删除 `<= last_included_index` 的日志条目（快照后压缩）。
    pub fn compact_to(&mut self, last_included_index: Index, last_included_term: Term) -> Result<()> {
        assert!(last_included_index <= self.commit_index, "compact beyond commit");
        if last_included_index + 1 <= self.first_index && last_included_index > 0 {
            return Ok(());
        }
        for i in self.first_index..=last_included_index {
            self.engine.delete(&Key::Entry(i).encode())?;
        }
        self.engine.set(
            &Key::SnapshotMeta.encode(),
            encode_value(&(last_included_index, last_included_term)),
        )?;
        if self.fsync {
            self.engine.flush()?;
        }
        self.first_index = last_included_index + 1;
        self.snapshot_term = last_included_term;
        if self.last_index < last_included_index {
            self.last_index = last_included_index;
            self.last_term = last_included_term;
        }
        Ok(())
    }

    /// 安装快照后重置日志：丢弃全部条目，仅保留快照基座。
    pub fn reset_with_snapshot(
        &mut self,
        last_included_index: Index,
        last_included_term: Term,
    ) -> Result<()> {
        // 删除所有 entry
        let to_delete: Vec<_> = self
            .engine
            .scan_dyn((
                Bound::Included(Key::Entry(0).encode()),
                Bound::Included(Key::Entry(u64::MAX).encode()),
            ))
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        for k in to_delete {
            self.engine.delete(&k)?;
        }
        self.engine.set(
            &Key::SnapshotMeta.encode(),
            encode_value(&(last_included_index, last_included_term)),
        )?;
        self.commit_index = last_included_index;
        self.commit_term = last_included_term;
        self.engine.set(
            &Key::CommitIndex.encode(),
            encode_value(&(self.commit_index, self.commit_term)),
        )?;
        if self.fsync {
            self.engine.flush()?;
        }
        self.first_index = last_included_index + 1;
        self.snapshot_term = last_included_term;
        self.last_index = last_included_index;
        self.last_term = last_included_term;
        Ok(())
    }

    /// 控制是否对写入做 fsync。关闭可能违反 Raft 保证，见 fsync 字段注释。
    pub fn enable_fsync(&mut self, fsync: bool) {
        self.fsync = fsync
    }

    /// 返回 commit 索引与任期。
    pub fn get_commit_index(&self) -> (Index, Term) {
        (self.commit_index, self.commit_term)
    }

    /// 返回最后一条日志的索引与任期。
    pub fn get_last_index(&self) -> (Index, Term) {
        (self.last_index, self.last_term)
    }

    /// 返回当前任期（无则为 0）与投票。
    pub fn get_term_vote(&self) -> (Term, Option<NodeID>) {
        (self.term, self.vote)
    }

    /// 保存当前任期与投票（若有）。强制任期不回退，且一个任期内只投一票。
    /// append() 使用此任期；splice() 不能写入超过该任期的条目。
    pub fn set_term_vote(&mut self, term: Term, vote: Option<NodeID>) -> Result<()> {
        assert!(term > 0, "can't set term 0");
        assert!(term >= self.term, "term regression {} → {}", self.term, term);
        assert!(term > self.term || self.vote.is_none() || vote == self.vote, "can't change vote");

        if term == self.term && vote == self.vote {
            return Ok(());
        }
        self.engine.set(&Key::TermVote.encode(), encode_value(&(term, vote)))?;
        // 即使 Log::fsync = false 也总是 fsync。任期变更很少，对性能影响不大，
        // 而双重投票可能导致多领导者与脑裂，后果严重。
        self.engine.flush()?;
        self.term = term;
        self.vote = vote;
        Ok(())
    }

    /// 在当前任期向日志追加命令并刷盘，返回其索引。
    /// None 表示 noop 命令，通常在 Raft 领导者变更后使用。
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        self.append_entry(command, None)
    }

    /// 追加一条成员配置变更日志。
    pub fn append_membership(&mut self, membership: MembershipEntry) -> Result<Index> {
        self.append_entry(None, Some(membership))
    }

    /// 追加完整条目字段。
    pub fn append_entry(
        &mut self,
        command: Option<Vec<u8>>,
        membership: Option<MembershipEntry>,
    ) -> Result<Index> {
        assert!(self.term > 0, "can't append entry in term 0");
        assert!(
            command.is_none() || membership.is_none(),
            "command and membership are mutually exclusive"
        );
        let entry = Entry {
            index: self.last_index + 1,
            term: self.term,
            command,
            membership,
        };
        self.engine.set(&Key::Entry(entry.index).encode(), entry.encode())?;
        if self.fsync {
            self.engine.flush()?;
        }
        self.last_index = entry.index;
        self.last_term = entry.term;
        Ok(entry.index)
    }

    /// 从日志中扫描最新的成员配置条目（若有）。
    pub fn latest_membership(&mut self) -> Result<Option<(Index, MembershipEntry)>> {
        let mut found = None;
        for entry in self.scan(1..=self.last_index) {
            let entry = entry?;
            if let Some(m) = entry.membership {
                found = Some((entry.index, m));
            }
        }
        Ok(found)
    }

    /// 提交到给定索引（含）。该索引必须存在且不早于当前 commit 索引。
    pub fn commit(&mut self, index: Index) -> Result<Index> {
        let term = match self.get(index)? {
            Some(entry) if entry.index < self.commit_index => {
                panic!("commit index regression {} → {}", self.commit_index, entry.index);
            }
            Some(entry) if entry.index == self.commit_index => return Ok(index),
            Some(entry) => entry.term,
            None => panic!("commit index {index} does not exist"),
        };
        self.engine.set(&Key::CommitIndex.encode(), encode_value(&(index, term)))?;
        // 注意：commit 索引不必 fsync，因为条目已 fsync，且可从多数派日志恢复。
        self.commit_index = index;
        self.commit_term = term;
        Ok(index)
    }

    /// 获取指定索引的条目；不存在则返回 None。
    pub fn get(&mut self, index: Index) -> Result<Option<Entry>> {
        self.engine.get(&Key::Entry(index).encode())?.map(|v| Entry::decode(&v)).transpose()
    }

    /// 检查日志是否包含给定索引与任期的条目。
    pub fn has(&mut self, index: Index, term: Term) -> Result<bool> {
        // 快路径：与 last_index 比较。跟随者处理 append/心跳时的常见情况。
        if index == 0 || index > self.last_index {
            return Ok(false);
        }
        // 快照基座
        if index + 1 == self.first_index && term == self.snapshot_term && index > 0 {
            return Ok(true);
        }
        if index < self.first_index {
            return Ok(false);
        }
        if (index, term) == (self.last_index, self.last_term) {
            return Ok(true);
        }
        Ok(self.get(index)?.map(|e| e.term == term).unwrap_or(false))
    }

    /// 返回给定索引范围内的日志条目迭代器。
    pub fn scan(&mut self, range: impl RangeBounds<Index>) -> Iterator<'_> {
        // 规范化边界，避免 BTreeMap range start > end panic。
        let start_idx = match range.start_bound() {
            Bound::Excluded(&i) => i.saturating_add(1),
            Bound::Included(&i) => i,
            Bound::Unbounded => 0,
        };
        let end_idx_inclusive = match range.end_bound() {
            Bound::Excluded(&i) => i.saturating_sub(1),
            Bound::Included(&i) => i,
            Bound::Unbounded => Index::MAX,
        };
        if start_idx > end_idx_inclusive {
            return Iterator::new(Box::new(std::iter::empty()));
        }
        let from = Bound::Included(Key::Entry(start_idx).encode());
        let to = Bound::Included(Key::Entry(end_idx_inclusive).encode());
        Iterator::new(self.engine.scan_dyn((from, to)))
    }

    /// 返回可应用条目的迭代器：从当前 applied 索引之后到 commit 索引。
    pub fn scan_apply(&mut self, applied_index: Index) -> Iterator<'_> {
        // 注意：不断言 commit_index >= applied_index，因为本地 commit 索引不刷盘——
        // 重启丢失后可从多数派日志恢复。
        if applied_index >= self.commit_index {
            return Iterator::new(Box::new(std::iter::empty()));
        }
        self.scan(applied_index + 1..=self.commit_index)
    }

    /// 将一组条目拼接到日志并刷盘。新索引会追加。
    /// 重叠且任期相同的索引必须相等并被忽略；重叠但任期不同时，
    /// 在首个冲突处截断现有日志，再拼接新条目。
    ///
    /// 条目索引必须连续、任期相等或递增；首条索引须在 [1, last_index+1] 内，
    /// 任期不低于前一条（base）且不超过当前任期。
    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<Index> {
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(self.last_index); // 空输入为 no-op
        };

        // 检查条目形态是否合法。
        assert!(first.index > 0 && first.term > 0, "spliced entry has index or term 0",);
        assert!(
            entries.windows(2).all(|w| w[0].index + 1 == w[1].index),
            "spliced entries are not contiguous"
        );
        assert!(
            entries.windows(2).all(|w| w[0].term <= w[1].term),
            "spliced entries have term regression",
        );

        // 检查条目能否接到现有日志，且任期不回退。
        assert!(last.term <= self.term, "splice term {} beyond current {}", last.term, self.term);
        match self.get(first.index - 1)? {
            Some(base) if first.term < base.term => {
                panic!("splice term regression {} → {}", base.term, first.term)
            }
            Some(_) => {}
            None if first.index == 1 => {}
            None => panic!("first index {} must touch existing log", first.index),
        }

        // 跳过日志中已存在的条目。
        let mut entries = entries.as_slice();
        let mut scan = self.scan(first.index..=last.index);
        while let Some(entry) = scan.next().transpose()? {
            // [0] 合法，因为扫描范围与 entries 大小相同。
            assert!(entry.index == entries[0].index, "index mismatch at {entry:?}");
            if entry.term != entries[0].term {
                break;
            }
            assert!(
                entry.command == entries[0].command && entry.membership == entries[0].membership,
                "command/membership mismatch at {entry:?}"
            );
            entries = &entries[1..];
        }
        drop(scan);

        // 若全部已存在则完成。
        let Some(first) = entries.first() else {
            return Ok(self.last_index);
        };

        // 写入尚未存在的条目，并删除旧日志尾部（若有）。
        // 不能写到 commit 索引以下，那些条目必须不可变。
        assert!(first.index > self.commit_index, "spliced entries below commit index");

        for entry in entries {
            self.engine.set(&Key::Entry(entry.index).encode(), entry.encode())?;
        }
        for index in last.index + 1..=self.last_index {
            self.engine.delete(&Key::Entry(index).encode())?;
        }
        if self.fsync {
            self.engine.flush()?;
        }

        self.last_index = last.index;
        self.last_term = last.term;
        Ok(self.last_index)
    }

    /// 返回日志引擎状态。
    pub fn status(&mut self) -> Result<storage::Status> {
        self.engine.status()
    }
}

/// 日志条目迭代器。
pub struct Iterator<'a> {
    inner: Box<dyn storage::ScanIterator + 'a>,
}

impl<'a> Iterator<'a> {
    fn new(inner: Box<dyn storage::ScanIterator + 'a>) -> Self {
        Self { inner }
    }
}

impl std::iter::Iterator for Iterator<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.and_then(|(_, v)| Entry::decode(&v)))
    }
}
