// 区间边界类型：扫描日志时构造半开/闭区间
use std::ops::{Bound, RangeBounds};

// 序列化/反序列化：日志条目与元数据落盘依赖 bincode + serde
use serde::{Deserialize, Serialize};

// 成员配置变更条目类型（联合共识 / 单一配置）
use super::membership::MembershipEntry;
// 节点 ID 与任期号，日志条目与投票元数据会用到
use super::{NodeID, Term};
// 统一错误类型，解码/存储失败向上返回
use crate::error::Result;
// 底层键值存储引擎抽象（可插拔）
use crate::storage;

/// 日志索引（条目位置）。从 1 开始；0 表示无索引。
pub type Index = u64;

/// Bincode 标准配置，用于日志条目与元数据的序列化。
const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 用 bincode 序列化值。
fn encode_value<T: Serialize>(value: &T) -> Vec<u8> {
    // 序列化失败视为编程错误：Raft 内部结构必须可编码
    bincode::serde::encode_to_vec(value, BINCODE).expect("value must be serializable")
}

/// 用 bincode 反序列化值。
fn decode_value<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    // 只取解码结果本体，忽略 bincode 返回的已消费字节数
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)?.0)
}

/// 包含状态机命令的日志条目。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 日志条目：index/term + 命令或成员变更（互斥）
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
    // 可选成员配置；与 command 互斥，追加后立即影响投票集
    pub membership: Option<MembershipEntry>,
}

// 条目编解码：与 Key/Engine 之间的字节契约
impl Entry {
    // 将本条目编码为可持久化的字节
    fn encode(&self) -> Vec<u8> {
        // 复用统一 bincode 编码，保证与存储读写格式一致
        encode_value(self)
    }

    // 从存储字节还原日志条目
    fn decode(bytes: &[u8]) -> Result<Self> {
        // 解码失败向上传播，避免静默损坏日志
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
// 引擎键空间：条目按索引有序，元数据键排在条目之后
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

// 键空间编码：决定条目有序扫描与元数据分区
impl Key {
    /// 编码为有序字节键。
    pub fn encode(&self) -> Vec<u8> {
        // 按键种类生成固定前缀（及可选索引），决定存储字典序
        match self {
            // 日志条目键：0x00 前缀 + 大端索引，保证按索引有序扫描
            Key::Entry(index) => {
                // 预分配 1 字节前缀 + 8 字节 u64
                let mut buf = Vec::with_capacity(1 + 8);
                // 条目键前缀，保证全部 Entry 排在元数据键之前
                buf.push(0x00);
                // 大端写入索引，字典序即索引序
                buf.extend_from_slice(&index.to_be_bytes());
                // 返回完整有序键
                buf
            }
            // 任期/投票元数据键
            Key::TermVote => vec![0x01],
            // 已提交索引元数据键
            Key::CommitIndex => vec![0x02],
            // 快照元数据键（last_included_index/term）
            Key::SnapshotMeta => vec![0x03],
            // 快照数据键（状态机字节）
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

// 日志核心 API：追加/提交/拼接/压缩/快照与查询
impl Log {
    /// 使用给定存储引擎初始化日志。
    pub fn new(mut engine: Box<dyn storage::Engine>) -> Result<Self> {
        // 从磁盘加载初始内存状态。
        // 读取持久化的 (term, vote)；不存在则视为 term=0、未投票
        let (term, vote) = engine
            // 选举安全元数据键
            .get(&Key::TermVote.encode())?
            // Option<Vec<u8>> → Option<&[u8]>，便于借用解码
            .as_deref()
            // 有值则 bincode 还原 (Term, Option<NodeID>)
            .map(decode_value)
            // 把 Option<Result<_>> 展平为 Result<Option<_>>
            .transpose()?
            // 冷启动：无任期、未投票
            .unwrap_or((0, None));
        // 扫描全部条目，取最后一条作为 last_index/last_term；空日志则为 (0,0)
        let (mut last_index, mut last_term) = engine
            // 全量条目区间扫描（按 Entry 键字典序）
            .scan_dyn((
                // 条目键下界：索引 0 起（实际条目从 1 开始）
                Bound::Included(Key::Entry(0).encode()),
                // 条目键上界：最大 u64 索引
                Bound::Included(Key::Entry(u64::MAX).encode()),
            // 结束多行表达式
            ))
            // 取序最大的一条，即当前日志尾
            .last()
            // 存储错误向上冒泡
            .transpose()?
            // 解码尾条目
            .map(|(_, v)| Entry::decode(&v))
            // 解码错误向上冒泡
            .transpose()?
            // 抽出 (index, term) 作为 last 指针
            .map(|e| (e.index, e.term))
            // 空日志：尚未有任何条目
            .unwrap_or((0, 0));
        // 读取持久化的 commit 索引与任期；缺失则 (0,0)
        let (mut commit_index, mut commit_term) = engine
            // commit 元数据键
            .get(&Key::CommitIndex.encode())?
            // 借用字节切片
            .as_deref()
            // 解码 (commit_index, commit_term)
            .map(decode_value)
            // 展平 Result
            .transpose()?
            // 无 commit 记录：尚未提交任何条目
            .unwrap_or((0, 0));
        // 读取快照元数据；无快照时 (0,0)
        let (snap_index, snapshot_term) = engine
            // 快照基座元数据键
            .get(&Key::SnapshotMeta.encode())?
            // 借用字节切片
            .as_deref()
            // 解码 (last_included_index, last_included_term)
            .map(decode_value)
            // 展平 Result
            .transpose()?
            // 无快照：基座为 0
            .unwrap_or((0, 0));
        // first_index = 快照之后下一条；无快照且无日志时为 1。
        let mut first_index = if snap_index > 0 {
            // 有快照：保留日志从 last_included_index+1 开始
            snap_index + 1
        // 无快照且磁盘上也没有条目
        } else if last_index == 0 {
            // 无快照也无条目：约定从索引 1 起写
            1
        // 无快照但已有条目：从最小索引恢复 first
        } else {
            // 扫描最小 entry 索引
            engine
                // 同样扫全部 Entry 键区间
                .scan_dyn((
                    // 下界：最小可能条目键
                    Bound::Included(Key::Entry(0).encode()),
                    // 上界：最大可能条目键
                    Bound::Included(Key::Entry(u64::MAX).encode()),
                // 结束多行表达式
                ))
                // 取序最小的一条，即当前日志头
                .next()
                // 存储错误向上冒泡
                .transpose()?
                // 解码头条目
                .map(|(_, v)| Entry::decode(&v))
                // 解码错误向上冒泡
                .transpose()?
                // 头条目索引作为 first_index
                .map(|e| e.index)
                // 理论上不应为空，兜底为 1
                .unwrap_or(1)
        };

        // 快照之后若无剩余日志条目，last/commit 至少要覆盖快照基座。
        if snap_index > 0 {
            // 快照已包含到 snap_index，本地 last 不能落后于基座
            if last_index < snap_index {
                // 抬升 last 到快照覆盖的最后索引
                last_index = snap_index;
                // last 任期与快照基座任期对齐
                last_term = snapshot_term;
            }
            // 快照内容视为已提交，commit 至少推进到基座
            if commit_index < snap_index {
                // 抬升 commit 到快照覆盖点
                commit_index = snap_index;
                // commit 任期与快照基座任期对齐
                commit_term = snapshot_term;
            }
            // 校正 first_index，保证与快照元数据一致
            if first_index != snap_index + 1 {
                // 强制 first 紧挨快照之后
                first_index = snap_index + 1;
            }
        }

        let fsync = true; // 默认开启 fsync
        // 组装内存中的 Log 视图，与磁盘状态对齐
        Ok(Self {
            // 接管调用方传入的存储引擎
            engine,
            // 已恢复的当前任期
            term,
            // 已恢复的本任期投票
            vote,
            // 当前仍保留的日志起点
            first_index,
            // 快照基座任期（无快照时为 0）
            snapshot_term,
            // 日志尾索引
            last_index,
            // 日志尾任期
            last_term,
            // 已提交索引
            commit_index,
            // 已提交任期
            commit_term,
            // 是否在写后 fsync
            fsync,
        // 结束闭包/结构体表达式
        })
    }

    /// 日志中仍保留的第一条索引。
    pub fn get_first_index(&self) -> Index {
        // 供复制/快照逻辑判断本地前缀是否已被压缩
        self.first_index
    }

    /// 快照基座：`(last_included_index, last_included_term)`；无快照时 `(0,0)`。
    pub fn get_snapshot_meta(&self) -> (Index, Term) {
        // first_index<=1 表示尚未做压缩，无有效快照基座
        if self.first_index <= 1 {
            // 无快照：约定返回 (0,0)
            (0, 0)
        // 已压缩：暴露 last_included 供 InstallSnapshot/复制对齐
        } else {
            // 基座索引为 first_index-1，任期来自快照元数据
            (self.first_index - 1, self.snapshot_term)
        }
    }

    /// 截断并删除 `<= last_included_index` 的日志条目（快照后压缩）。
    pub fn compact_to(&mut self, last_included_index: Index, last_included_term: Term) -> Result<()> {
        // 安全约束：只能压缩已提交前缀，防止丢未提交数据
        assert!(last_included_index <= self.commit_index, "compact beyond commit");
        // 已压缩过或目标不推进：幂等返回
        if last_included_index + 1 <= self.first_index && last_included_index > 0 {
            // 目标基座不新于现有 first，无需再删
            return Ok(());
        }
        // 逐条删除将被快照覆盖的前缀条目
        for i in self.first_index..=last_included_index {
            // 删除单条 Entry 键，释放已快照历史
            self.engine.delete(&Key::Entry(i).encode())?;
        }
        // 持久化新的快照元数据
        self.engine.set(
            // 快照基座键
            &Key::SnapshotMeta.encode(),
            // 编码 (last_included_index, last_included_term)
            encode_value(&(last_included_index, last_included_term)),
        // 元数据写入失败则压缩中止
        )?;
        // 按配置刷盘，保证压缩结果崩溃可恢复
        if self.fsync {
            // 强制落盘，避免崩溃后仍见已删前缀
            self.engine.flush()?;
        }
        // 内存 first_index 推进到快照之后
        self.first_index = last_included_index + 1;
        // 记录基座任期，供 has()/InstallSnapshot 匹配
        self.snapshot_term = last_included_term;
        // 若本地日志本就短于快照，同步 last 指针到基座
        if self.last_index < last_included_index {
            // last 至少等于快照覆盖点
            self.last_index = last_included_index;
            // last 任期同步为基座任期
            self.last_term = last_included_term;
        }
        // 压缩完成
        Ok(())
    }

    /// 安装快照后重置日志：丢弃全部条目，仅保留快照基座。
    pub fn reset_with_snapshot(
        // 可变借用自身以改内存视图与引擎
        &mut self,
        // 快照覆盖到的最后日志索引
        last_included_index: Index,
        // 该索引对应条目的任期
        last_included_term: Term,
    // 安装失败时调用方应中止应用快照
    ) -> Result<()> {
        // 删除所有 entry
        // 先收集全部条目键，避免边扫边删
        let to_delete: Vec<_> = self
            // 访问底层引擎
            .engine
            // 扫描全部 Entry 键
            .scan_dyn((
                // 条目下界
                Bound::Included(Key::Entry(0).encode()),
                // 条目上界
                Bound::Included(Key::Entry(u64::MAX).encode()),
            // 结束多行表达式
            ))
            // 只保留键；忽略单条扫描错误以免中断清空
            .filter_map(|r| r.ok().map(|(k, _)| k))
            // 物化键列表后再删
            .collect();
        // 清空本地条目：快照已覆盖历史
        for k in to_delete {
            // 删除收集到的每一条 Entry
            self.engine.delete(&k)?;
        }
        // 写入快照元数据
        self.engine.set(
            // 快照基座键
            &Key::SnapshotMeta.encode(),
            // 编码新基座 (index, term)
            encode_value(&(last_included_index, last_included_term)),
        // 基座元数据必须先落库
        )?;
        // 快照内容视为已提交
        self.commit_index = last_included_index;
        // commit 任期对齐快照基座
        self.commit_term = last_included_term;
        // 持久化 commit，重启后与状态机对齐
        self.engine.set(
            // commit 元数据键
            &Key::CommitIndex.encode(),
            // 编码当前 (commit_index, commit_term)
            encode_value(&(self.commit_index, self.commit_term)),
        // commit 与快照基座一并持久，避免重启分叉
        )?;
        // 按配置刷盘，保证 InstallSnapshot 结果持久
        if self.fsync {
            // 强制落盘
            self.engine.flush()?;
        }
        // 内存视图重置为「仅有快照基座」
        self.first_index = last_included_index + 1;
        // 基座任期
        self.snapshot_term = last_included_term;
        // last 停在快照覆盖点（其后尚无条目）
        self.last_index = last_included_index;
        // last 任期与基座一致
        self.last_term = last_included_term;
        // 重置完成
        Ok(())
    }

    /// 控制是否对写入做 fsync。关闭可能违反 Raft 保证，见 fsync 字段注释。
    pub fn enable_fsync(&mut self, fsync: bool) {
        // 运行时开关：测试可关，生产应开
        self.fsync = fsync
    }

    /// 返回 commit 索引与任期。
    pub fn get_commit_index(&self) -> (Index, Term) {
        // 供节点状态机 apply 与心跳携带 leaderCommit
        (self.commit_index, self.commit_term)
    }

    /// 返回最后一条日志的索引与任期。
    pub fn get_last_index(&self) -> (Index, Term) {
        // 供选举 RequestVote 与复制进度比较
        (self.last_index, self.last_term)
    }

    /// 返回当前任期（无则为 0）与投票。
    pub fn get_term_vote(&self) -> (Term, Option<NodeID>) {
        // 供消息 term 校验与投票决策
        (self.term, self.vote)
    }

    /// 保存当前任期与投票（若有）。强制任期不回退，且一个任期内只投一票。
    /// append() 使用此任期；splice() 不能写入超过该任期的条目。
    pub fn set_term_vote(&mut self, term: Term, vote: Option<NodeID>) -> Result<()> {
        // term 0 非法：协议从 1 起
        assert!(term > 0, "can't set term 0");
        // 任期只能单调不减，防止时钟回拨式脑裂
        assert!(term >= self.term, "term regression {} → {}", self.term, term);
        // 同一任期内已投票则不可改投他人
        assert!(term > self.term || self.vote.is_none() || vote == self.vote, "can't change vote");

        // 幂等：无变化则跳过写盘
        if term == self.term && vote == self.vote {
            // 已是目标状态，避免无意义 fsync
            return Ok(());
        }
        // 持久化 (term, vote)，选举安全的关键
        self.engine.set(&Key::TermVote.encode(), encode_value(&(term, vote)))?;
        // 即使 Log::fsync = false 也总是 fsync。任期变更很少，对性能影响不大，
        // 而双重投票可能导致多领导者与脑裂，后果严重。
        self.engine.flush()?;
        // 更新内存视图
        self.term = term;
        // 记录本任期投票对象（可为 None 表示仅升任期）
        self.vote = vote;
        // 任期/投票已持久
        Ok(())
    }

    /// 在当前任期向日志追加命令并刷盘，返回其索引。
    /// None 表示 noop 命令，通常在 Raft 领导者变更后使用。
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        // 普通客户端/noop 写：无成员变更字段
        self.append_entry(command, None)
    }

    /// 追加一条成员配置变更日志。
    pub fn append_membership(&mut self, membership: MembershipEntry) -> Result<Index> {
        // 成员变更条目：command 为空，仅携带 membership
        self.append_entry(None, Some(membership))
    }

    /// 追加完整条目字段。
    pub fn append_entry(
        // 可变借用以推进 last 并写引擎
        &mut self,
        // 状态机命令；noop 或成员变更时为 None
        command: Option<Vec<u8>>,
        // 成员配置；普通命令时为 None
        membership: Option<MembershipEntry>,
    // 返回新条目索引；写盘失败则不推进 last
    ) -> Result<Index> {
        // 领导者必须已进入有效任期才能提出条目
        assert!(self.term > 0, "can't append entry in term 0");
        // 业务命令与成员变更互斥，避免一条日志语义歧义
        assert!(
            // 至多一种载荷
            command.is_none() || membership.is_none(),
            // 违反互斥则条目语义无法解释
            "command and membership are mutually exclusive"
        );
        // 在 last 之后连续追加，任期取当前领导者任期
        let entry = Entry {
            // 新索引紧接 last，保证连续性
            index: self.last_index + 1,
            // 领导者当前任期
            term: self.term,
            // 客户端命令或 noop
            command,
            // 可选成员变更
            membership,
        };
        // 按索引键写入引擎
        self.engine.set(&Key::Entry(entry.index).encode(), entry.encode())?;
        // Raft 要求追加持久化后再复制/应答
        if self.fsync {
            // 追加必须落盘，崩溃后不能丢未复制承诺
            self.engine.flush()?;
        }
        // 推进 last 指针，供后续 append/心跳使用
        self.last_index = entry.index;
        // 同步 last 任期
        self.last_term = entry.term;
        // 返回新条目索引给上层
        Ok(entry.index)
    }

    /// 从日志中扫描最新的成员配置条目（若有）。
    pub fn latest_membership(&mut self) -> Result<Option<(Index, MembershipEntry)>> {
        // 线性扫描找最后一条 membership（配置量少，可接受）
        let mut found = None;
        // 从 1 扫到 last，覆盖全量保留日志
        for entry in self.scan(1..=self.last_index) {
            // 单条解码/存储错误向上返回
            let entry = entry?;
            // 后出现的配置覆盖先前结果
            if let Some(m) = entry.membership {
                // 记录最新 (index, membership)
                found = Some((entry.index, m));
            }
        }
        // 无成员变更条目时为 None
        Ok(found)
    }

    /// 提交到给定索引（含）。该索引必须存在且不早于当前 commit 索引。
    pub fn commit(&mut self, index: Index) -> Result<Index> {
        // 取出目标条目任期，并校验不回退、条目存在
        let term = match self.get(index)? {
            // 禁止 commit 索引回退
            Some(entry) if entry.index < self.commit_index => {
                // 违反单调性：协议层 bug
                panic!("commit index regression {} → {}", self.commit_index, entry.index);
            }
            // 已提交到该点：幂等
            Some(entry) if entry.index == self.commit_index => return Ok(index),
            // 正常推进：记录该条目任期
            Some(entry) => entry.term,
            // 提交不存在的索引是严重错误
            None => panic!("commit index {index} does not exist"),
        };
        // 持久化 (commit_index, commit_term)
        self.engine.set(&Key::CommitIndex.encode(), encode_value(&(index, term)))?;
        // 注意：commit 索引不必 fsync，因为条目已 fsync，且可从多数派日志恢复。
        // 更新内存 commit 视图，驱动状态机 apply
        self.commit_index = index;
        // 同步 commit 任期，供快照/状态查询
        self.commit_term = term;
        // 返回新的 commit 索引
        Ok(index)
    }

    /// 获取指定索引的条目；不存在则返回 None。
    pub fn get(&mut self, index: Index) -> Result<Option<Entry>> {
        // 按 Entry 键读取并解码；缺失返回 None
        self.engine.get(&Key::Entry(index).encode())?.map(|v| Entry::decode(&v)).transpose()
    }

    /// 检查日志是否包含给定索引与任期的条目。
    pub fn has(&mut self, index: Index, term: Term) -> Result<bool> {
        // 快路径：与 last_index 比较。跟随者处理 append/心跳时的常见情况。
        // 索引 0 或超过本地 last：肯定不存在
        if index == 0 || index > self.last_index {
            // prevLogIndex 无效或本地更短
            return Ok(false);
        }
        // 快照基座
        // 恰好落在 last_included 且 term 匹配：视为存在（条目已压缩）
        if index + 1 == self.first_index && term == self.snapshot_term && index > 0 {
            // 压缩前缀上的匹配成功，用于 prevLog 校验
            return Ok(true);
        }
        // 已压缩掉且不是基座：无法匹配
        if index < self.first_index {
            // 历史已删且 term 对不上基座
            return Ok(false);
        }
        // 与 last 完全一致的快路径
        if (index, term) == (self.last_index, self.last_term) {
            // 常见：心跳 prev 正好是本地尾
            return Ok(true);
        }
        // 回落到盘读取该索引并比对任期
        Ok(self.get(index)?.map(|e| e.term == term).unwrap_or(false))
    }

    /// 返回给定索引范围内的日志条目迭代器。
    pub fn scan(&mut self, range: impl RangeBounds<Index>) -> Iterator<'_> {
        // 规范化边界，避免 BTreeMap range start > end panic。
        // 将 RangeBounds 转为闭区间 [start_idx, end_idx_inclusive]
        let start_idx = match range.start_bound() {
            // 半开下界：下一条起
            Bound::Excluded(&i) => i.saturating_add(1),
            // 闭下界：含该索引
            Bound::Included(&i) => i,
            // 无下界：从 0 起（实际条目从 1）
            Bound::Unbounded => 0,
        };
        // 规范化上界为闭区间终点
        let end_idx_inclusive = match range.end_bound() {
            // 半开上界：前一条为止
            Bound::Excluded(&i) => i.saturating_sub(1),
            // 闭上界：含该索引
            Bound::Included(&i) => i,
            // 无上界：扫到最大索引
            Bound::Unbounded => Index::MAX,
        };
        // 空区间：返回空迭代器
        if start_idx > end_idx_inclusive {
            // 调用方 range 非法或空，避免引擎 panic
            return Iterator::new(Box::new(std::iter::empty()));
        }
        // 映射为存储层有序键区间
        let from = Bound::Included(Key::Entry(start_idx).encode());
        // 上界同样编码为 Entry 键
        let to = Bound::Included(Key::Entry(end_idx_inclusive).encode());
        // 包装为 Entry 解码迭代器
        Iterator::new(self.engine.scan_dyn((from, to)))
    }

    /// 返回可应用条目的迭代器：从当前 applied 索引之后到 commit 索引。
    pub fn scan_apply(&mut self, applied_index: Index) -> Iterator<'_> {
        // 注意：不断言 commit_index >= applied_index，因为本地 commit 索引不刷盘——
        // 重启丢失后可从多数派日志恢复。
        // 状态机已追平 commit：无可应用条目
        if applied_index >= self.commit_index {
            // 空迭代，调用方无需 apply
            return Iterator::new(Box::new(std::iter::empty()));
        }
        // 扫描 (applied, commit] 区间，顺序应用到状态机
        self.scan(applied_index + 1..=self.commit_index)
    }

    /// 将一组条目拼接到日志并刷盘。新索引会追加。
    /// 重叠且任期相同的索引必须相等并被忽略；重叠但任期不同时，
    /// 在首个冲突处截断现有日志，再拼接新条目。
    ///
    /// 条目索引必须连续、任期相等或递增；首条索引须在 [1, last_index+1] 内，
    /// 任期不低于前一条（base）且不超过当前任期。
    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<Index> {
        // 空输入不改变日志
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(self.last_index); // 空输入为 no-op
        };

        // 检查条目形态是否合法。
        // 索引/任期从 1 起，0 非法
        assert!(first.index > 0 && first.term > 0, "spliced entry has index or term 0",);
        // 索引必须严格连续，无空洞
        assert!(
            // 相邻条目 index 差必须为 1
            entries.windows(2).all(|w| w[0].index + 1 == w[1].index),
            // 空洞会破坏日志匹配与 commit 推进
            "spliced entries are not contiguous"
        );
        // 批内任期单调不减
        assert!(
            // 不允许后一条 term 小于前一条
            entries.windows(2).all(|w| w[0].term <= w[1].term),
            // 批内 term 回退违反 Raft 日志属性
            "spliced entries have term regression",
        );

        // 检查条目能否接到现有日志，且任期不回退。
        // 不能写入超过本节点已知当前任期的条目
        assert!(last.term <= self.term, "splice term {} beyond current {}", last.term, self.term);
        // 与 base 条目衔接：任期不回退，且必须贴住现有日志或从 1 起
        match self.get(first.index - 1)? {
            // 前一条任期更高：违反 term 不降
            Some(base) if first.term < base.term => {
                // 协议层错误：领导者不应下发回退任期
                panic!("splice term regression {} → {}", base.term, first.term)
            }
            // base 存在且任期合法：可拼接
            Some(_) => {}
            // 从日志起点开始追加
            None if first.index == 1 => {}
            // 中间空洞：违反日志连续性
            None => panic!("first index {} must touch existing log", first.index),
        }

        // 跳过日志中已存在的条目。
        // 剩余待写入切片（跳过与本地一致的前缀）
        let mut entries = entries.as_slice();
        // 扫描重叠区间，比对 index/term/command
        let mut scan = self.scan(first.index..=last.index);
        // 逐条与本地重叠前缀比对
        while let Some(entry) = scan.next().transpose()? {
            // [0] 合法，因为扫描范围与 entries 大小相同。
            // 索引应对齐
            assert!(entry.index == entries[0].index, "index mismatch at {entry:?}");
            // 任期冲突：自此截断并重写
            if entry.term != entries[0].term {
                // 停止跳过，entries 余下部分将覆盖冲突尾
                break;
            }
            // 同 index/term 则命令与成员字段必须一致（日志匹配属性）
            assert!(
                // 相同 (index,term) 必须同 command/membership
                entry.command == entries[0].command && entry.membership == entries[0].membership,
                // 违反日志匹配属性：同 index/term 内容必须唯一
                "command/membership mismatch at {entry:?}"
            );
            // 跳过已匹配条目
            entries = &entries[1..];
        }
        // 释放扫描对 engine 的借用，后续才能写
        drop(scan);

        // 若全部已存在则完成。
        let Some(first) = entries.first() else {
            // 重叠前缀完全一致且无新条目，last 不变
            return Ok(self.last_index);
        };

        // 写入尚未存在的条目，并删除旧日志尾部（若有）。
        // 不能写到 commit 索引以下，那些条目必须不可变。
        assert!(first.index > self.commit_index, "spliced entries below commit index");

        // 写入冲突点及之后的新条目
        for entry in entries {
            // 覆盖或追加 Entry 键
            self.engine.set(&Key::Entry(entry.index).encode(), entry.encode())?;
        }
        // 删除新 last 之后的旧尾部（未提交分歧日志）
        for index in last.index + 1..=self.last_index {
            // 截断本地更长的冲突后缀
            self.engine.delete(&Key::Entry(index).encode())?;
        }
        // 刷盘保证一致性复制结果持久
        if self.fsync {
            // 冲突解决结果必须落盘
            self.engine.flush()?;
        }

        // 更新 last 指针到拼接结果末尾
        self.last_index = last.index;
        // 同步 last 任期为批末条目任期
        self.last_term = last.term;
        // 返回新的 last_index
        Ok(self.last_index)
    }

    /// 返回日志引擎状态。
    pub fn status(&mut self) -> Result<storage::Status> {
        // 透传底层存储状态（供节点 Status 响应）
        self.engine.status()
    }
}

/// 日志条目迭代器。
pub struct Iterator<'a> {
    // 底层存储扫描迭代器，按键序产出 (key, value)
    inner: Box<dyn storage::ScanIterator + 'a>,
}

// 构造期：仅包装底层 ScanIterator
impl<'a> Iterator<'a> {
    // 包装存储扫描为 Entry 迭代器
    fn new(inner: Box<dyn storage::ScanIterator + 'a>) -> Self {
        // 持有动态扫描器，生命周期绑定到 Log 引擎借用
        Self { inner }
    }
}

// 标准迭代协议：按索引序产出已解码条目
impl std::iter::Iterator for Iterator<'_> {
    // 每次产出解码后的条目或存储/解码错误
    type Item = Result<Entry>;

    // 推进底层扫描并解码为 Entry
    fn next(&mut self) -> Option<Self::Item> {
        // 忽略键，只解码 value 为 Entry
        self.inner.next().map(|r| r.and_then(|(_, v)| Entry::decode(&v)))
    }
}
