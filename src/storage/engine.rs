// Bound 用于前缀扫描的开闭区间；RangeBounds 为 scan 入参
use std::ops::{Bound, RangeBounds};

// Status 可序列化，便于经 Status 客户端请求返回
use serde::{Deserialize, Serialize};

// 引擎方法统一返回库 Result
use crate::error::Result;

/// 为键前缀生成扫描范围。
///
/// 排他上界通过对最后一个非 `0xff` 字节加 1 得到；若前缀全是 `0xff`，
/// 则上界无界（其后不可能再有其它前缀）。
pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    // 下界：包含该前缀本身
    let start = Bound::Included(prefix.to_vec());
    // 上界：找到最后一个可进位的字节，构造「下一前缀」作为排他端
    let end = match prefix.iter().rposition(|&b| b != 0xff) {
        // 将该字节 +1，前面字节保持，形成字典序上紧邻的下一区间起点
        Some(i) => Bound::Excluded(
            // 前缀前缀字节 + 进位字节拼出排他上界键
            prefix.iter().take(i).copied().chain(std::iter::once(prefix[i] + 1)).collect(),
        // Excluded 构造结束
        ),
        // 全 0xff：无法构造更大前缀，上界开放
        None => Bound::Unbounded,
    // match 结束，得到 end
    };
    // 返回半开/开闭组合范围，供 BTreeMap 等有序扫描使用
    (start, end)
// prefix_range 函数结束
}

/// 键值存储引擎，保存任意字节串。键按字典序维护，因此支持范围扫描。
/// 例如：扫描某张表的全部行（共用键前缀），或扫描 Raft 日志尾部（给定索引之后）。
///
/// 只有在调用 [`Engine::flush()`] 之后，写入才保证落盘。
///
/// 为简单起见，同一时刻只支持单个使用者，因此所有方法（含读）都需要可变引用。
/// 由于 Raft 本身是串行执行的，这一点通常不成问题。
pub trait Engine: Send {
    /// [`Engine::scan`] 返回的迭代器类型。
    // 关联类型：具体引擎可返回零分配的专用迭代器
    type ScanIterator<'a>: ScanIterator + 'a
    // 生命周期与 Self 绑定，避免迭代器悬垂
    where
        // Sized 约束仅用于静态分发路径；dyn 时用 scan_dyn
        Self: Sized + 'a; // 在 trait 对象中省略，以保持 dyn 兼容

    /// 删除一个键；键不存在时无操作。
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// 将缓冲数据刷入磁盘。
    fn flush(&mut self) -> Result<()>;

    /// 获取键对应的值（若存在）。
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 按有序范围迭代键值对。
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    // 仅静态类型可返回关联迭代器
    where
        // 静态分发版本；trait 对象请用 scan_dyn
        Self: Sized; // 在 trait 对象中省略，以保持 dyn 兼容

    /// 与 scan 类似，但可用于 trait 对象（动态分发）。
    // 显式 Bound 元组 + Box 迭代器，满足 dyn Engine 调用
    fn scan_dyn(&mut self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator + '_>;

    /// 迭代所有以给定前缀开头的键值对。
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_>
    // 默认方法同样要求 Sized
    where
        // 默认实现走 prefix_range + scan
        Self: Sized, // 在 trait 对象中省略，以保持 dyn 兼容
    // 默认方法体：前缀 → 范围 → 扫描
    {
        // 将前缀转为有序范围再扫描
        self.scan(prefix_range(prefix))
    // scan_prefix 默认实现结束
    }

    /// 设置键的值；若已存在则覆盖。
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// 返回引擎状态信息。
    fn status(&mut self) -> Result<Status>;
// Engine trait 定义结束
}

/// 由 [`Engine::scan()`] 返回的键值对扫描迭代器。
// 双向迭代 + 每项可能 IO 失败
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

/// 所有可作为扫描迭代器的迭代器的 blanket 实现。
// 任何满足签名的双端迭代器自动成为 ScanIterator
impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIterator for I {}

/// 引擎状态信息。
// 可返回给客户端 Status 请求，也用于压缩决策
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 对外暴露的引擎度量快照
pub struct Status {
    /// 存储引擎名称。
    pub name: String,
    /// 存活键的数量。
    pub keys: u64,
    /// 存活键值对的逻辑大小。
    pub size: u64,
    /// 磁盘上全部数据大小（含垃圾）。
    pub disk_size: u64,
    /// 磁盘上存活数据大小（不含垃圾）。
    pub live_disk_size: u64,
// Status 字段定义结束
}

// 垃圾度量的便捷方法
impl Status {
    /// 磁盘上垃圾数据的大小。
    pub fn garbage_disk_size(&self) -> u64 {
        // 总盘占用减去存活部分
        self.disk_size - self.live_disk_size
    // garbage_disk_size 结束
    }

    /// 磁盘垃圾占总大小的比例。
    pub fn garbage_disk_percent(&self) -> f64 {
        // 空库避免除零
        if self.disk_size == 0 {
            // 无数据时垃圾占比记 0
            return 0.0;
        // 空库分支结束
        }
        // 百分比形式，便于日志与阈值比较
        self.garbage_disk_size() as f64 / self.disk_size as f64 * 100.0
    // garbage_disk_percent 结束
    }
// Status impl 结束
}
