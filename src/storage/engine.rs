use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

use crate::encoding::keycode;
use crate::error::Result;

/// 键值存储引擎，保存任意字节串。键按字典序维护，因此支持范围扫描。
/// 例如：扫描某张表的全部行（共用键前缀），或扫描 Raft 日志尾部（给定索引之后）。
///
/// 键应使用保持顺序的 Keycode 编码，参见 [`crate::encoding::keycode`]。
///
/// 只有在调用 [`Engine::flush()`] 之后，写入才保证落盘。
///
/// 为简单起见，同一时刻只支持单个使用者，因此所有方法（含读）都需要可变引用。
/// 由于 Raft 本身是串行执行的，这一点通常不成问题。
pub trait Engine: Send {
    /// [`Engine::scan`] 返回的迭代器类型。
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a; // 在 trait 对象中省略，以保持 dyn 兼容

    /// 删除一个键；键不存在时无操作。
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// 将缓冲数据刷入磁盘。
    fn flush(&mut self) -> Result<()>;

    /// 获取键对应的值（若存在）。
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 按有序范围迭代键值对。
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where
        Self: Sized; // 在 trait 对象中省略，以保持 dyn 兼容

    /// 与 scan 类似，但可用于 trait 对象（动态分发）。
    fn scan_dyn(&mut self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator + '_>;

    /// 迭代所有以给定前缀开头的键值对。
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_>
    where
        Self: Sized, // 在 trait 对象中省略，以保持 dyn 兼容
    {
        self.scan(keycode::prefix_range(prefix))
    }

    /// 设置键的值；若已存在则覆盖。
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// 返回引擎状态信息。
    fn status(&mut self) -> Result<Status>;
}

/// 由 [`Engine::scan()`] 返回的键值对扫描迭代器。
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

/// 所有可作为扫描迭代器的迭代器的 blanket 实现。
impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIterator for I {}

/// 引擎状态信息。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
}

impl Status {
    /// 磁盘上垃圾数据的大小。
    pub fn garbage_disk_size(&self) -> u64 {
        self.disk_size - self.live_disk_size
    }

    /// 磁盘垃圾占总大小的比例。
    pub fn garbage_disk_percent(&self) -> f64 {
        if self.disk_size == 0 {
            return 0.0;
        }
        self.garbage_disk_size() as f64 / self.disk_size as f64 * 100.0
    }
}
