// 内存 keydir：有序映射键 → 值位置
use std::collections::BTreeMap;
// keydir 范围扫描的迭代器类型
use std::collections::btree_map::Range;
// 底层日志文件句柄
use std::fs::File;
// 缓冲读写与定位
use std::io::{BufReader, BufWriter, Read as _, Seek as _, SeekFrom, Write as _};
// 范围边界，用于 scan
use std::ops::{Bound, RangeBounds};
// 日志文件路径
use std::path::PathBuf;
// 构建 keydir 时用 std IO Result，与库 Result 区分
use std::result::Result as StdResult;

// 跨进程排他锁，防止双开同一数据文件
use fs4::fs_std::FileExt;
// 打开/压缩/截断时的运维日志
use log::{error, info};

// Engine trait 与对外 Status
use super::{Engine, Status};
// 库错误类型
use crate::error::{Error, Result};

/// BitCask 的极简变体。BitCask 是日志结构键值引擎（如 Riak 所用）。
/// 与其他实现生成的 BitCask 库不兼容。参见：
/// <https://riak.com/assets/bitcask-intro.pdf>
///
/// BitCask 将键值对追加写入日志文件，并在内存中维护键到文件偏移的映射。
/// 所有存活键须能装入内存。删除会向日志写入墓碑值。为清理旧垃圾
///（已删除或被覆盖的键），可通过只写入存活数据的新日志来压缩，丢弃被替换的值与墓碑。
///
/// 本实现比标准 BitCask 简单得多：
///
/// * 不写多个固定大小日志文件，而是使用单个任意大小的追加日志。
///   这会增加压缩量（每次压缩须重写整文件），也可能超过文件系统文件大小限制。
///   不过本库预期数据库较小。
///
/// * 压缩期间会锁住数据库的读写。可接受：仅在节点启动时压缩，且文件预期较小。
///
/// * 不使用 hint 文件，打开时扫描日志本身构建 keydir。Hint 只省略值，
///   而本库值预期较小，hint 几乎与压缩后的日志一样大。
///
/// * 日志条目不含时间戳或校验和。
///
/// 编码后的日志条目结构：
///
/// 1. 键长度，大端 u32 [4 字节]。
/// 2. 值长度，大端 i32；墓碑为 -1 [4 字节]。
/// 3. 键原始字节 [<= 2 GB]。
/// 4. 值原始字节 [<= 2 GB]。
pub struct BitCask {
    /// 当前追加写日志文件。
    log: Log,
    /// 将键映射到 [`BitCask::log`] 中值的偏移与长度。
    keydir: KeyDir,
// 结束当前作用域
}

/// 将键映射到日志文件中值的位置。
// 使用 BTreeMap 以支持有序范围扫描（Raft 日志按键序迭代）
type KeyDir = BTreeMap<Vec<u8>, ValueLocation>;

/// 日志文件中值的位置。
// Copy 便于从 keydir 取出后按位置随机读
#[derive(Clone, Copy)]
// 定义数据结构
struct ValueLocation {
    /// 值在日志文件中的字节偏移。
    offset: u64,
    /// 值的字节长度。
    length: usize,
// 结束当前作用域
}

// 为类型实现方法
impl ValueLocation {
    // 值在文件中的结束偏移（不含），用于 EOF 校验
    fn end(&self) -> u64 {
        // offset + length
        self.offset + self.length as u64
    // 结束当前作用域
    }
// 结束当前作用域
}

// 为类型实现方法
impl BitCask {
    /// 在给定路径打开或创建 BitCask 数据库。
    pub fn new(path: PathBuf) -> Result<Self> {
        // 打开/创建追加日志
        let mut log = Log::new(path.clone())?;
        // 扫描日志重建内存索引
        let keydir = log.build_keydir()?;
        // 记录存活键数量，便于启动观测
        info!("Opened {} with {} live keys", path.display(), keydir.len());
        // 组装引擎实例
        Ok(Self { log, keydir })
    // 结束当前作用域
    }

    /// 打开 BitCask 数据库；若打开时垃圾占比与字节数超过阈值则自动压缩。
    pub fn new_maybe_compact(
        // 列表或字段续项
        path: PathBuf,
        // 垃圾占比阈值（0.0–1.0）
        garbage_min_fraction: f64,
        // 垃圾绝对字节阈值
        garbage_min_bytes: u64,
    // 业务逻辑步骤
    ) -> Result<Self> {
        // 先正常打开
        let mut engine = Self::new(path)?;

        // 读取盘占用与存活统计
        let status = engine.status()?;
        // 磁盘总大小（含垃圾）
        let total_size = status.disk_size;
        // 垃圾字节数
        let garbage_size = status.garbage_disk_size();
        // 垃圾占比
        let garbage_fraction = garbage_size as f64 / total_size as f64;
        // 同时满足：有垃圾、字节数与占比都超阈值才压缩
        if garbage_size > 0
            // 业务逻辑步骤
            && garbage_size >= garbage_min_bytes
            // 业务逻辑步骤
            && garbage_fraction >= garbage_min_fraction
        // 进入代码块
        {
            // 压缩前记录预期可回收空间
            info!(
                // 列表或字段续项
                "Compacting {} to remove {:.0}% garbage ({:.1} MB out of {:.1} MB)",
                // 列表或字段续项
                engine.log.path.display(),
                // 列表或字段续项
                garbage_fraction * 100.0,
                // 列表或字段续项
                garbage_size as f64 / 1024.0 / 1024.0,
                // 业务逻辑步骤
                total_size as f64 / 1024.0 / 1024.0
            // 结束调用参数列表
            );
            // 重写仅含存活键的新日志
            engine.compact()?;
            // 压缩后记录目标大小
            info!(
                // 列表或字段续项
                "Compacted {} to size {:.1} MB",
                // 列表或字段续项
                engine.log.path.display(),
                // 业务逻辑步骤
                (total_size - garbage_size) as f64 / 1024.0 / 1024.0
            // 结束调用参数列表
            );
        // 结束当前作用域
        }

        // 返回可能已压缩的引擎
        Ok(engine)
    // 结束当前作用域
    }
// 结束当前作用域
}

// 实现通用 Engine 接口，供 Raft Log 使用
impl Engine for BitCask {
    // 关联扫描迭代器
    type ScanIterator<'a> = ScanIterator<'a>;

    // 删除：写墓碑并从 keydir 移除
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        // 追加墓碑条目（value = None）
        self.log.write_entry(key, None)?;
        // 内存索引删除，后续 get 视为不存在
        self.keydir.remove(key);
        // 成功返回
        Ok(())
    // 结束当前作用域
    }

    // 将日志刷到稳定存储
    fn flush(&mut self) -> Result<()> {
        // 测试中不做 fsync 以加速。在此禁用而非在测试里设
        // raft::Log::fsync = false，以便断言即使 flush 是空操作也会刷盘。
        #[cfg(not(test))]
        // 生产路径：fsync 整个文件
        self.log.file.sync_all()?;
        // 成功返回
        Ok(())
    // 结束当前作用域
    }

    // 按 keydir 定位后从日志随机读值
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 无索引项则键不存在
        let Some(location) = self.keydir.get(key) else {
            // 提前返回
            return Ok(None);
        // 结束当前作用域
        };
        // 按偏移读取值并包装为 Some
        self.log.read_value(*location).map(Some)
    // 结束当前作用域
    }

    // 在 keydir 上做有序范围扫描，值懒加载
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        // 持有 keydir 范围迭代器 + 日志可变借用
        ScanIterator { inner: self.keydir.range(range), log: &mut self.log }
    // 结束当前作用域
    }

    // dyn Engine 路径：装箱静态 scan 结果
    fn scan_dyn(
        // 借用参数
        &mut self,
        // 列表或字段续项
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    // 业务逻辑步骤
    ) -> Box<dyn super::ScanIterator + '_> {

        // 复用 scan 实现
        Box::new(self.scan(range))
    // 结束当前作用域
    }

    // 设置/覆盖键：追加新值并更新 keydir
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // 写日志并拿到新值位置
        let value_location = self.log.write_entry(key, Some(&*value))?;
        // 覆盖内存索引（旧位置成为垃圾，待压缩回收）
        self.keydir.insert(key.to_vec(), value_location);
        // 成功返回
        Ok(())
    // 结束当前作用域
    }

    // 汇总逻辑大小与磁盘占用，供 Status 与压缩阈值
    fn status(&mut self) -> Result<Status> {
        // 存活键数
        let keys = self.keydir.len() as u64;
        // 逻辑大小：各存活键长 + 值长之和
        let size =
            // 链式变换结果
            self.keydir.iter().map(|(key, value_loc)| (key.len() + value_loc.length) as u64).sum();
        // 文件实际字节数（含垃圾）
        let disk_size = self.log.file.metadata()?.len();
        // 存活盘占用估算：逻辑大小 + 每键 8 字节长度前缀
        let live_disk_size = size + 8 * keys; // 计入长度前缀
        // 引擎名固定为 bitcask
        Ok(Status { name: "bitcask".to_string(), keys, size, disk_size, live_disk_size })
    // 结束当前作用域
    }
// 结束当前作用域
}

// 为类型实现方法
impl BitCask {
    /// 压缩当前日志：写出仅含存活键的新日志，并替换当前文件。
    pub fn compact(&mut self) -> Result<()> {
        // 创建临时日志文件；若已存在则截断。
        // 同目录 `.new` 扩展名，便于 rename 替换
        let new_path = self.log.path.with_extension("new");
        // 打开临时日志
        let mut new_log = Log::new(new_path)?;
        // 确保为空文件
        new_log.file.set_len(0)?;

        // 将全部存活条目写入新日志，并生成新 KeyDir。
        let mut new_keydir = KeyDir::new();
        // 遍历当前存活键
        for (key, value_loc) in &self.keydir {
            // 从旧日志读出当前值
            let value = self.log.read_value(*value_loc)?;
            // 写入新日志并记录新位置
            let value_loc = new_log.write_entry(key, Some(&value))?;
            // 填入新索引
            new_keydir.insert(key.clone(), value_loc);
        // 结束当前作用域
        }

        // 用新日志替换当前日志。
        // 原子 rename 到原路径
        std::fs::rename(&new_log.path, &self.log.path)?;
        // 更新临时 Log 的 path 字段为正式路径
        new_log.path = self.log.path.clone();

        // 切换到新日志与新 keydir
        self.log = new_log;
        // 业务逻辑步骤
        self.keydir = new_keydir;
        // 成功返回
        Ok(())
    // 结束当前作用域
    }
// 结束当前作用域
}

/// 数据库关闭时尝试刷盘。
impl Drop for BitCask {
    // 定义函数
    fn drop(&mut self) {
        // 尽力 fsync；失败只记日志，析构不能返回错误
        if let Err(error) = self.flush() {
            // 业务逻辑步骤
            error!("failed to flush file: {}", error)
        // 结束当前作用域
        }
    // 结束当前作用域
    }
// 结束当前作用域
}

/// BitCask 范围扫描迭代器。
pub struct ScanIterator<'a> {
    /// keydir 的范围迭代器。
    inner: Range<'a, Vec<u8>, ValueLocation>,
    /// 用于按位置读取值的日志引用。
    log: &'a mut Log,
// 结束当前作用域
}

// 为类型实现方法
impl ScanIterator<'_> {
    /// 真正从数据文件按位置读取值。
    fn map(&mut self, item: (&Vec<u8>, &ValueLocation)) -> <Self as Iterator>::Item {
        // 解构键与位置
        let (key, value_loc) = item;
        // 克隆键并从日志读值
        Ok((key.clone(), self.log.read_value(*value_loc)?))
    // 结束当前作用域
    }
// 结束当前作用域
}

// 正向迭代：按键升序产出
impl Iterator for ScanIterator<'_> {
    // 每项为可能失败的 (key, value)
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    /// 迭代器逐步从数据文件读取数据。
    fn next(&mut self) -> Option<Self::Item> {
        // 取下一个 keydir 项并加载值
        self.inner.next().map(|item| self.map(item))
    // 结束当前作用域
    }
// 结束当前作用域
}

// 反向迭代：支持从尾部扫描 Raft 日志等场景
impl DoubleEndedIterator for ScanIterator<'_> {
    // 定义函数
    fn next_back(&mut self) -> Option<Self::Item> {
        // 从范围高端取项并加载值
        self.inner.next_back().map(|item| self.map(item))
    // 结束当前作用域
    }
// 结束当前作用域
}

/// BitCask 追加写日志文件，包含按如下格式编码的键值条目序列：
///
/// 1. 键长度，大端 u32 [4 字节]。
/// 2. 值长度，大端 i32；墓碑为 -1 [4 字节]。
/// 3. 键原始字节 [<= 2 GB]。
/// 4. 值原始字节 [<= 2 GB]。
struct Log {
    /// 已打开的日志文件。
    file: File,
    /// 日志文件路径。
    path: PathBuf,
// 结束当前作用域
}

// 为类型实现方法
impl Log {
    /// 打开日志文件；不存在则创建。在关闭前对文件加排他锁；
    /// 若锁已被持有则报错。
    fn new(path: PathBuf) -> Result<Self> {
        // 确保父目录存在
        if let Some(dir) = path.parent() {
            // 业务逻辑步骤
            std::fs::create_dir_all(dir)?
        // 结束当前作用域
        }
        // 读写创建、不截断，以便恢复已有日志
        let file = std::fs::OpenOptions::new()
            // 方法链调用
            .read(true)
            // 方法链调用
            .write(true)
            // 方法链调用
            .create(true)
            // 方法链调用
            .truncate(false)
            // 方法链调用
            .open(&path)?;
        // 排他锁：同一数据目录只允许一个进程打开
        if !file.try_lock_exclusive()? {
            // 提前返回
            return Err(Error::IO(format!("file {path:?} is already is use")));
        // 结束当前作用域
        }
        // 返回打开的日志
        Ok(Self { file, path })
    // 结束当前作用域
    }

    /// 扫描日志文件构建 keydir。若遇到不完整条目，视为不完整写入，截断文件余下部分。
    fn build_keydir(&mut self) -> Result<KeyDir> {
        // 复用 4 字节缓冲读长度字段
        let mut len_buf = [0u8; 4];
        // 累积最终内存索引
        let mut keydir = KeyDir::new();
        // 文件总长度，作为扫描上界
        let file_len = self.file.metadata()?.len();
        // 从文件偏移 0 开始，用 BufReader 逐条读取记录。
        let mut r = BufReader::new(&mut self.file);
        // 当前条目起始偏移
        let mut offset = r.seek(SeekFrom::Start(0))?;

        // 顺序扫描直到文件尾
        while offset < file_len {
            // 读取下一条目，返回键与值位置；墓碑返回 None。
            // 闭包内用 std IO 错误，便于识别 UnexpectedEof
            let result = || -> StdResult<(Vec<u8>, Option<ValueLocation>), std::io::Error> {
                // 读取键长度：4 字节 u32。
                r.read_exact(&mut len_buf)?;
                // 大端键长度
                let key_len = u32::from_be_bytes(len_buf);

                // 读取值长度：4 字节 i32，墓碑为 -1。
                r.read_exact(&mut len_buf)?;
                // 负长度 → 墓碑；否则记录值在文件中的位置
                let value_loc = match i32::from_be_bytes(len_buf) {
                    // 值为 -1（..0）表示删除标记，索引会从 keydir 移除该键。
                    ..0 => None, // 墓碑
                    // 值从 header(8) + key 之后开始
                    len => Some(ValueLocation {
                        // 列表或字段续项
                        offset: offset + 8 + key_len as u64,
                        // 列表或字段续项
                        length: len as usize,
                    // 列表或字段续项
                    }),
                // 结束当前作用域
                };

                // 读取键。
                let mut key: Vec<u8> = vec![0; key_len as usize];
                // 业务逻辑步骤
                r.read_exact(&mut key)?;

                // 跳过值。
                if let Some(value_loc) = value_loc {
                    // 值越过 EOF 视为截断写入
                    if value_loc.end() > file_len {
                        // 提前返回
                        return Err(std::io::Error::new(
                            // 列表或字段续项
                            std::io::ErrorKind::UnexpectedEof,
                            // 列表或字段续项
                            "value extends beyond end of file",
                        // 语法续行
                        ));
                    // 结束当前作用域
                    }
                    // 索引只需知道值位置，用 seek 跳过值内容以加速扫描。
                    r.seek_relative(value_loc.length as i64)?;
                // 结束当前作用域
                }

                // 更新文件偏移。
                // 下一条起点 = 本条起点 + 头 8 + 键 + 值（墓碑无值）
                offset += 8 + key_len as u64 + value_loc.map_or(0, |v| v.length) as u64;

                // 返回本条解析结果
                Ok((key, value_loc))
            // 业务逻辑步骤
            }();

            // 用该条目更新 keydir。
            match result {
                // 正常 put：覆盖该键最新位置
                Ok((key, Some(value_loc))) => keydir.insert(key, value_loc),
                // 墓碑：删除索引
                Ok((key, None)) => keydir.remove(&key),
                // 若在文件末尾发现不完整条目，视为不完整写入并截断文件。
                // 场景：写入中断电可能导致文件末尾只写了一半数据。
                // 处理：假定末尾不完整数据无效，set_len(offset) 截到最后一条完整记录。
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // 记录截断位置
                    error!("Found incomplete entry at offset {offset}, truncating file");
                    // 截断到最后完整条目末尾
                    self.file.set_len(offset)?; // 截断文件
                    // 结束扫描
                    break;
                // 结束当前作用域
                }
                // 其它 IO 错误向上传播
                Err(err) => return Err(err.into()),
            // 结束当前作用域
            };
        // 结束当前作用域
        }

        // 返回重建的索引
        Ok(keydir)
    // 结束当前作用域
    }

    /// 从日志文件给定位置读取值。
    fn read_value(&mut self, location: ValueLocation) -> Result<Vec<u8>> {
        // 预先分配要读取的空间。
        let mut value: Vec<u8> = vec![0; location.length];
        // SeekFrom::Start(n) 表示从文件开头向后偏移 n 字节。
        // seek 将文件读取指针移到指定位置。
        self.file.seek(SeekFrom::Start(location.offset))?;
        // read_exact 从当前指针读取，直到填满缓冲区（location.length 字节）。
        self.file.read_exact(&mut value)?;
        // 返回读取的值字节
        Ok(value)
    // 结束当前作用域
    }

    /// 向日志追加键值条目；值为 None 表示墓碑。返回条目中值在日志的位置，
    /// 供 [`KeyDir`] 使用。
    fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<ValueLocation> {
        // 本条总字节：8 字节头 + 键 + 值（墓碑无值）
        let length = 8 + key.len() + value.map_or(0, |v| v.len());
        // 追加写：定位到文件末尾，即为本条起始 offset
        let offset = self.file.seek(SeekFrom::End(0))?;
        // 按条目大小分配写缓冲，减少系统调用
        let mut w = BufWriter::with_capacity(length, &mut self.file);

        // 键长度：4 字节 u32。
        w.write_all(&(key.len() as u32).to_be_bytes())?;

        // 值长度：4 字节 i32，墓碑为 -1。
        w.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;

        // 实际键与值。
        w.write_all(key)?;
        // 墓碑时 unwrap_or_default 写空切片
        w.write_all(value.unwrap_or_default())?;
        // 冲刷 BufWriter 到文件
        w.flush()?;

        // 将条目位置转换为值位置。
        // 值从 header+key 之后开始；墓碑 length=0
        Ok(ValueLocation {
            // 列表或字段续项
            offset: offset + 8 + key.len() as u64,
            // 列表或字段续项
            length: value.map_or(0, |v| v.len()),
        // 语法续行
        })
    // 结束当前作用域
    }
// 结束当前作用域
}


// 单元测试（goldenscript 套件）已剥离；见 tests/ 集成测试。
