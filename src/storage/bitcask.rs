use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read as _, Seek as _, SeekFrom, Write as _};
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::result::Result as StdResult;

use fs4::fs_std::FileExt;
use log::{error, info};

use super::{Engine, Status};
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
}

/// 将键映射到日志文件中值的位置。
type KeyDir = BTreeMap<Vec<u8>, ValueLocation>;

/// 日志文件中值的位置。
#[derive(Clone, Copy)]
struct ValueLocation {
    /// 值在日志文件中的字节偏移。
    offset: u64,
    /// 值的字节长度。
    length: usize,
}

impl ValueLocation {
    fn end(&self) -> u64 {
        self.offset + self.length as u64
    }
}

impl BitCask {
    /// 在给定路径打开或创建 BitCask 数据库。
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path.clone())?;
        let keydir = log.build_keydir()?;
        info!("Opened {} with {} live keys", path.display(), keydir.len());
        Ok(Self { log, keydir })
    }

    /// 打开 BitCask 数据库；若打开时垃圾占比与字节数超过阈值则自动压缩。
    pub fn new_maybe_compact(
        path: PathBuf,
        garbage_min_fraction: f64,
        garbage_min_bytes: u64,
    ) -> Result<Self> {
        let mut engine = Self::new(path)?;

        let status = engine.status()?;
        let total_size = status.disk_size;
        let garbage_size = status.garbage_disk_size();
        let garbage_fraction = garbage_size as f64 / total_size as f64;
        if garbage_size > 0
            && garbage_size >= garbage_min_bytes
            && garbage_fraction >= garbage_min_fraction
        {
            info!(
                "Compacting {} to remove {:.0}% garbage ({:.1} MB out of {:.1} MB)",
                engine.log.path.display(),
                garbage_fraction * 100.0,
                garbage_size as f64 / 1024.0 / 1024.0,
                total_size as f64 / 1024.0 / 1024.0
            );
            engine.compact()?;
            info!(
                "Compacted {} to size {:.1} MB",
                engine.log.path.display(),
                (total_size - garbage_size) as f64 / 1024.0 / 1024.0
            );
        }

        Ok(engine)
    }
}

impl Engine for BitCask {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // 测试中不做 fsync 以加速。在此禁用而非在测试里设
        // raft::Log::fsync = false，以便断言即使 flush 是空操作也会刷盘。
        #[cfg(not(test))]
        self.log.file.sync_all()?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(location) = self.keydir.get(key) else {
            return Ok(None);
        };
        self.log.read_value(*location).map(Some)
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        ScanIterator { inner: self.keydir.range(range), log: &mut self.log }
    }

    fn scan_dyn(
        &mut self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn super::ScanIterator + '_> {

        Box::new(self.scan(range))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let value_location = self.log.write_entry(key, Some(&*value))?;
        self.keydir.insert(key.to_vec(), value_location);
        Ok(())
    }

    fn status(&mut self) -> Result<Status> {
        let keys = self.keydir.len() as u64;
        let size =
            self.keydir.iter().map(|(key, value_loc)| (key.len() + value_loc.length) as u64).sum();
        let disk_size = self.log.file.metadata()?.len();
        let live_disk_size = size + 8 * keys; // 计入长度前缀
        Ok(Status { name: "bitcask".to_string(), keys, size, disk_size, live_disk_size })
    }
}

impl BitCask {
    /// 压缩当前日志：写出仅含存活键的新日志，并替换当前文件。
    pub fn compact(&mut self) -> Result<()> {
        // 创建临时日志文件；若已存在则截断。
        let new_path = self.log.path.with_extension("new");
        let mut new_log = Log::new(new_path)?;
        new_log.file.set_len(0)?;

        // 将全部存活条目写入新日志，并生成新 KeyDir。
        let mut new_keydir = KeyDir::new();
        for (key, value_loc) in &self.keydir {
            let value = self.log.read_value(*value_loc)?;
            let value_loc = new_log.write_entry(key, Some(&value))?;
            new_keydir.insert(key.clone(), value_loc);
        }

        // 用新日志替换当前日志。
        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.keydir = new_keydir;
        Ok(())
    }
}

/// 数据库关闭时尝试刷盘。
impl Drop for BitCask {
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            error!("failed to flush file: {}", error)
        }
    }
}

/// BitCask 范围扫描迭代器。
pub struct ScanIterator<'a> {
    /// keydir 的范围迭代器。
    inner: Range<'a, Vec<u8>, ValueLocation>,
    /// 用于按位置读取值的日志引用。
    log: &'a mut Log,
}

impl ScanIterator<'_> {
    /// 真正从数据文件按位置读取值。
    fn map(&mut self, item: (&Vec<u8>, &ValueLocation)) -> <Self as Iterator>::Item {
        let (key, value_loc) = item;
        Ok((key.clone(), self.log.read_value(*value_loc)?))
    }
}

impl Iterator for ScanIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    /// 迭代器逐步从数据文件读取数据。
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl DoubleEndedIterator for ScanIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
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
}

impl Log {
    /// 打开日志文件；不存在则创建。在关闭前对文件加排他锁；
    /// 若锁已被持有则报错。
    fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if !file.try_lock_exclusive()? {
            return Err(Error::IO(format!("file {path:?} is already is use")));
        }
        Ok(Self { file, path })
    }

    /// 扫描日志文件构建 keydir。若遇到不完整条目，视为不完整写入，截断文件余下部分。
    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut len_buf = [0u8; 4];
        let mut keydir = KeyDir::new();
        let file_len = self.file.metadata()?.len();
        // 从文件偏移 0 开始，用 BufReader 逐条读取记录。
        let mut r = BufReader::new(&mut self.file);
        let mut offset = r.seek(SeekFrom::Start(0))?;

        while offset < file_len {
            // 读取下一条目，返回键与值位置；墓碑返回 None。
            let result = || -> StdResult<(Vec<u8>, Option<ValueLocation>), std::io::Error> {
                // 读取键长度：4 字节 u32。
                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);

                // 读取值长度：4 字节 i32，墓碑为 -1。
                r.read_exact(&mut len_buf)?;
                let value_loc = match i32::from_be_bytes(len_buf) {
                    // 值为 -1（..0）表示删除标记，索引会从 keydir 移除该键。
                    ..0 => None, // 墓碑
                    len => Some(ValueLocation {
                        offset: offset + 8 + key_len as u64,
                        length: len as usize,
                    }),
                };

                // 读取键。
                let mut key: Vec<u8> = vec![0; key_len as usize];
                r.read_exact(&mut key)?;

                // 跳过值。
                if let Some(value_loc) = value_loc {
                    if value_loc.end() > file_len {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "value extends beyond end of file",
                        ));
                    }
                    // 索引只需知道值位置，用 seek 跳过值内容以加速扫描。
                    r.seek_relative(value_loc.length as i64)?;
                }

                // 更新文件偏移。
                offset += 8 + key_len as u64 + value_loc.map_or(0, |v| v.length) as u64;

                Ok((key, value_loc))
            }();

            // 用该条目更新 keydir。
            match result {
                Ok((key, Some(value_loc))) => keydir.insert(key, value_loc),
                Ok((key, None)) => keydir.remove(&key),
                // 若在文件末尾发现不完整条目，视为不完整写入并截断文件。
                // 场景：写入中断电可能导致文件末尾只写了一半数据。
                // 处理：假定末尾不完整数据无效，set_len(offset) 截到最后一条完整记录。
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    error!("Found incomplete entry at offset {offset}, truncating file");
                    self.file.set_len(offset)?; // 截断文件
                    break;
                }
                Err(err) => return Err(err.into()),
            };
        }

        Ok(keydir)
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
        Ok(value)
    }

    /// 向日志追加键值条目；值为 None 表示墓碑。返回条目中值在日志的位置，
    /// 供 [`KeyDir`] 使用。
    fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<ValueLocation> {
        let length = 8 + key.len() + value.map_or(0, |v| v.len());
        let offset = self.file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(length, &mut self.file);

        // 键长度：4 字节 u32。
        w.write_all(&(key.len() as u32).to_be_bytes())?;

        // 值长度：4 字节 i32，墓碑为 -1。
        w.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;

        // 实际键与值。
        w.write_all(key)?;
        w.write_all(value.unwrap_or_default())?;
        w.flush()?;

        // 将条目位置转换为值位置。
        Ok(ValueLocation {
            offset: offset + 8 + key.len() as u64,
            length: value.map_or(0, |v| v.len()),
        })
    }
}


// 单元测试（goldenscript 套件）已剥离；见 tests/ 集成测试。
