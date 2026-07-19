use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::{Bound, RangeBounds};

use super::{Engine, Status};
use crate::error::Result;

/// 基于标准库 B 树的内存键值存储引擎。数据不持久化，主要用于测试与演示。
#[derive(Default)]
pub struct Memory(BTreeMap<Vec<u8>, Vec<u8>>);

impl Memory {
    /// 创建一个新的内存键值存储引擎。
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for Memory {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.0.get(key).cloned())
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        ScanIterator(self.0.range(range))
    }

    fn scan_dyn(
        &mut self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn super::ScanIterator + '_> {
        Box::new(self.scan(range))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.0.insert(key.to_vec(), value);
        Ok(())
    }

    fn status(&mut self) -> Result<Status> {
        Ok(Status {
            name: "memory".to_string(),
            keys: self.0.len() as u64,
            size: self.0.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
            disk_size: 0,
            live_disk_size: 0,
        })
    }
}

pub struct ScanIterator<'a>(Range<'a, Vec<u8>, Vec<u8>>);

impl Iterator for ScanIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}

impl DoubleEndedIterator for ScanIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}
