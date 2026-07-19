use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::{Bound, RangeBounds};

use super::{Engine, Status};
use crate::error::Result;

/// An in-memory key-value storage engine using the Rust standard library's
/// B-tree implementation. Data is not persisted. Primarily for testing.
#[derive(Default)]
pub struct Memory(BTreeMap<Vec<u8>, Vec<u8>>);

impl Memory {
    /// Creates a new Memory key-value storage engine.
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
