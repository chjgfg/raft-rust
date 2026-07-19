use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

use crate::encoding::keycode;
use crate::error::Result;

/// A key/value storage engine, which stores arbitrary byte strings. Keys are
/// maintained in lexicographical order, which allows for range scans. This is
/// needed e.g. to scan all rows in a specific SQL table (where all table rows
/// have a common key prefix), or to scan the tail of the Raft log (after a
/// given log entry index).
///
/// Keys should use the Keycode order-preserving encoding, see
/// [`crate::encoding::keycode`].
///
/// Writes are only guaranteed durable after calling [`Engine::flush()`].
///
/// For simplicity, this only supports a single user at a time, so all methods
/// (including reads) take a mutable reference. This isn't that big of a deal
/// since Raft execution is serial anyway.
pub trait Engine: Send {
    /// The iterator returned by [`Engine::scan`].
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a; // omit in trait objects, for dyn compatibility

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to disk.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where
        Self: Sized; // omit in trait objects, for dyn compatibility

    /// Like scan, but can be used from trait objects (with dynamic dispatch).
    fn scan_dyn(&mut self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator + '_>;

    /// Iterates over all key/value pairs starting with the given prefix.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_>
    where
        Self: Sized, // omit in trait objects, for dyn compatibility
    {
        self.scan(keycode::prefix_range(prefix))
    }

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Returns the engine status.
    fn status(&mut self) -> Result<Status>;
}

/// A scan iterator over key/value pairs, returned by [`Engine::scan()`].
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

/// Blanket implementation for all iterators that can act as a scan iterator.
impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIterator for I {}

/// Engine status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The name of the storage engine.
    pub name: String,
    /// The number of live keys in the engine.
    pub keys: u64,
    /// The logical size of live key/value pairs.
    pub size: u64,
    /// The on-disk size of all data, live and garbage.
    pub disk_size: u64,
    /// The on-disk size of live data, excluding garbage.
    pub live_disk_size: u64,
}

impl Status {
    /// The on-disk size of garbage data.
    pub fn garbage_disk_size(&self) -> u64 {
        self.disk_size - self.live_disk_size
    }

    /// The ratio of on-disk garbage to total size.
    pub fn garbage_disk_percent(&self) -> f64 {
        if self.disk_size == 0 {
            return 0.0;
        }
        self.garbage_disk_size() as f64 / self.disk_size as f64 * 100.0
    }
}
