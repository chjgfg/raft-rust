//! Key/value storage engines used by the Raft log.
//!
//! The log is stored as ordered key/value pairs. Use [`Memory`] for in-process
//! HashMap-style storage (actually a `BTreeMap` so range scans work), or
//! implement [`Engine`] for your own durable backend.

pub mod engine;
pub mod memory;

pub use engine::{Engine, ScanIterator, Status};
pub use memory::Memory;
