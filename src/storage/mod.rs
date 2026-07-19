//! Raft 日志使用的键值存储引擎。
//!
//! 日志以有序键值对形式存放。进程内可用 [`Memory`]（底层是 `BTreeMap`，
//! 用法类似 HashMap，并支持范围扫描），也可自行实现 [`Engine`] 作为持久化后端。

pub mod engine;
pub mod memory;

pub use engine::{Engine, ScanIterator, Status};
pub use memory::Memory;
