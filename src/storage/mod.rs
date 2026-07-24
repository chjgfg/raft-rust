//! Raft 日志使用的键值存储引擎。
//!
//! 日志以有序键值对形式存放。持久化实现为 [`BitCask`]（日志结构引擎）。

pub mod bitcask;
pub mod engine;

pub use bitcask::BitCask;
pub use engine::{Engine, ScanIterator, Status};
