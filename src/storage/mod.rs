//! Raft 日志使用的键值存储引擎。
//!
//! 日志以有序键值对形式存放。持久化实现为 [`BitCask`]（日志结构引擎）。

// BitCask 日志结构实现
pub mod bitcask;
// Engine trait 与扫描辅助
pub mod engine;

// 对外导出持久化后端
pub use bitcask::BitCask;
// 对外导出引擎接口、扫描迭代器与状态结构
pub use engine::{Engine, ScanIterator, Status};
