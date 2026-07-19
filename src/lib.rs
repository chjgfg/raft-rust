//! 独立的 Raft 共识库，抽取自
//! [toydb](https://github.com/erikgrinaker/toydb)。
//!
//! # 概述
//!
//! 本 crate 提供由 `step()` / `tick()` 驱动的纯 Raft 共识节点。
//! 存储与应用状态机均可插拔：
//!
//! * [`storage::Engine`] — 用于 Raft 日志的有序键值存储。
//!   内置内存后端 [`storage::Memory`]（基于 `BTreeMap`）。
//! * [`raft::State`] — 从已提交日志顺序应用的确定性状态机。
//!
//! **不包含**网络层。出站消息通过
//! `crossbeam::channel::Sender<raft::Envelope>` 发出；入站消息用
//! [`raft::Node::step`] 喂入。参见 `examples/kv_cluster.rs`，其中用 channel
//! 与内存存储演示了多节点进程内集群。
//!
//! # 最小用法
//!
//! ```ignore
//! use raft_rust::raft::{self, Log, Node, Options, State};
//! use raft_rust::storage::Memory;
//! use crossbeam::channel;
//!
//! let (tx, rx) = channel::unbounded();
//! let log = Log::new(Box::new(Memory::new()))?;
//! let state: Box<dyn State> = Box::new(MyState::default());
//! let node = Node::new(1, peers, log, state, tx, Options::default())?;
//! // 通过 node.tick()? 与 node.step(envelope)? 驱动
//! ```

#![warn(clippy::all)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::module_inception)]
#![allow(clippy::type_complexity)]

pub mod encoding;
pub mod error;
pub mod raft;
pub mod storage;

pub use error::{Error, Result};
pub use raft::{
    Envelope, Entry, Index, Log, Message, Node, NodeID, Options, Request, Response, State, Status,
    Term, TICK_INTERVAL,
};
pub use raft::kv;
pub use storage::{Engine, Memory};
