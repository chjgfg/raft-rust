//! 独立的 Raft 共识库。
//!
//! # 概述
//!
//! 本 crate 提供由 `step()` / `tick()` 驱动的纯 Raft 共识节点。
//! 存储与应用状态机均可插拔：
//!
//! * [`storage::Engine`] — 用于 Raft 日志的有序键值存储。
//!   内置持久化后端 [`storage::BitCask`]。
//! * [`raft::State`] — 从已提交日志顺序应用的确定性状态机。
//! * [`net`] — 可选 TCP + bincode 传输；`raft-node` / `raft-cli` 二进制基于此。
//!
//! 核心 `Node` 仍由 `step()` / `tick()` 驱动；出站消息经
//! `crossbeam::channel::Sender<raft::Envelope>` 发出。进程内部署见 `cluster` 模块，
//! 多进程部署见 `config/node*.yaml` + `raft-node`。
//!
//! # 最小用法
//!
//! ```ignore
//! use raft_rust::raft::{self, Log, Node, Options, State};
//! use raft_rust::storage::BitCask;
//! use crossbeam::channel;
//!
//! let (tx, rx) = channel::unbounded();
//! let log = Log::new(Box::new(BitCask::new("data/node".into())?))?;
//! let state: Box<dyn State> = Box::new(MyState::default());
//! let node = Node::new(1, peers, log, state, tx, Options::default())?;
//! // 通过 node.tick()? 与 node.step(envelope)? 驱动
//! ```

#![warn(clippy::all)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::module_inception)]
#![allow(clippy::type_complexity)]

pub mod cluster;
pub mod config;
pub mod net;
pub mod error;
pub mod raft;
pub mod storage;

pub use error::{Error, Result};
pub use raft::{
    encode_session, Envelope, Entry, Index, Key, Log, Membership, MembershipEntry, Message, Node,
    NodeID, Options, Request, Response, SessionState, State, Status, Term, TICK_INTERVAL,
};
pub use raft::kv;
pub use storage::{BitCask, Engine};
