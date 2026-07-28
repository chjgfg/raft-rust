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

// 默认开启全部 clippy lint
#![warn(clippy::all)]
// Message/Envelope 等变体较大，允许 large_enum_variant
#![allow(clippy::large_enum_variant)]
// 允许模块与类型同名风格
#![allow(clippy::module_inception)]
// 复杂通道类型签名过长时允许
#![allow(clippy::type_complexity)]

// 进程内多节点编排模块
pub mod cluster;
// YAML 节点配置模块
pub mod config;
// TCP+bincode 网络传输模块
pub mod net;
// 统一错误类型模块
pub mod error;
// Raft 协议核心模块
pub mod raft;
// 日志持久化引擎模块
pub mod storage;

// 对外重导出 Error/Result
pub use error::{Error, Result};
// 对外重导出 Raft 主 API
pub use raft::{
    // 协议类型：信封、日志、消息、节点与请求响应
    encode_session, Envelope, Entry, Index, Key, Log, Membership, MembershipEntry, Message, Node,
    // 节点 ID、选项、会话与状态机相关类型
    NodeID, Options, Request, Response, SessionState, State, Status, Term, TICK_INTERVAL,
// 当前作用域结束
};
// 对外重导出演示用 KV 状态机
pub use raft::kv;
// 对外重导出 BitCask 与 Engine
pub use storage::{BitCask, Engine};
