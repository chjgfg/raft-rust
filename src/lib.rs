//! Standalone Raft consensus library, extracted from
//! [toydb](https://github.com/erikgrinaker/toydb).
//!
//! # Overview
//!
//! This crate provides a pure Raft consensus node driven by `step()` / `tick()`.
//! Storage and the application state machine are pluggable:
//!
//! * [`storage::Engine`] — ordered key/value store used for the Raft log.
//!   An in-memory [`storage::Memory`] backend (backed by `BTreeMap`) is included.
//! * [`raft::State`] — deterministic state machine applied from the committed log.
//!
//! Networking is **not** included. Outbound messages leave via a
//! `crossbeam::channel::Sender<raft::Envelope>`; inbound messages are fed back
//! with [`raft::Node::step`]. See `examples/kv_cluster.rs` for a multi-node
//! in-process cluster using only channels and `HashMap` storage.
//!
//! # Minimal usage
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
//! // Drive with node.tick()? and node.step(envelope)?
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
