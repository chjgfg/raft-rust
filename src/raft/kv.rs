//! A simple string key/value state machine for demos and tests.
//!
//! Commands and responses are bincode-encoded `Vec<u8>`, matching the
//! opaque byte interface of [`super::State`].

use std::collections::BTreeMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::{Entry, Index, State};
use crate::encoding::{self, Value as _};
use crate::error::Result;

/// In-memory string key/value store driven by Raft.
#[derive(Default)]
pub struct Kv {
    applied_index: Index,
    data: BTreeMap<String, String>,
}

impl Kv {
    /// Creates an empty key/value state machine.
    pub fn new() -> Box<Self> {
        Box::new(Self::default())
    }

    /// Returns a snapshot of the current data (for inspection / tests).
    pub fn data(&self) -> &BTreeMap<String, String> {
        &self.data
    }
}

impl State for Kv {
    fn get_applied_index(&self) -> Index {
        self.applied_index
    }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        let command = entry.command.as_deref().map(Command::decode).transpose()?;
        let response = match command {
            Some(Command::Put { key, value }) => {
                self.data.insert(key, value);
                Response::Put(entry.index).encode()
            }
            Some(c @ (Command::Get { .. } | Command::Scan)) => {
                panic!("{c} submitted as write command")
            }
            None => Vec::new(),
        };
        self.applied_index = entry.index;
        Ok(response)
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match Command::decode(&command)? {
            Command::Get { key } => Ok(Response::Get(self.data.get(&key).cloned()).encode()),
            Command::Scan => Ok(Response::Scan(self.data.clone()).encode()),
            c @ Command::Put { .. } => panic!("{c} submitted as read command"),
        }
    }
}

/// A key/value command. Encode with [`encoding::Value::encode`] before
/// wrapping in [`super::Request::Read`] / [`super::Request::Write`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// Fetch the value of the given key.
    Get { key: String },
    /// Store a key/value pair (write; returns the applied index).
    Put { key: String, value: String },
    /// Return all key/value pairs.
    Scan,
}

impl encoding::Value for Command {}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get { key } => write!(f, "get {key}"),
            Self::Put { key, value } => write!(f, "put {key}={value}"),
            Self::Scan => write!(f, "scan"),
        }
    }
}

/// A [`Command`] response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// Result of Get.
    Get(Option<String>),
    /// Applied index of a Put.
    Put(Index),
    /// All pairs from Scan.
    Scan(BTreeMap<String, String>),
}

impl encoding::Value for Response {}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get(Some(value)) => write!(f, "{value}"),
            Self::Get(None) => write!(f, "None"),
            Self::Put(applied_index) => write!(f, "{applied_index}"),
            Self::Scan(kvs) => {
                let mut first = true;
                for (k, v) in kvs {
                    if !first {
                        write!(f, ",")?;
                    }
                    write!(f, "{k}={v}")?;
                    first = false;
                }
                Ok(())
            }
        }
    }
}
