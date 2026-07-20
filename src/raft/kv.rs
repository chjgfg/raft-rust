//! 用于演示与测试的简单字符串键值状态机。
//!
//! 命令与响应均为 bincode 编码的 `Vec<u8>`，与 [`super::State`] 的不透明字节接口一致。

use std::collections::BTreeMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::{Entry, Index, State};
use crate::error::Result;

/// Bincode 标准配置，用于命令 / 响应编码。
const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 用 bincode 编码应用层值。
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(value, BINCODE).expect("value must be serializable")
}

/// 用 bincode 解码应用层值。
pub fn decode<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)?.0)
}

/// 由 Raft 驱动的内存字符串键值存储。
#[derive(Default)]
pub struct Kv {
    applied_index: Index,
    data: BTreeMap<String, String>,
}

impl Kv {
    /// 创建一个空的键值状态机。
    pub fn new() -> Box<Self> {
        Box::new(Self::default())
    }

    /// 返回当前数据的快照（便于检查 / 测试）。
    pub fn data(&self) -> &BTreeMap<String, String> {
        &self.data
    }
}

impl State for Kv {
    fn get_applied_index(&self) -> Index {
        self.applied_index
    }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        let command = entry.command.as_deref().map(decode::<Command>).transpose()?;
        let response = match command {
            Some(Command::Put { key, value }) => {
                self.data.insert(key, value);
                encode(&Response::Put(entry.index))
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
        match decode::<Command>(&command)? {
            Command::Get { key } => Ok(encode(&Response::Get(self.data.get(&key).cloned()))),
            Command::Scan => Ok(encode(&Response::Scan(self.data.clone()))),
            c @ Command::Put { .. } => panic!("{c} submitted as read command"),
        }
    }
}

/// 键值命令。先用 [`encode`] 编码，再包装进 [`super::Request::Read`] / [`super::Request::Write`]。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// 获取给定键的值。
    Get { key: String },
    /// 存储键值对（写操作；返回已应用索引）。
    Put { key: String, value: String },
    /// 返回全部键值对。
    Scan,
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get { key } => write!(f, "get {key}"),
            Self::Put { key, value } => write!(f, "put {key}={value}"),
            Self::Scan => write!(f, "scan"),
        }
    }
}

/// [`Command`] 的响应。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// Get 的结果。
    Get(Option<String>),
    /// Put 的已应用索引。
    Put(Index),
    /// Scan 返回的全部键值对。
    Scan(BTreeMap<String, String>),
}

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
