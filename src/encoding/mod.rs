//! 二进制数据编码。
//!
//! * keycode：用于键值存储中的键。
//! * bincode：用于键值存储中的值以及网络协议。

pub mod bincode;
pub mod keycode;

use std::cmp::{Eq, Ord};
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;
use std::io::{Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// 为键枚举自动提供 Keycode 编解码方法。这些类型用作键值存储中的键。
pub trait Key<'de>: Serialize + Deserialize<'de> {
    /// 使用 Keycode 从字节切片解码键。
    fn decode(bytes: &'de [u8]) -> Result<Self> {
        keycode::deserialize(bytes)
    }

    /// 使用 Keycode 将键编码为字节向量。
    fn encode(&self) -> Vec<u8> {
        keycode::serialize(self)
    }
}

/// 为值类型自动提供 Bincode 编解码方法。用于键值存储引擎中的值，
/// 以及网络协议消息等其它值。
pub trait Value: Serialize + DeserializeOwned {
    /// 使用 Bincode 从字节切片解码值。
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
    }

    /// 使用 Bincode 从 reader 解码值。
    fn decode_from<R: Read>(reader: R) -> Result<Self> {
        bincode::deserialize_from(reader)
    }

    /// 使用 Bincode 从 reader 解码值；若 reader 已关闭则返回 None。
    fn maybe_decode_from<R: Read>(reader: R) -> Result<Option<Self>> {
        bincode::maybe_deserialize_from(reader)
    }

    /// 使用 Bincode 将值编码为字节向量。
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self)
    }

    /// 使用 Bincode 将值写入 writer。
    fn encode_into<W: Write>(&self, writer: W) -> Result<()> {
        bincode::serialize_into(writer, self)
    }
}

/// 对包装值类型的常见容器提供 blanket 实现。
impl<V: Value> Value for Option<V> {}
impl<V: Value> Value for Result<V> {}
impl<V: Value> Value for Vec<V> {}
impl<V1: Value, V2: Value> Value for (V1, V2) {}
impl<V: Value + Eq + Hash> Value for HashSet<V> {}
impl<V: Value + Eq + Ord + Hash> Value for BTreeSet<V> {}
