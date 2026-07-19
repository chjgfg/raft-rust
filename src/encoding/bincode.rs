//! Bincode 用于编码值（键值存储与网络协议）。
//! 它是一种依赖内部数据结构稳定的 Rust 专用编码，对本项目足够用。参见：
//! <https://github.com/bincode-org/bincode>
//!
//! 本模块封装 [`bincode`] crate，并使用标准配置。

use std::io::{Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// 使用标准 Bincode 配置。
const CONFIG: bincode::config::Configuration = bincode::config::standard();

/// 使用 Bincode 序列化值。
pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    // 失败则 panic：说明数据结构本身有问题。
    bincode::serde::encode_to_vec(value, CONFIG).expect("value must be serializable")
}

/// 使用 Bincode 反序列化值。
pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, CONFIG)?.0)
}

/// 使用 Bincode 将值序列化到 writer。
pub fn serialize_into<W: Write, T: Serialize>(mut writer: W, value: &T) -> Result<()> {
    bincode::serde::encode_into_std_write(value, &mut writer, CONFIG)?;
    Ok(())
}

/// 使用 Bincode 从 reader 反序列化值。
pub fn deserialize_from<R: Read, T: DeserializeOwned>(mut reader: R) -> Result<T> {
    Ok(bincode::serde::decode_from_std_read(&mut reader, CONFIG)?)
}

/// 使用 Bincode 从 reader 反序列化值；若 reader 已关闭则返回 None。
pub fn maybe_deserialize_from<R: Read, T: DeserializeOwned>(mut reader: R) -> Result<Option<T>> {
    match bincode::serde::decode_from_std_read(&mut reader, CONFIG) {
        Ok(t) => Ok(Some(t)),
        Err(bincode::error::DecodeError::Io { inner, .. })
            if inner.kind() == std::io::ErrorKind::UnexpectedEof
                || inner.kind() == std::io::ErrorKind::ConnectionReset =>
        {
            Ok(None)
        }
        Err(err) => Err(Error::from(err)),
    }
}
