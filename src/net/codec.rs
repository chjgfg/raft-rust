//! 长度前缀帧：`u32 LE length + bincode(WireMsg)`。

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::raft::{Envelope, Request, Response};

const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 线路消息：Raft 协议 或 客户端请求/响应。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMsg {
    Raft(Envelope),
    Client { id: Uuid, request: Request },
    ClientReply { id: Uuid, response: std::result::Result<Response, Error> },
}

pub fn encode(msg: &WireMsg) -> Result<Vec<u8>> {
    let body = bincode::serde::encode_to_vec(msg, BINCODE)
        .map_err(|e| Error::InvalidData(e.to_string()))?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<WireMsg> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)
        .map_err(|e| Error::InvalidData(e.to_string()))?
        .0)
}

pub fn write_msg(w: &mut impl Write, msg: &WireMsg) -> Result<()> {
    let frame = encode(msg)?;
    w.write_all(&frame).map_err(|e| Error::IO(e.to_string()))?;
    w.flush().map_err(|e| Error::IO(e.to_string()))?;
    Ok(())
}

pub fn read_msg(r: &mut impl Read) -> Result<WireMsg> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).map_err(|e| Error::IO(e.to_string()))?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(Error::InvalidData(format!("frame too large: {len}")));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).map_err(|e| Error::IO(e.to_string()))?;
    decode(&body)
}
