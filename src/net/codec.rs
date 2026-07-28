//! 长度前缀帧：`u32 LE length + bincode(WireMsg)`。

// 从任意 Read/Write 读写帧（TCP 流）
use std::io::{Read, Write};

// WireMsg 需跨进程序列化
use serde::{Deserialize, Serialize};
// 客户端请求用 UUID 关联请求与响应
use uuid::Uuid;

// 编解码错误映射为库错误
use crate::error::{Error, Result};
// Raft 信封与客户端请求/响应类型
use crate::raft::{Envelope, Request, Response};

// 全模块统一的 bincode 配置，保证编解码一致
const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 线路消息：Raft 协议 或 客户端请求/响应。
// 可克隆以便重试发送；可序列化以走 TCP
// 三种载荷共用同一帧格式，由接收端按变体分发
#[derive(Clone, Debug, Serialize, Deserialize)]
// 节点间与客户端共用一条 TCP 语义
pub enum WireMsg {
    // 节点间 Raft 协议消息（选举、复制、心跳等）
    Raft(Envelope),
    // 客户端写入/读取请求，id 用于匹配 ClientReply
    Client { id: Uuid, request: Request },
    // 服务端对客户端请求的回复（成功响应或 Error）
    ClientReply { id: Uuid, response: std::result::Result<Response, Error> },
// 枚举定义结束
}

// 将线路消息编码为「4 字节小端长度 + body」完整帧
pub fn encode(msg: &WireMsg) -> Result<Vec<u8>> {
    // 先用 bincode 序列化消息体
    let body = bincode::serde::encode_to_vec(msg, BINCODE)
        // 序列化失败视为协议/数据非法
        .map_err(|e| Error::InvalidData(e.to_string()))?;
    // 预分配：长度前缀 4 字节 + body
    let mut out = Vec::with_capacity(4 + body.len());
    // 写入小端 u32 长度，便于接收端一次读头再读 body
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    // 追加序列化后的消息体
    out.extend_from_slice(&body);
    // 返回完整可写帧
    Ok(out)
// encode 结束
}

// 仅解码 body（不含长度前缀），供 read_msg 在读完长度后调用
pub fn decode(bytes: &[u8]) -> Result<WireMsg> {
    // borrow_decode 返回 (值, 消耗字节数)，这里只取值
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)
        // 反序列化失败映射为 InvalidData
        .map_err(|e| Error::InvalidData(e.to_string()))?
        // 丢弃已消耗长度，只保留消息
        .0)
// decode 结束
}

// 向流写入一帧并 flush，确保对端能及时读到
pub fn write_msg(w: &mut impl Write, msg: &WireMsg) -> Result<()> {
    // 先编码为长度前缀帧
    let frame = encode(msg)?;
    // 写满整个帧
    w.write_all(&frame).map_err(|e| Error::IO(e.to_string()))?;
    // 冲刷缓冲，避免消息滞留在用户态
    w.flush().map_err(|e| Error::IO(e.to_string()))?;
    // 写路径成功完成
    Ok(())
// write_msg 结束
}

// 从流阻塞读取一帧并解码为 WireMsg
pub fn read_msg(r: &mut impl Read) -> Result<WireMsg> {
    // 先读 4 字节小端长度头
    let mut len_buf = [0u8; 4];
    // 读头不足则按 IO 错误返回（对端关闭/超时）
    r.read_exact(&mut len_buf).map_err(|e| Error::IO(e.to_string()))?;
    // 解析 body 长度
    let len = u32::from_le_bytes(len_buf) as usize;
    // 防止恶意/损坏帧占用过大内存（上限 64MB）
    if len > 64 * 1024 * 1024 {
        // 超限直接拒绝，避免 OOM
        return Err(Error::InvalidData(format!("frame too large: {len}")));
    // 长度校验分支结束
    }
    // 按长度分配并精确读取 body
    let mut body = vec![0u8; len];
    // body 读不全同样映射为 IO 错误
    r.read_exact(&mut body).map_err(|e| Error::IO(e.to_string()))?;
    // 解码 body 为业务消息
    decode(&body)
// read_msg 结束
}
