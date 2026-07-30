//! 用于演示与测试的简单字符串键值状态机。
//!
//! 命令与响应均为 bincode 编码的 `Vec<u8>`，与 [`super::State`] 的不透明字节接口一致。

// 有序 map：扫描结果确定、便于测试
use std::{collections::BTreeMap, panic};
// 命令/响应的可读显示
use std::fmt::Display;

// 命令与响应需可编解码
use serde::{Deserialize, Serialize};

// 实现 State 所需类型
use super::{Entry, Index, State};
// 解码失败映射为库错误
use crate::error::Result;

/// Bincode 标准配置，用于命令 / 响应编码。
const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 用 bincode 编码应用层值。
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    // 应用层类型应始终可序列化；失败直接 panic
    bincode::serde::encode_to_vec(value, BINCODE).expect("value must be serializable")
// 结束当前作用域
}

/// 用 bincode 解码应用层值。
pub fn decode<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    // 取解码结果的第一项（值），忽略消耗字节数
    Ok(bincode::serde::borrow_decode_from_slice(bytes, BINCODE)?.0)
// 结束当前作用域
}

/// 由 Raft 驱动的内存字符串键值存储。
// Default：空库 applied_index=0
#[derive(Default)]
// 定义数据结构
pub struct Kv {
    // 最后成功 apply 的日志索引
    applied_index: Index,
    // 业务数据：字符串键值
    data: BTreeMap<String, String>,
// 结束当前作用域
}

// 为类型实现方法
impl Kv {
    /// 创建一个空的键值状态机。
    pub fn new() -> Box<Self> {
        // 装箱以便作为 dyn State 使用
        Box::new(Self::default())
    // 结束当前作用域
    }

    /// 返回当前数据的快照（便于检查 / 测试）。
    pub fn data(&self) -> &BTreeMap<String, String> {
        // 只读暴露内部 map
        &self.data
    // 结束当前作用域
    }
// 结束当前作用域
}

// 实现 Raft 状态机接口
impl State for Kv {
    // 汇报已应用索引
    fn get_applied_index(&self) -> Index {
        // 业务逻辑步骤
        self.applied_index
    // 结束当前作用域
    }

    // 将已提交日志应用到 KV；读类命令不应走写路径
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        // 空命令（noop/成员）→ command 为 None；有载荷则解码
        let command = entry.command.as_deref().map(decode::<Command>).transpose()?;
        // 按命令类型执行
        let response = match command {
            // Put：写入并返回已应用索引
            Some(Command::Put { key, value }) => {
                // 业务逻辑步骤
                self.data.insert(key, value);
                // 编码命令字节
                encode(&Response::Put(entry.index))
            // 结束当前作用域
            }
            // Get/Scan 误作为写提交：编程错误，直接 panic
            Some(c @ (Command::Get { .. } | Command::Scan)) => {
                // 失败或不可达路径
                panic!("{c} submitted as write command")
            // 结束当前作用域
            }
            // noop 或无命令：返回空结果
            None => Vec::new(),
        // 结束当前作用域
        };
        // 推进 applied 索引
        self.applied_index = entry.index;
        // 返回编码后的响应
        Ok(response)
    // 结束当前作用域
    }

    // 只读路径：Get/Scan
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        // 解码读命令
        match decode::<Command>(&command)? {
            // 查单键，可能不存在
            Command::Get { key } => Ok(encode(&Response::Get(self.data.get(&key).cloned()))),
            // 返回全表克隆
            Command::Scan => Ok(encode(&Response::Scan(self.data.clone()))),
            // 写命令误走读路径：panic
            c @ Command::Put { .. } => panic!("{c} submitted as read command"),
        // 结束当前作用域
        }
    // 结束当前作用域
    }

    // 导出 (applied_index, data) 作为快照
    fn snapshot(&self) -> Result<Vec<u8>> {
        // 成功返回
        Ok(encode(&(self.applied_index, &self.data)))
    // 结束当前作用域
    }

    // 从快照恢复数据与 applied 索引
    fn restore(&mut self, snapshot: &[u8], index: Index) -> Result<()> {
        // 解码快照中的索引与全表
        let (applied, data): (Index, BTreeMap<String, String>) = decode(snapshot)?;
        // 取快照索引与调用方 index 的较大者，避免回退
        self.applied_index = index.max(applied);
        // 替换内存数据
        self.data = data;
        // 成功返回
        Ok(())
    // 结束当前作用域
    }
// 结束当前作用域
}

/// 键值命令。先用 [`encode`] 编码，再包装进 [`super::Request::Read`] / [`super::Request::Write`]。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义枚举
pub enum Command {
    /// 获取给定键的值。
    Get { key: String },
    /// 存储键值对（写操作；返回已应用索引）。
    Put { key: String, value: String },
    /// 返回全部键值对。
    Scan,
// 结束当前作用域
}

// 便于日志与 panic 信息中打印命令
impl Display for Command {
    // 定义函数
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 人类可读的命令摘要
        match self {
            // 列表或字段续项
            Self::Get { key } => write!(f, "get {key}"),
            // 列表或字段续项
            Self::Put { key, value } => write!(f, "put {key}={value}"),
            // 列表或字段续项
            Self::Scan => write!(f, "scan"),
        // 结束当前作用域
        }
    // 结束当前作用域
    }
// 结束当前作用域
}

/// [`Command`] 的响应。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 定义枚举
pub enum Response {
    /// Get 的结果。
    Get(Option<String>),
    /// Put 的已应用索引。
    Put(Index),
    /// Scan 返回的全部键值对。
    Scan(BTreeMap<String, String>),
// 结束当前作用域
}

// 便于 CLI 打印响应
impl Display for Response {
    // 定义函数
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 按响应类型格式化输出
        match self {
            // 有值直接打印
            Self::Get(Some(value)) => write!(f, "{value}"),
            // 缺失键
            Self::Get(None) => write!(f, "None"),
            // 写成功返回 applied index
            Self::Put(applied_index) => write!(f, "{applied_index}"),
            // Scan：k=v 逗号分隔
            Self::Scan(kvs) => {
                // 控制首项不加逗号
                let mut first = true;
                // 按 BTreeMap 序输出
                for (k, v) in kvs {
                    // 非首项加分隔符
                    if !first {
                        // 业务逻辑步骤
                        write!(f, ",")?;
                    // 结束当前作用域
                    }
                    // 写出一对
                    write!(f, "{k}={v}")?;
                    // 后续项需分隔
                    first = false;
                // 结束当前作用域
                }
                // 成功返回
                Ok(())
            // 结束当前作用域
            }
        // 结束当前作用域
        }
    // 结束当前作用域
    }
// 结束当前作用域
}
