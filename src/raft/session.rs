//! 客户端 session 去重：把 (client_id, seq) 编入日志命令，apply 时幂等。

// 每个 client 缓存最后一次成功写的 seq 与响应
use std::collections::HashMap;

// SessionCommand 写入日志需可序列化
use serde::{Deserialize, Serialize};
// 客户端会话标识
use uuid::Uuid;

// 包装底层 State
use super::{Entry, Index, State};
// apply/read 错误类型
use crate::error::Result;

// session 编解码与快照使用统一 bincode 配置
const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 写入日志的 session 包装命令。
#[derive(Clone, Debug, Serialize, Deserialize)]
// 定义数据结构
pub struct SessionCommand {
    // 客户端会话 ID
    pub client_id: Uuid,
    // 会话内单调序号
    pub seq: u64,
    // 真正交给内层状态机的命令
    pub payload: Vec<u8>,
// 结束当前作用域
}

// 将 session 元数据 + 应用命令编码为日志命令字节
pub fn encode_session(client_id: Uuid, seq: u64, payload: Vec<u8>) -> Vec<u8> {
    // 魔数前缀，避免与裸应用命令混淆。
    let mut out = b"SESS".to_vec();
    // 序列化 SessionCommand 体
    let body = bincode::serde::encode_to_vec(
        // 借用参数
        &SessionCommand { client_id, seq, payload },
        // 列表或字段续项
        BINCODE,
    // 结束调用参数列表
    )
    // 失败则说明原因
    .expect("session encode");
    // 拼在魔数之后
    out.extend_from_slice(&body);
    // 返回完整日志命令
    out
// 结束当前作用域
}

// 尝试从日志命令字节解析 session 包装；非 session 返回 None
pub fn decode_session(bytes: &[u8]) -> Option<SessionCommand> {
    // 长度不足或魔数不匹配 → 非 session 命令
    if bytes.len() < 4 || &bytes[..4] != b"SESS" {
        // 提前返回
        return None;
    // 结束当前作用域
    }
    // 解码 body；失败视为非 session（或损坏，上层当普通命令处理可能再失败）
    bincode::serde::borrow_decode_from_slice(&bytes[4..], BINCODE)
        // 方法链调用
        .ok()
        // 链式变换结果
        .map(|(c, _)| c)
// 结束当前作用域
}

/// 包装任意 State，对带 session 头的写命令做去重。
pub struct SessionState {
    // 被包装的真实应用状态机
    inner: Box<dyn State>,
    /// client_id -> (last_seq, cached response)
    sessions: HashMap<Uuid, (u64, Vec<u8>)>,
// 结束当前作用域
}

// 为类型实现方法
impl SessionState {
    // 用内层状态机构造带 session 去重的包装
    pub fn new(inner: Box<dyn State>) -> Box<Self> {
        // 初始无会话缓存
        Box::new(Self { inner, sessions: HashMap::new() })
    // 结束当前作用域
    }

    // 拆出内层状态机（测试或迁移用）
    pub fn into_inner(self) -> Box<dyn State> {
        // 业务逻辑步骤
        self.inner
    // 结束当前作用域
    }
// 结束当前作用域
}

// 实现 State：写路径做幂等，读/快照委托并附加 session 表
impl State for SessionState {
    // 委托内层 applied 索引
    fn get_applied_index(&self) -> Index {
        // 业务逻辑步骤
        self.inner.get_applied_index()
    // 结束当前作用域
    }

    // apply：识别 session 头并按 seq 幂等
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        // 成员变更 / noop
        // 无 command：直接交给内层（noop 推进 applied）
        let Some(cmd) = entry.command.as_ref() else {
            // 提前返回
            return self.inner.apply(entry);
        // 结束当前作用域
        };
        // 若是 session 包装命令
        if let Some(sess) = decode_session(cmd) {
            // 幂等规则（只缓存每个 client 最后一次成功写）：
            // - seq == last：同一请求重试 / 日志里重复提出 → 返回缓存，不二次执行
            // - seq <  last：过期序号 → noop，不返回「更新的那次」缓存（避免串结果）
            // - seq >  last：正常执行并更新缓存
            if let Some((last, cached)) = self.sessions.get(&sess.client_id).cloned() {
                // 重试同一 seq：推进 applied 但不改业务状态
                if sess.seq == last {
                    // 构造 noop 条目推进内层 applied_index
                    let noop = Entry {
                        // 日志条目字段
                        index: entry.index,
                        // 携带当前任期
                        term: entry.term,
                        // 日志条目字段
                        command: None,
                        // 日志条目字段
                        membership: None,
                    // 结束当前作用域
                    };
                    // 忽略 noop 返回值
                    let _ = self.inner.apply(noop)?;
                    // 返回上次缓存的成功响应
                    return Ok(cached);
                // 结束当前作用域
                }
                // 过期 seq：仍推进 applied，返回空
                if sess.seq < last {
                    // 绑定中间结果
                    let noop = Entry {
                        // 日志条目字段
                        index: entry.index,
                        // 携带当前任期
                        term: entry.term,
                        // 日志条目字段
                        command: None,
                        // 日志条目字段
                        membership: None,
                    // 结束当前作用域
                    };
                    // 绑定中间结果
                    let _ = self.inner.apply(noop)?;
                    // 不返回旧缓存，避免客户端拿到「更新的那次」结果
                    return Ok(Vec::new());
                // 结束当前作用域
                }
            // 结束当前作用域
            }
            // 新 seq：解开 payload 交给内层
            let inner_entry = Entry {
                // 日志条目字段
                index: entry.index,
                // 携带当前任期
                term: entry.term,
                // 仅应用层命令
                command: Some(sess.payload),
                // session 写不应携带 membership
                membership: None,
            // 结束当前作用域
            };
            // 真正执行业务写
            let resp = self.inner.apply(inner_entry)?;
            // 缓存 (seq, 响应) 供重试
            self.sessions.insert(sess.client_id, (sess.seq, resp.clone()));
            // 提前返回
            return Ok(resp);
        // 结束当前作用域
        }
        // 非 session 命令：原样委托
        self.inner.apply(entry)
    // 结束当前作用域
    }

    // 读不涉及 session，直接委托
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        // 业务逻辑步骤
        self.inner.read(command)
    // 结束当前作用域
    }

    // 快照 = 内层快照 + 全部 session 缓存
    fn snapshot(&self) -> Result<Vec<u8>> {
        // 先导出内层状态
        let inner_snap = self.inner.snapshot()?;
        // 将会话表展平为可序列化列表
        let sessions: Vec<(Uuid, u64, Vec<u8>)> = self
            // 业务逻辑步骤
            .sessions
            // 方法链调用
            .iter()
            // 链式变换结果
            .map(|(id, (seq, resp))| (*id, *seq, resp.clone()))
            // 方法链调用
            .collect();
        // 打包编码
        Ok(bincode::serde::encode_to_vec(&(inner_snap, sessions), BINCODE)
            // 失败则说明原因
            .expect("session snapshot"))
    // 结束当前作用域
    }

    // 从组合快照恢复内层与 session 表
    fn restore(&mut self, data: &[u8], index: Index) -> Result<()> {
        // 解码 (内层快照, sessions)
        let (inner_snap, sessions): (Vec<u8>, Vec<(Uuid, u64, Vec<u8>)>) =
            // 业务逻辑步骤
            bincode::serde::borrow_decode_from_slice(data, BINCODE)
                // 链式变换结果
                .map_err(|e| crate::error::Error::InvalidData(e.to_string()))?
                // 语法续行
                .0;
        // 恢复内层状态机
        self.inner.restore(&inner_snap, index)?;
        // 重建 session 哈希表
        self.sessions = sessions.into_iter().map(|(id, seq, resp)| (id, (seq, resp))).collect();
        // 成功返回
        Ok(())
    // 结束当前作用域
    }
// 结束当前作用域
}
