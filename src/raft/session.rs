//! 客户端 session 去重：把 (client_id, seq) 编入日志命令，apply 时幂等。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Entry, Index, State};
use crate::error::Result;

const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// 写入日志的 session 包装命令。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionCommand {
    pub client_id: Uuid,
    pub seq: u64,
    pub payload: Vec<u8>,
}

pub fn encode_session(client_id: Uuid, seq: u64, payload: Vec<u8>) -> Vec<u8> {
    // 魔数前缀，避免与裸应用命令混淆。
    let mut out = b"SESS".to_vec();
    let body = bincode::serde::encode_to_vec(
        &SessionCommand { client_id, seq, payload },
        BINCODE,
    )
    .expect("session encode");
    out.extend_from_slice(&body);
    out
}

pub fn decode_session(bytes: &[u8]) -> Option<SessionCommand> {
    if bytes.len() < 4 || &bytes[..4] != b"SESS" {
        return None;
    }
    bincode::serde::borrow_decode_from_slice(&bytes[4..], BINCODE)
        .ok()
        .map(|(c, _)| c)
}

/// 包装任意 State，对带 session 头的写命令做去重。
pub struct SessionState {
    inner: Box<dyn State>,
    /// client_id -> (last_seq, cached response)
    sessions: HashMap<Uuid, (u64, Vec<u8>)>,
}

impl SessionState {
    pub fn new(inner: Box<dyn State>) -> Box<Self> {
        Box::new(Self { inner, sessions: HashMap::new() })
    }

    pub fn into_inner(self) -> Box<dyn State> {
        self.inner
    }
}

impl State for SessionState {
    fn get_applied_index(&self) -> Index {
        self.inner.get_applied_index()
    }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        // 成员变更 / noop
        let Some(cmd) = entry.command.as_ref() else {
            return self.inner.apply(entry);
        };
        if let Some(sess) = decode_session(cmd) {
            // 幂等规则（只缓存每个 client 最后一次成功写）：
            // - seq == last：同一请求重试 / 日志里重复提出 → 返回缓存，不二次执行
            // - seq <  last：过期序号 → noop，不返回「更新的那次」缓存（避免串结果）
            // - seq >  last：正常执行并更新缓存
            if let Some((last, cached)) = self.sessions.get(&sess.client_id).cloned() {
                if sess.seq == last {
                    let noop = Entry {
                        index: entry.index,
                        term: entry.term,
                        command: None,
                        membership: None,
                    };
                    let _ = self.inner.apply(noop)?;
                    return Ok(cached);
                }
                if sess.seq < last {
                    let noop = Entry {
                        index: entry.index,
                        term: entry.term,
                        command: None,
                        membership: None,
                    };
                    let _ = self.inner.apply(noop)?;
                    return Ok(Vec::new());
                }
            }
            let inner_entry = Entry {
                index: entry.index,
                term: entry.term,
                command: Some(sess.payload),
                membership: None,
            };
            let resp = self.inner.apply(inner_entry)?;
            self.sessions.insert(sess.client_id, (sess.seq, resp.clone()));
            return Ok(resp);
        }
        self.inner.apply(entry)
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        self.inner.read(command)
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        let inner_snap = self.inner.snapshot()?;
        let sessions: Vec<(Uuid, u64, Vec<u8>)> = self
            .sessions
            .iter()
            .map(|(id, (seq, resp))| (*id, *seq, resp.clone()))
            .collect();
        Ok(bincode::serde::encode_to_vec(&(inner_snap, sessions), BINCODE)
            .expect("session snapshot"))
    }

    fn restore(&mut self, data: &[u8], index: Index) -> Result<()> {
        let (inner_snap, sessions): (Vec<u8>, Vec<(Uuid, u64, Vec<u8>)>) =
            bincode::serde::borrow_decode_from_slice(data, BINCODE)
                .map_err(|e| crate::error::Error::InvalidData(e.to_string()))?
                .0;
        self.inner.restore(&inner_snap, index)?;
        self.sessions = sessions.into_iter().map(|(id, seq, resp)| (id, (seq, resp))).collect();
        Ok(())
    }
}
