use super::{Entry, Index};
use crate::error::Result;

/// 由 Raft 管理的状态机。
///
/// 写命令经 `apply` 在所有节点上复制并应用；读命令经 `read` 只在领导者执行。
pub trait State: Send {
    /// 返回状态机中最后已应用的日志索引。
    fn get_applied_index(&self) -> Index;

    /// 将一条日志条目应用到状态机，并返回客户端结果。
    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>>;

    /// 在状态机中执行读命令，并返回客户端结果。不得修改状态。
    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>>;

    /// 导出快照字节（含足以恢复的完整状态）。
    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    /// 从快照恢复，并将 applied 索引设为 `index`。
    fn restore(&mut self, _snapshot: &[u8], _index: Index) -> Result<()> {
        Ok(())
    }
}
