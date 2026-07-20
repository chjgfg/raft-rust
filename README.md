# raft-rust

从 [erikgrinaker/toydb](https://github.com/erikgrinaker/toydb) 抽取的独立 **Raft 共识**库。

原版 Raft 模块偏教学，紧贴 [Raft 论文](https://raft.github.io/raft.pdf)。本 crate 把它从 toyDB 中剥离，
便于你用简单的内存存储（HashMap / BTreeMap）或自定义存储引擎嵌入自己的项目。

## 功能

| 部分 | 提供内容 |
|---|---|
| 共识 | 领导者选举、日志复制、线性一致读（多数确认） |
| 日志存储 | 可插拔 `storage::Engine`；内置 `storage::Memory`（BTreeMap） |
| 状态机 | 可插拔 `raft::State`（对任意 `Vec<u8>` 命令 apply / read） |
| 传输 | **不包含** — 需自行投递 `Envelope` 消息 |

### 未包含（与 toyDB 相同）

无成员变更、快照、日志截断、领导者租约、pre-vote，也无自动客户端重试。
适合学习与原型，不是生产级 Raft。

## 快速开始

```bash
# 运行 3 节点进程内演示（Memory 存储 + channel 传输）
cargo run --example kv_cluster
```

### 嵌入自己的代码

```rust
use std::collections::HashSet;
use crossbeam::channel;
use raft_rust::raft::{Log, Node, Options, State};
use raft_rust::storage::Memory;

// 1. Raft 日志存储（内存 BTreeMap —— 可换成自己的 Engine）
let log = Log::new(Box::new(Memory::new()))?;

// 2. 你的确定性状态机
let state: Box<dyn State> = Box::new(MyState::new());

// 3. 出站消息通道 —— 把这些 Envelope 投递给同伴 / 客户端
let (tx, rx) = channel::unbounded();

// 4. 创建节点（单节点集群会立即成为领导者）
let peers: HashSet<_> = [2, 3].into_iter().collect();
let mut node = Node::new(1, peers, log, state, tx, Options::default())?;

// 5. 驱动节点
// node = node.tick()?;           // 推进逻辑时间（约每 100ms）
// node = node.step(envelope)?;   // 处理入站消息
```

为应用实现 `raft::State`：

```rust
use raft_rust::raft::{Entry, Index, State};
use raft_rust::error::Result;

struct MyState { applied: Index /* + 你的数据 */ }

impl State for MyState {
    fn get_applied_index(&self) -> Index { self.applied }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        // entry.command 为 None 表示 Raft noop（领导者选举）
        // 否则在每个节点上确定性解码并应用
        self.applied = entry.index;
        Ok(vec![])
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        // 仅在领导者上的本地读 —— 不得修改状态
        Ok(vec![])
    }
}
```

持久化存储可实现 `storage::Engine`（BitCask 风格、RocksDB 等）。
实验用 `Memory` 即可：

```rust
use raft_rust::storage::{Engine, Memory};
let engine = Memory::new(); // BTreeMap<Vec<u8>, Vec<u8>>
```

## 目录结构

```
src/
  lib.rs            公开重导出
  error.rs          Error / Result
  raft/
    mod.rs          协议文档与常量
    node.rs         Follower / Candidate / Leader 状态机
    log.rs          基于 Engine 的复制日志（内联键编码 + bincode 值编码）
    message.rs      Envelope、Message、Request、Response
    state.rs        State trait
    kv.rs           示例用字符串 KV 状态机
  storage/
    engine.rs       Engine trait（有序 KV）
    memory.rs       内存 BTreeMap 引擎
examples/
  kv_cluster.rs     3 节点集群演示
tests/
  single_node.rs    单节点冒烟测试
```

## 驱动节点

节点内 Raft 是**同步、单线程**的：

1. 大约每隔 `TICK_INTERVAL`（100 ms）调用 `tick()` —— 心跳与选举。
2. 每条入站同伴/客户端消息调用 `step(envelope)`。
3. 从传给 `Node::new` 的 `Sender<Envelope>` 读取出站消息并投递
   （TCP、channel 等）。`ClientResponse` 回给客户端；其它发往 `envelope.to` 对应同伴。

完整路由示例见 `examples/kv_cluster.rs`。

## 许可证

Apache-2.0（与 toyDB 相同）。核心 Raft 逻辑来自 Erik Grinaker 的 toyDB；
本仓库仅将其整理为独立库。
