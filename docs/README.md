# raft-rust 文档

> **仅供学习使用。** 本项目是 Raft 共识与进程内/多进程 KV 演示的教学实现，**不是**生产级协调服务。

独立 **Raft** 实现（Rust）：  
**选举 → 日志复制 → 提交 → 状态机 apply**，底层 **BitCask** 持久化；部署形态为 **每节点一份 YAML + `raft-node` 进程 + TCP**。

---

## 文档导航

| 入口 | 说明 |
|------|------|
| **[中文专题索引](./zh/README.md)** | 按序号阅读：架构、消息、选举、存取、角色、存储 |
| [项目 README](../README.md) | 快速启动、配置字段、CLI |

建议阅读顺序：

1. [01 架构总览](./zh/01-架构总览.md)  
2. [02 消息与数据结构](./zh/02-消息与数据结构.md)  
3. [03 选举全流程](./zh/03-选举全流程.md)  
4. [04 数据存取全流程](./zh/04-数据存取全流程.md)  
5. [05 节点角色与职责](./zh/05-节点角色与职责.md)  
6. [06 BitCask 与日志键空间](./zh/06-BitCask与日志键空间.md)  

---

## 一句话数据路径

```text
raft-cli
  → TCP WireMsg::Client
  → 任意 raft-node（可能是 Follower）
  → 转发给 Leader（Message::ClientRequest）
  → Leader 写 Raft Log（BitCask 键 Entry(index)）
  → Append 复制到多数 Follower
  → 多数确认后 commit
  → State::apply → 内存 Kv（SessionState 包装）
  → ClientResponse 原路返回
```

读路径**不进日志**：只在 Leader 上做多数确认后的 `State::read`。

---

## 源码对照

| 模块 | 路径 |
|------|------|
| 节点状态机 | `src/raft/node.rs` |
| 消息 | `src/raft/message.rs` |
| 日志 | `src/raft/log.rs` |
| KV 状态机 | `src/raft/kv.rs` |
| Session | `src/raft/session.rs` |
| TCP | `src/net/` |
| 节点进程 | `src/bin/raft_node.rs` |
| CLI | `src/bin/raft_cli.rs` |
| BitCask | `src/storage/bitcask.rs` |

**文档与代码不一致时以代码为准。**
