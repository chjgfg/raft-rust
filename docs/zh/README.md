# raft-rust 中文文档索引

本目录是**中文专题文档**。写作约定：讲清**业务含义**和**数据怎么流**，并标明对应源码路径。

**按文件名前缀顺序阅读。** 每篇文首有导航。

---

## 按序号阅读

| 序号 | 文档 | 一句话 |
|------|------|--------|
| 01 | [架构总览](./01-架构总览.md) | 进程、角色、分层 |
| 02 | [消息与数据结构](./02-消息与数据结构.md) | Envelope / Message / Request / Entry / 帧 |
| 03 | [选举全流程](./03-选举全流程.md) | Pre-vote → 真选举 → 当选；节点怎么变 |
| 04 | [数据存取全流程](./04-数据存取全流程.md) | 从节点到主节点到日志到 Kv |
| 05 | [节点角色与职责](./05-节点角色与职责.md) | Follower / Candidate / Leader 各干什么 |
| 06 | [BitCask 与日志键空间](./06-BitCask与日志键空间.md) | 磁盘上键值长什么样 |

```text
入门 01–02
核心路径 03–04
职责对照 05
存储加深 06
```

---

## 快速上手

```bash
# 单节点
cargo run --bin raft-node -- --config config/single.yaml
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple

# 三节点：三个终端分别
cargo run --bin raft-node -- --config config/node1.yaml
cargo run --bin raft-node -- --config config/node2.yaml
cargo run --bin raft-node -- --config config/node3.yaml
```

配置与 CLI 细节见仓库根 [README.md](../../README.md)。

源码内 `//!` 注释与文档互补；**以代码为准**。
