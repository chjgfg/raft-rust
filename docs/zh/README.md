# raft-rust 文档索引

> **文档语言：** [中文专题索引（本页）](./README.md) · [English topic index](../en/README.md) · [项目说明（中文）](../README.md) · [Project README (EN)](../../README.md)

本目录是**中文专题文档**（01–14）。独立 Raft 实现，**每节点一份 YAML + `raft-node` 进程 + TCP**，底层 BitCask 持久化。

**按文件名前缀 `01` → `14` 顺序阅读。** 每篇标题下与文末有「上一篇 / 下一篇 / 索引」导航。

写作约定：讲清**业务含义**和**数据怎么流**，避免只罗列类型名。

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
| 07 | [日志复制与冲突修复](./07-日志复制与冲突修复.md) | Append / reject / probe / splice |
| 08 | [线性一致读](./08-线性一致读.md) | read_seq 多数确认与门闩 |
| 09 | [Session 与 CLI 幂等](./09-Session与CLI幂等.md) | client_id / seq / `.raft-cli-session` |
| 10 | [成员变更与领导转移](./10-成员变更与领导转移.md) | Joint → Simple；去掉领导 step down |
| 11 | [快照与落后追赶](./11-快照与落后追赶.md) | compact、InstallSnapshot |
| 12 | [配置部署与进程运行时](./12-配置部署与进程运行时.md) | YAML、raft-node/cli 主循环 |
| 13 | [故障场景与测试对照](./13-故障场景与测试对照.md) | 分区/杀主/重启 ↔ tests |
| 14 | [FAQ 与能力边界](./14-FAQ与能力边界.md) | 常见问题与不做清单 |

```text
入门 01–02
核心路径 03–04
职责与存储 05–06
协议加深 07–11
工程 12–14
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

配置与 CLI 细节见仓库根 [README.md](../../README.md)，或 [12 配置部署](./12-配置部署与进程运行时.md)。

源码内 `//!` 注释与文档互补；**以代码为准**。
