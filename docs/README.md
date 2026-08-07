# raft-rust

> **仅供学习使用。** 本项目是 Raft 共识与进程内/多进程 KV 演示的教学实现，**不是**生产级协调服务。

独立 **Raft** 实现（Rust）：  
**选举 → 日志复制 → 提交 → 状态机 apply**，底层 **BitCask** 持久化；部署形态为 **每节点一份 YAML + `raft-node` 进程 + TCP**。

---

## 文档导航

| 语言 | 入口 |
|------|------|
| **中文专题** | **[zh/README.md](./zh/README.md)** — 学习路径 01–14 |
| **英文专题** | **[en/README.md](./en/README.md)** — 学习路径 01–14（ASCII 文件名） |
| 英文项目说明 | **[根目录 README.md](../README.md)** |

建议先读（中文）：

1. [01 架构总览](./zh/01-架构总览.md)  
2. [04 数据存取全流程](./zh/04-数据存取全流程.md)  
3. 再按 [中文专题索引](./zh/README.md) 进入选举、复制、快照等章节  

---

## 这是什么项目？

`raft-rust` 是一个**体量可控、可读性强**的 Raft 实现，你可以：

- 作为 **Rust 库嵌入**（`Node::step` / `Node::tick` 驱动纯协议核心 + 可插拔存储 / 状态机）；  
- 用 **`raft-node`** 跑单节点或三节点集群；  
- 用 **`raft-cli`** 发 `put` / `get` / `scan` / `status` / `members`。  

它把经典 Raft 链路完整串了一遍：

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

**没有**生产级 TLS，**没有**分块快照流，**没有**完整运维面。全部用于学习 Raft 与本地原型。

### 适合用来做什么

- 系统学习「Raft 内部长什么样」  
- 对照文档只啃一层（只看选举、只看复制、只看快照）  
- 在单机多进程里试验选主 / put/get / 分区 / 杀主  
- 在测试里用 `cluster::Cluster` 做进程内故障注入  

### 不适合用来做什么

| 场景 | 原因 |
|------|------|
| 生产业务协调服务 | 缺 TLS / 认证、分块快照、运维工具 |
| 高可用强依赖 | 教学向实现，未做生产加固 |
| 追求完整 Raft 功能面 | 刻意简化（见 [14 FAQ](./zh/14-FAQ与能力边界.md)） |

需要生产级共识请用 etcd / TiKV 等成熟系统。  
**本仓库的目标是帮你看懂 Raft，而不是替你扛生产流量。**

---

## 能力一览（学习向功能面）

| 类别 | 支持 |
|------|------|
| **共识** | 选主、日志复制、多数提交 |
| **读** | 领导者线性一致读（多数确认） |
| **Pre-vote / CheckQuorum** | `Options` 可开关，减轻分区干扰 |
| **成员变更** | 联合共识（Joint Consensus） |
| **领导转移** | `ChangeMembership` 可去掉当前领导，Simple 提交后旧领导 step down |
| **写去重** | `WriteSession(client_id, seq)` + `SessionState` |
| **快照** | `snapshot` / `InstallSnapshot` / 日志 `compact_to` |
| **存储** | **BitCask** 日志结构引擎（`data_dir/bitcask.log`） |
| **网络** | TCP 帧：`WireMsg::{Raft, Client, ClientReply}` |
| **测试用进程内集群** | `cluster::Cluster`（channel + 故障注入） |

### 刻意限制（为了代码还能读）

- 明文 TCP，**无 TLS / 认证**
- 快照整包发送，不分块流式
- 无自动领导优雅迁移 RPC（通过成员变更去掉旧领导实现 step down）
- 无生产级运维能力（监控、动态配置热更新等）

---

## 怎么用这个项目

### 1）以阅读为主学习（推荐）

```text
docs/zh/   中文路径 01 → 14
docs/en/   英文路径 01 → 14
```

建议顺序：

1. 架构 + 消息（先建立全局）  
2. 选举与数据流（核心路径）  
3. 节点角色与存储（职责与落盘）  
4. 复制 / 成员 / 快照（协议加深）  
5. 工程与排错（部署 / 故障 / FAQ）  

### 2）单节点

```bash
# 需要 Rust 工具链（edition 2024）

cargo run --bin raft-node -- --config config/single.yaml
```

配置里 **`peers: []`**（或不写同伴），进程启动后**立刻成为 Leader**。

```bash
# 另一个终端
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 get a
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 scan
```

### 3）三节点

三个终端分别启动 `config/node1.yaml` / `node2.yaml` / `node3.yaml`，CLI 写多个 peer：

```bash
cargo run --bin raft-node -- --config config/node1.yaml
cargo run --bin raft-node -- --config config/node2.yaml
cargo run --bin raft-node -- --config config/node3.yaml

cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 status
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put b banana
```

### 4）作为库嵌入

```rust
use raft_rust::cluster::{wait_for_leader, Cluster};

let cluster = Cluster::spawn(&[1, 2, 3]); // 测试用进程内集群
let mut client = cluster.client();
wait_for_leader(&mut client)?;
client.put("k", "v")?;
```

或直接使用 `Node` + 自建传输（见 `src/lib.rs` / `src/net`）。

### 5）测试

```bash
cargo build --bins    # 真多进程测试需要 raft-node 二进制
cargo test
```

覆盖范围包括：选主、转发、分区、杀主、丢包/乱序、并发客户端、Pre-vote / CheckQuorum、成员变更、**移除领导（领导转移）**、Session 去重、BitCask、**重启恢复**、**快照截断后重启**、**真多进程**（`tests/multi_process.rs`）。

进程内演示（非部署路径）：

```bash
cargo run --example kv_cluster
```

---

## 项目结构

```text
raft-rust/
├── README.md              ← 英文项目入口
├── docs/
│   ├── README.md          ← 中文项目入口（本页）
│   ├── zh/                ← 中文设计文档 01–14
│   │   └── README.md      ← 中文专题索引
│   └── en/                ← 英文设计文档 01–14
│       └── README.md      ← 英文专题索引
├── src/
│   ├── lib.rs             # crate 根
│   ├── bin/raft_node.rs   # 节点进程
│   ├── bin/raft_cli.rs    # 命令行客户端
│   ├── net/               # TCP + bincode
│   ├── cluster/           # 进程内集群（测试 / example）
│   ├── config.rs          # 节点 YAML 加载
│   ├── raft/              # 协议：node / log / membership / session / kv
│   └── storage/           # Engine + BitCask
├── config/
│   ├── single.yaml        # 单节点
│   └── node1.yaml … node3.yaml
├── tests/                 # 单元与集成测试
└── examples/kv_cluster.rs # 进程内三节点演示
```

| 二进制 | 路径 | 用途 |
|--------|------|------|
| `raft-node` | `src/bin/raft_node.rs` | 节点进程（单节点或多节点中的一员） |
| `raft-cli` | `src/bin/raft_cli.rs` | 命令行客户端 |

---

## 学习建议

1. **边读边开终端** — 用 `raft-cli status` 看任期与提交位点，改配置再观察变化。  
2. **单节点先跑通**，关心选主与复制再用三节点。  
3. **跟着编号文档走** — 每篇都标明对应源码路径。  
4. **文档与代码不一致时以代码为准** — 这是活的学习仓库。  
5. **不要把本项目部署成生产协调服务，也不要存你丢不起的数据。**

---

## 开发

```bash
cargo build --bins
cargo test
cargo run --example kv_cluster
```

常见分支（以远程为准）：`main`、`docs`。

---

## 许可证

`Cargo.toml` 声明 `Apache-2.0`。实现紧贴 [Raft 论文](https://raft.github.io/raft.pdf)，并包含成员变更、Pre-vote、快照、多进程部署等扩展。

---

## 再次声明

本仓库是一个**学习项目**。  
用途是 **阅读、实验、教学与自学**。  
**请勿**当作生产级协调服务使用，也**请勿**假设它具备工业级可靠性或兼容性。
