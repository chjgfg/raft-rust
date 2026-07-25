# raft-rust

独立的 **Raft 共识**实现（Rust）。

- **库**：`step()` / `tick()` 驱动的纯协议核心 + 可插拔存储 / 状态机  
- **部署**：每个节点一份 YAML、一个进程（`raft-node`），节点间 **TCP + bincode**  
- **客户端**：`raft-cli`（put / get / scan / status / members）

不是生产级产品：无 TLS、快照整包发送、教学向实现。适合学习 Raft 与本地原型。

## 深入文档

- **[docs/README.md](./docs/README.md)** — 文档入口  
- **[docs/zh/README.md](./docs/zh/README.md)** — 中文专题索引（**01–14**）  

| 想了解 | 从这读 |
|--------|--------|
| 选举 | [03](./docs/zh/03-选举全流程.md) |
| put 从从节点到存储 | [04](./docs/zh/04-数据存取全流程.md) |
| 复制冲突 / 线性读 / Session | [07](./docs/zh/07-日志复制与冲突修复.md) · [08](./docs/zh/08-线性一致读.md) · [09](./docs/zh/09-Session与CLI幂等.md) |
| 成员变更 / 快照 | [10](./docs/zh/10-成员变更与领导转移.md) · [11](./docs/zh/11-快照与落后追赶.md) |
| 部署 / 故障 / FAQ | [12](./docs/zh/12-配置部署与进程运行时.md) · [13](./docs/zh/13-故障场景与测试对照.md) · [14](./docs/zh/14-FAQ与能力边界.md) |

---

## 功能

| 能力 | 说明 |
|---|---|
| 共识 | 选主、日志复制、多数提交 |
| 读 | 领导者线性一致读（多数确认） |
| Pre-vote / CheckQuorum | `Options` 可开关，减轻分区干扰 |
| 成员变更 | 联合共识（Joint Consensus） |
| 领导转移 | `ChangeMembership` 可去掉当前领导，Simple 提交后旧领导 step down |
| 写去重 | `WriteSession(client_id, seq)` + `SessionState` |
| 快照 | `snapshot` / `InstallSnapshot` / 日志 `compact_to` |
| 存储 | **BitCask** 日志结构引擎（`data_dir/bitcask.log`） |
| 网络 | TCP 帧：`WireMsg::{Raft, Client, ClientReply}` |
| 测试用进程内集群 | `cluster::Cluster`（channel + 故障注入） |

---

## 依赖

- Rust（edition 2024）
- 构建：`cargo build --bins`

---

## 单节点（最快上手）

配置里 **`peers: []`**（或不写同伴），进程启动后**立刻成为 Leader**。

```bash
# 终端 1：节点
cargo run --bin raft-node -- --config config/single.yaml

# 终端 2：CLI（只连一个地址）
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 status
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 get a
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 scan
```

`config/single.yaml`：

```yaml
node:
  id: 1
  listen: "127.0.0.1:7001"
  data_dir: "data/single"

peers: []

options:
  heartbeat_interval: 2
  election_timeout_min: 5
  election_timeout_max: 10
  max_append_entries: 100
  pre_vote: true
  check_quorum: true
  snapshot_threshold: 1000
```

数据目录：`data/single/bitcask.log`（已在 `.gitignore` 中忽略 `data/`）。

也可：`cargo run -- --config config/single.yaml`（`default-run = raft-node`）。

---

## 多节点（每节点一份配置）

三份配置、三个进程，例如 `config/node1.yaml` / `node2.yaml` / `node3.yaml`。

```bash
# 三个终端分别启动
cargo run --bin raft-node -- --config config/node1.yaml
cargo run --bin raft-node -- --config config/node2.yaml
cargo run --bin raft-node -- --config config/node3.yaml

# CLI：可写多个 peer，失败/Abort 时换节点重试
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 status
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put b banana
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 get b
```

`config/node1.yaml` 示例：

```yaml
node:
  id: 1
  listen: "127.0.0.1:7001"
  data_dir: "data/node1"

peers:
  - { id: 2, addr: "127.0.0.1:7002" }
  - { id: 3, addr: "127.0.0.1:7003" }

options:
  heartbeat_interval: 2
  election_timeout_min: 5
  election_timeout_max: 10
  max_append_entries: 100
  pre_vote: true
  check_quorum: true
  snapshot_threshold: 1000
```

节点 2、3 同理：改 `id` / `listen` / `data_dir`，`peers` 写另外两个地址。

---

## raft-cli 命令

```text
raft-cli --peers host:port[,host:port...] [--client-id UUID] <command>
```

| 命令 | 说明 |
|---|---|
| `put <key> <value>` | 带 session 的写（幂等序号） |
| `get <key>` | 线性一致读 |
| `scan` | 扫描全部 KV |
| `status` | 领导、term、commit、voters 等 |
| `members <id,id,...>` | 成员变更（目标投票集合） |

### 客户端 session 文件

- 路径：**运行 CLI 时的当前工作目录**下的 `.raft-cli-session`
- 内容：`client_id` + `last_seq`（每次 `put` 递增）
- 作用：超时重试同一写时带相同 `(client_id, seq)`，避免双执行
- 可用 `--client-id <uuid>` 覆盖 id；文件已在 `.gitignore` 中

这不是集群配置，也不是节点数据；删了只会让 CLI 当成新客户端重新编号。

---

## 配置字段

| 字段 | 含义 |
|---|---|
| `node.id` | 节点 ID（`u8`，集群内唯一） |
| `node.listen` | 本机监听 `ip:port` |
| `node.data_dir` | 数据目录（BitCask + 快照元数据） |
| `peers` | 其它节点 `{ id, addr }`；**空 = 单节点** |
| `options.heartbeat_interval` | 心跳间隔（tick） |
| `options.election_timeout_min/max` | 选举超时范围（tick） |
| `options.max_append_entries` | 单次 Append 最多条数 |
| `options.pre_vote` | 是否 Pre-vote |
| `options.check_quorum` | 领导丢多数是否下台 |
| `options.snapshot_threshold` | 距上次快照 apply 多少条后压缩；`0` 关闭 |

逻辑时间：`TICK_INTERVAL` = 100ms。

---

## 测试

```bash
cargo build --bins    # 真多进程测试需要 raft-node 二进制
cargo test
```

覆盖范围包括：

- 进程内：选主、转发、分区、杀主、丢包/乱序、并发客户端  
- Pre-vote / CheckQuorum  
- 成员变更、**移除领导（领导转移）**  
- Session 去重、BitCask  
- **重启恢复**、**快照截断后重启**  
- **真多进程**（`tests/multi_process.rs` 拉起子进程）  

进程内演示（非部署路径）：

```bash
cargo run --example kv_cluster
```

---

## 库内嵌入（可选）

```rust
use raft_rust::cluster::{wait_for_leader, Cluster};

let cluster = Cluster::spawn(&[1, 2, 3]); // 测试用进程内集群
let mut client = cluster.client();
wait_for_leader(&mut client)?;
client.put("k", "v")?;
```

或直接使用 `Node` + 自建传输（见 `src/lib.rs` / `src/net`）。

---

## 目录结构

```text
src/
  bin/raft_node.rs     节点进程（单节点或多节点中的一员）
  bin/raft_cli.rs      命令行客户端
  net/                 TCP + bincode
  cluster/             进程内集群（测试 / example）
  config.rs            节点 YAML 加载
  raft/                协议：node / log / membership / session / kv
  storage/             Engine + BitCask
config/
  single.yaml          单节点
  node1.yaml … node3.yaml
tests/                 单元与集成测试
examples/kv_cluster.rs 进程内三节点演示
```

---

## 已知限制

- 明文 TCP，**无 TLS / 认证**（生产可在前面做 TLS 终止）  
- 快照整包发送，不分块流式  
- 无自动领导优雅迁移 RPC（通过成员变更去掉旧领导实现 step down）  
- 非生产级运维能力（监控、动态配置热更新等未做）  

---

## 许可证

Apache-2.0。实现紧贴 [Raft 论文](https://raft.github.io/raft.pdf)，并包含成员变更、Pre-vote、快照、多进程部署等扩展。
