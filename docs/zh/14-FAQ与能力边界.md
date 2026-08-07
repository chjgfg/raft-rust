# 14. FAQ 与能力边界

> **导航：** [← 13. 故障场景与测试对照](./13-故障场景与测试对照.md) · [索引](./README.md)

---

## 1. 这是生产级 Raft 吗？

**不是。** 用于学习与本地原型。缺 TLS、运维面、分块快照、完善的流控与审计等。

---

## 2. 业务数据存在磁盘哪里？

- **Raft 日志 / term / commit / 快照元数据与快照体**：`{data_dir}/bitcask.log`（BitCask）  
- **字符串 KV 表**：进程内 `Kv` 内存；靠日志重放或 `SnapshotData` 恢复  

**没有**把 `put b banana` 存成 BitCask 用户键 `"b"`。见 [04](./04-数据存取全流程.md)、[06](./06-BitCask与日志键空间.md)。

---

## 3. 为什么连从节点也能 put/get？

Follower **转发** `ClientRequest` 到 Leader；真正写日志与线性读只在 Leader。见 [04](./04-数据存取全流程.md)。

---

## 4. `.raft-cli-session` 是什么？在哪？

CLI 的 **client_id + last_seq**，用于写幂等。路径 = **运行 CLI 时的 cwd** 下的 `.raft-cli-session`。  
只有 **`put`** 会创建/更新；只开 node 或只 `status` **不会有**。见 [09](./09-Session与CLI幂等.md)。

---

## 5. Abort 是什么？

`Error::Abort`：无主、选举中、领导切换、转发取消等。  
**不是**业务错误。CLI 会换 peer 重试；带 Session 的 put 重试同一 seq。

---

## 6. 单节点和三节点怎么切换？

- 单节点：`peers: []`（`config/single.yaml`）  
- 三节点：三份 YAML 互指 peer，起三个 `raft-node`  

不能只改一份配置就「变出」另外两个进程。

---

## 7. 如何加第四个节点？

1. 写 `node4.yaml` 并启动进程（空 data 或从快照追）  
2. CLI：`members 1,2,3,4`  
3. 等待 Joint→Simple；新节点追日志/快照  

见 [10](./10-成员变更与领导转移.md)、[11](./11-快照与落后追赶.md)。

---

## 8. 如何去掉当前领导？

`members` 目标集合**不含**当前 leader → Simple 提交后旧领导 step down，剩余节点选新主。  
无单独 TransferLeadership RPC。

---

## 9. 有 TLS 吗？

**无。** 明文 TCP。生产可在前面做 TLS 终止或自封装。

---

## 10. 快照会分块吗？

**不会。** `InstallSnapshot` 一次带全量 `data`。

---

## 11. 进程内 `cluster` 和多进程有何不同？

| | `src/cluster` | `raft-node`×N |
|--|---------------|----------------|
| 传输 | 内存 channel | TCP |
| 用途 | 单测/故障注入 | 真实多进程 |
| 配置 | 代码里 node id 列表 | 每进程 YAML |

---

## 12. 嵌入库怎么用？

```rust
// 进程内测试集群
use raft_rust::cluster::{Cluster, wait_for_leader};
let c = Cluster::spawn(&[1,2,3]);
let mut client = c.client();
wait_for_leader(&mut client)?;
client.put("k","v")?;

// 或自建 Node + 自管传输
// Node::new / step / tick + Sender<Envelope>
```

实现自己的 `State` / `Engine` 可换状态机与存储。

---

## 13. 文档与代码冲突？

**以代码为准。**

---

## 14. 刻意不做的能力

- 领导者租约读  
- 客户端库内无限重试以外的复杂会话（仅 CLI 文件 session）  
- 完整可观测性（metrics/tracing 体系）  
- 跨机安全通信  

---

## 15. 推荐阅读顺序（全文）

```text
01 架构 → 02 消息 → 03 选举 → 04 存取
05 角色 → 06 BitCask
07 复制冲突 → 08 线性读 → 09 Session
10 成员变更 → 11 快照
12 部署运行时 → 13 故障测试 → 14 FAQ
```

---

> **导航：** [← 13. 故障场景与测试对照](./13-故障场景与测试对照.md) · [索引](./README.md)
