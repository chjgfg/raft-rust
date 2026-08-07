# 09. Session 与 CLI 幂等

> **导航：** [← 08. 线性一致读](./08-线性一致读.md) · [10. 成员变更与领导转移 →](./10-成员变更与领导转移.md) · [索引](./README.md)

写可能因超时重试而**执行两次**。本实现用 **client session（client_id + seq）** 做 at-most-once 应用。源码：`src/raft/session.rs`、`src/bin/raft_cli.rs`、Leader 的 `WriteSession`。

---

## 1. 问题

```text
CLI put a=1 → Leader 已 commit+apply
响应在路上丢失 / CLI 超时
CLI 再 put a=1 → 若无去重 → 又一条日志、再 apply 一次
```

对覆盖写 `Put` 结果可能看起来一样；对「计数 +1」类命令会错。本库 CLI 主要是 Put，但协议按通用幂等设计。

---

## 2. 协议形状

### 2.1 客户端请求

```text
Request::WriteSession {
  client_id: Uuid,   // 客户端会话 ID
  seq: u64,          // 该会话内单调递增
  command: Vec<u8>,  // 通常 bincode(kv::Command::Put)
}
```

对比：`Request::Write(bytes)` **无** session，测试或自用不保证重试安全。

### 2.2 进入 Raft 日志的字节

Leader 不把三个字段拆开存，而是：

```text
encode_session(client_id, seq, command)
  = b"SESS" ‖ bincode(SessionCommand { client_id, seq, payload: command })
```

整段作为 `Entry.command`。这样 **重放日志也能重建 session 表**（表本身也可进 snapshot）。

---

## 3. apply 时规则（`SessionState`）

每个 `client_id` 只保留 **最后一次成功** 的 `(last_seq, cached_response_bytes)`。

| 到达的 seq | 行为 |
|------------|------|
| **> last**（或首次） | 对 inner 执行 `payload`；更新缓存为本次响应 |
| **== last** | 视为同一请求重试：inner 走 **noop**（只推 applied_index）；**返回缓存响应** |
| **< last** | 过期序号：inner noop；**返回空字节**（不返回更新写的缓存，防串结果） |

成员变更 / 纯 noop 条目：`command=None`，直接交给 inner，不走 session。

---

## 4. CLI 如何维护 session

文件：**运行 `raft-cli` 时的当前工作目录**下 `.raft-cli-session`：

```text
client_id=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
last_seq=3
```

| 操作 | 行为 |
|------|------|
| 首次 put | 若无文件则 new UUID；`seq=1`；save |
| 再次 put | `seq = last_seq+1`；先 save 再发（崩溃后序号不回退） |
| get/scan/status | **不**改 session 文件 |
| `--client-id UUID` | 覆盖 client_id，仍用文件中的 last_seq 递增 |

重试（Abort/超时换 peer）：**同一进程内同一次 put** 使用**同一个**已分配的 `(client_id, seq)` 再发，不会先 next_seq。

注意：只有 **`put` 成功走到 `session.save()`** 才会创建文件；只跑 node 或只 status **不会**出现该文件。

---

## 5. 数据流（带 Session 的写）

```text
CLI: (id, seq=5) + Put
  → Leader: Entry.command = SESS|…|Put
  → 复制提交
  → 各节点 SessionState::apply
       首次 seq=5 → Kv put；缓存响应
  → 响应丢失
CLI 重试 (id, seq=5)
  → 再一条日志 或 若未再提出则仅重试 RPC
  → apply 见 seq==last → 返回同一 Put 响应字节，Kv 不二次修改语义值
```

若 Leader 在第一次已写入日志后崩溃：重试可能再 propose 一条 **相同 session 的新 index**；各节点 apply 第二条时仍按 seq 去重，状态正确。

---

## 6. 与状态机快照

`SessionState::snapshot` 把 inner 快照 + 全部 `(client_id, seq, resp)` 一并序列化。  
重启 `restore` 后去重表仍在，避免「快照后重放历史 seq」出问题（紧接的日志重放仍遵守规则）。

---

## 7. 源码锚点

| 点 | 文件 |
|----|------|
| 编码 | `encode_session` / `decode_session` |
| 去重 apply | `SessionState::apply` |
| CLI 文件 | `raft_cli.rs` `SessionStore` |
| Leader 入口 | `Request::WriteSession` |

---

> **导航：** [← 08. 线性一致读](./08-线性一致读.md) · [10. 成员变更与领导转移 →](./10-成员变更与领导转移.md) · [索引](./README.md)
