# 06. BitCask 与日志键空间

> **导航：** [← 05. 节点角色与职责](./05-节点角色与职责.md) · [07. 日志复制与冲突修复 →](./07-日志复制与冲突修复.md) · [索引](./README.md)

本文说明：**磁盘上到底有什么**，以及 Raft `Log` 如何把逻辑键映射到 BitCask 物理文件。

源码：`src/storage/bitcask.rs`、`src/storage/engine.rs`、`src/raft/log.rs`。

---

## 1. 两层结构

```text
┌────────────────────────────────────────────┐
│  Raft Log API（log.rs）                     │
│  append / splice / commit / set_term_vote  │
│  逻辑键 Key::Entry / TermVote / ...         │
└──────────────────┬─────────────────────────┘
                   │ Engine::set/get/delete/scan/flush
┌──────────────────▼─────────────────────────┐
│  BitCask（bitcask.rs）                      │
│  内存 keydir:  逻辑键 → 文件偏移             │
│  磁盘文件:     data_dir/bitcask.log 追加     │
└────────────────────────────────────────────┘
```

业务 `Kv` **不在**这一层按字符串键存储；它在 apply 后的内存里（可整体 snapshot 到 `Key::SnapshotData`）。

---

## 2. 物理文件记录格式

BitCask 每条 append 记录：

```text
┌──────────┬───────────┬─────────┬───────────┐
│ key_len  │ value_len │  key    │  value    │
│ u32 BE   │ i32 BE    │ key_len │ value_len │
│ 4 字节   │ 4 字节    │         │           │
└──────────┴───────────┴─────────┴───────────┘
```

- `value_len = -1`：墓碑（删除），无 value 字节  
- 打开文件时顺序扫描重建 `keydir`  
- 同一逻辑键多次 set：文件变长，keydir 指向**最新**偏移；旧记录变 garbage  
- `flush` → `sync_all`（非 test 配置）

路径示例：`data/node1/bitcask.log`（由 `raft-node` 打开）。

---

## 3. Raft 逻辑键编码

`src/raft/log.rs` `Key::encode()`：

| 变体 | 字节 | 值内容（bincode，除非注明） |
|------|------|---------------------------|
| `Entry(index)` | `0x00` + `index` 大端 8 字节 | `Entry{index,term,command,membership}` |
| `TermVote` | `0x01` | `(term, Option<NodeID>)` |
| `CommitIndex` | `0x02` | `(commit_index, commit_term)` |
| `SnapshotMeta` | `0x03` | `(last_included_index, last_included_term)` |
| `SnapshotData` | `0x04` | 状态机 snapshot **原始字节** |

字典序：所有 `Entry` 键排在元数据键之前（`0x00 < 0x01`）。

### Entry 值结构回顾

```rust
Entry {
  index: u64,
  term: u64,
  command: Option<Vec<u8>>,      // Put / SESS|Put / None
  membership: Option<MembershipEntry>,
}
```

写业务时 `command` 常见形态：

```text
"SESS" ‖ bincode({ client_id, seq, payload: bincode(Put{key,value}) })
```

---

## 4. 操作 → 键 对照

| Raft 操作 | Engine 调用 | 逻辑键 |
|-----------|-------------|--------|
| 追加日志 | `set` | `Entry(i)` |
| 冲突覆盖/截断尾 | `set` / `delete` | 多个 `Entry` |
| 投票 / 新 term | `set` + **总是 flush** | `TermVote` |
| 提交 | `set`（可不 flush） | `CommitIndex` |
| 本地快照压缩 | `delete` 旧 Entry + `set` Meta/Data | `SnapshotMeta/Data` + 删 `Entry(≤snap)` |
| 安装快照 | 清空 Entry + Meta/Data | 同上 |

---

## 5. 与「扫描」相关的路径

### 5.1 Raft 日志扫描

`Log::scan(range)` → `engine.scan_dyn(Entry 键范围)` → 解码为 `Entry`。

用于：

- 向 follower 发送 `Append` 一批条目  
- `scan_apply`：从 `applied+1` 到 `commit` 依次应用  

### 5.2 应用层 Scan

`kv::Command::Scan` **不扫 BitCask**，而是：

```text
Leader State::read → Kv 内存 BTreeMap 全表 clone → 编码返回
```

### 5.3 BitCask 内部 scan

`Engine::scan` 按 **逻辑键字典序** 走 keydir，再按偏移读文件。Raft 用它枚举 Entry 前缀。

---

## 6. 一张图：put 之后磁盘与内存

假设 Leader 刚提交并 apply 了 `put b=banana`（index=42）：

```text
BitCask 文件中（示意）:
  TermVote     → (6, Some(leader_id))
  CommitIndex  → (42, 6)
  Entry(1)     → noop 或历史
  ...
  Entry(42)    → command 含 SESS|Put b banana
  (可选) SnapshotData → 整表快照

进程内存 Kv:
  data["b"] = "banana"
  applied_index = 42
```

Follower 在收到 Append 后也有 `Entry(42)`；在收到新 commit 并 apply 后内存才有 `b`。

---

## 7. 重启恢复

`raft-node` 启动：

```text
1. BitCask::new(path)     // 扫文件建 keydir
2. 读 SnapshotData；若有 snapshot meta 则 state.restore
3. Log::new(engine)       // 加载 term/vote/last/commit/first_index
4. Node::new → maybe_apply  // 重放 commit 之后尚未进状态机的条目
```

因此：

- **持久的是 Raft 日志与元数据（+可选状态机快照）**  
- **纯内存 Kv 靠重放或快照恢复**  

---

## 8. 常见误解

| 误解 | 实情 |
|------|------|
| BitCask 里有键 `"b"` | 否，业务在 `Kv`；日志在 `Entry(i)` |
| 每个 put 只写一次磁盘 | Leader 写 Entry；各 Follower 也各写自己的 BitCask |
| commit 一定 fsync | 实现里 commit **可不** fsync；term/vote 必 fsync |
| scan 命令扫文件 | 扫的是领导内存状态机 |

---

## 9. 源码锚点

| 主题 | 文件 |
|------|------|
| 键编码 | `src/raft/log.rs` `Key` |
| append/commit | `Log::append` / `commit` / `splice` |
| BitCask 格式 | `src/storage/bitcask.rs` `write_entry` / `build_keydir` |
| 节点打开路径 | `src/bin/raft_node.rs` `data_dir/bitcask.log` |

---

> **导航：** [← 05](./05-节点角色与职责.md) · [索引](./README.md) · [07. 日志复制与冲突修复 →](./07-日志复制与冲突修复.md)
