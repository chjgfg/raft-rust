# 06. BitCask and Log Keyspace

> **Nav:** [← 05. Node Roles](./05-node-roles.md) · [07. Log Replication and Conflicts →](./07-log-replication-and-conflicts.md) · [Index](./README.md)

This document explains: **what is actually on disk**, and how the Raft `Log` maps logical keys to BitCask's physical file.

Source: `src/storage/bitcask.rs`, `src/storage/engine.rs`, `src/raft/log.rs`.

---

## 1. Two-Layer Structure

```text
┌────────────────────────────────────────────┐
│  Raft Log API (log.rs)                     │
│  append / splice / commit / set_term_vote  │
│  logical keys Key::Entry / TermVote / ...  │
└──────────────────┬─────────────────────────┘
                   │ Engine::set/get/delete/scan/flush
┌──────────────────▼─────────────────────────┐
│  BitCask (bitcask.rs)                      │
│  in-memory keydir: logical key → offset    │
│  disk file:    data_dir/bitcask.log append │
└────────────────────────────────────────────┘
```

The business `Kv` is **not** stored by string key in this layer; it lives in memory after apply (and can be snapshotted wholesale into `Key::SnapshotData`).

---

## 2. Physical File Record Format

Each BitCask append record:

```text
┌──────────┬───────────┬─────────┬───────────┐
│ key_len  │ value_len │  key    │  value    │
│ u32 BE   │ i32 BE    │ key_len │ value_len │
│ 4 bytes  │ 4 bytes   │         │           │
└──────────┴───────────┴─────────┴───────────┘
```

- `value_len = -1`: a tombstone (deletion); no value bytes  
- On file open, scan sequentially to rebuild the `keydir`  
- Setting the same logical key multiple times: the file grows, keydir points to the **latest** offset; old records become garbage  
- `flush` → `sync_all` (in non-test configs)  

Path example: `data/node1/bitcask.log` (opened by `raft-node`).

---

## 3. Raft Logical Key Encoding

`src/raft/log.rs` `Key::encode()`:

| Variant | Bytes | Value contents (bincode unless noted) |
|------|------|---------------------------|
| `Entry(index)` | `0x00` + `index` big-endian 8 bytes | `Entry{index,term,command,membership}` |
| `TermVote` | `0x01` | `(term, Option<NodeID>)` |
| `CommitIndex` | `0x02` | `(commit_index, commit_term)` |
| `SnapshotMeta` | `0x03` | `(last_included_index, last_included_term)` |
| `SnapshotData` | `0x04` | State-machine snapshot **raw bytes** |

Lexicographic order: all `Entry` keys sort before the metadata keys (`0x00 < 0x01`).

### Entry Value Structure Recap

```rust
Entry {
  index: u64,
  term: u64,
  command: Option<Vec<u8>>,      // Put / SESS|Put / None
  membership: Option<MembershipEntry>,
}
```

The common `command` shape when writing business data:

```text
"SESS" ‖ bincode({ client_id, seq, payload: bincode(Put{key,value}) })
```

---

## 4. Operation → Key Reference

| Raft operation | Engine call | Logical key |
|-----------|-------------|--------|
| Append to the log | `set` | `Entry(i)` |
| Overwrite on conflict / truncate the tail | `set` / `delete` | Multiple `Entry` |
| Vote / new term | `set` + **always flush** | `TermVote` |
| Commit | `set` (flush optional) | `CommitIndex` |
| Local snapshot compaction | `delete` old Entries + `set` Meta/Data | `SnapshotMeta/Data` + delete `Entry(≤snap)` |
| Install snapshot | Clear Entries + Meta/Data | Same as above |

---

## 5. Scan-Related Paths

### 5.1 Raft Log Scan

`Log::scan(range)` → `engine.scan_dyn(Entry key range)` → decode into `Entry`.

Used for:

- Sending a batch of entries to a follower in `Append`  
- `scan_apply`: apply entries in order from `applied+1` to `commit`  

### 5.2 Application-Layer Scan

`kv::Command::Scan` does **not** scan BitCask; instead:

```text
Leader State::read → clone the whole in-memory Kv BTreeMap → encode and return
```

### 5.3 BitCask's Internal Scan

`Engine::scan` walks the keydir in **logical key lexicographic order**, then reads the file by offset. Raft uses it to enumerate the Entry prefix.

---

## 6. One Picture: Disk and Memory After a put

Assume the Leader just committed and applied `put b=banana` (index=42):

```text
In the BitCask file (schematic):
  TermVote     → (6, Some(leader_id))
  CommitIndex  → (42, 6)
  Entry(1)     → noop or history
  ...
  Entry(42)    → command contains SESS|Put b banana
  (optional) SnapshotData → snapshot of the whole table

In-memory Kv:
  data["b"] = "banana"
  applied_index = 42
```

A Follower also has `Entry(42)` after receiving the Append; it only gets `b` in memory after receiving the new commit and applying it.

---

## 7. Restart Recovery

When `raft-node` starts:

```text
1. BitCask::new(path)     // scan the file to build the keydir
2. read SnapshotData; if there is snapshot meta, state.restore
3. Log::new(engine)       // load term/vote/last/commit/first_index
4. Node::new → maybe_apply  // replay entries after commit that are not yet in the state machine
```

Therefore:

- **What is persisted: the Raft log and metadata (+ the optional state-machine snapshot)**  
- **The purely in-memory Kv is recovered via replay or snapshot**  

---

## 8. Common Misconceptions

| Misconception | Reality |
|------|------|
| BitCask contains the key `"b"` | No: business data lives in `Kv`; the log lives in `Entry(i)` |
| Every put is written to disk once | The Leader writes the Entry; each Follower also writes its own BitCask |
| commit always fsyncs | In this implementation, commit **may** skip fsync; term/vote always fsync |
| The scan command scans files | It scans the leader's in-memory state machine |

---

## 9. Source Anchors

| Topic | File |
|------|------|
| Key encoding | `src/raft/log.rs` `Key` |
| append/commit | `Log::append` / `commit` / `splice` |
| BitCask format | `src/storage/bitcask.rs` `write_entry` / `build_keydir` |
| Node open path | `src/bin/raft_node.rs` `data_dir/bitcask.log` |

---

> **Nav:** [← 05. Node Roles](./05-node-roles.md) · [07. Log Replication and Conflicts →](./07-log-replication-and-conflicts.md) · [Index](./README.md)
