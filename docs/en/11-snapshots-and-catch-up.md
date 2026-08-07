# 11. Snapshots and Catch-up

> **Nav:** [← 10. Membership and Leadership Transfer](./10-membership-and-leadership-transfer.md) · [12. Config, Deploy, and Runtime →](./12-config-deploy-and-runtime.md) · [Index](./README.md)

This document explains: **when to take a local snapshot and truncate the log**, **how a lagging follower catches up via InstallSnapshot**, and where the snapshot bytes live. Source: `maybe_snapshot`, `maybe_send_append`, `InstallSnapshot` handling, `Log::compact_to` / `reset_with_snapshot`.

---

## 1. Why snapshots are needed

When the Raft log only grows and never shrinks:

- Disk grows without bound
- A new node / long-offline node must replay **all** historical Entries

A snapshot = a complete image of the state machine at some `applied_index` + metadata, after which you can **delete the Entries at and before that index**.

This implementation:

- **Local compaction**: the Leader/Follower truncates on its own after applying enough
- **InstallSnapshot**: when the Leader finds a peer's `next_index < first_index`, it sends the whole snapshot in one packet (**no chunking**)

---

## 2. Triggering a local snapshot (`maybe_snapshot`)

Called after `maybe_commit_and_apply` succeeds in applying (skipped when stepping down on leadership transfer).

Conditions (all):

```text
snapshot_threshold > 0
applied - snap_idx >= threshold     // at least N entries since the last snapshot
applied >= commit_index             // applied has caught up with the commit
the term of the applied entry is determinable (or equals the snapshot base)
```

Actions:

```text
1. data = state.snapshot()
2. engine.set(Key::SnapshotData, data)      // 0x04
3. log.compact_to(applied, term)
     delete Entry(first ..= applied)
     write SnapshotMeta (applied, term)
     first_index = applied + 1
```

Config: `options.snapshot_threshold` (YAML, default 1000; `0` = disabled).

---

## 3. Log shape after compaction

```text
Before: Entry(1)..Entry(1000)  commit=1000 applied=1000
After:
  SnapshotMeta = (1000, term)
  SnapshotData = state machine bytes
  first_index  = 1001
  no Entry(i) for i <= 1000
  new writes start at 1001
```

`Log::has` / `get` / `scan` treat indexes `< first_index` as not in the log (the base is described by SnapshotMeta).

---

## 4. Lagging follower: InstallSnapshot

### 4.1 When it is sent

`maybe_send_append(peer)`:

```text
if progress.next_index < log.first_index():
  send InstallSnapshot {
    last_included_index,
    last_included_term,   // from SnapshotMeta
    data: state.snapshot(),  // current state machine (same as or newer than the snapshot point)
    membership: active config,
  }
```

That is: the next log entry the peer wants has already been compacted away locally, so **the only option is to send the snapshot**.

### 4.2 Follower handling

```text
1. verify it comes from the current leader (otherwise ignore)
2. if last_included_index is too old (< local snap or applied) → do not install, still ACK
3. otherwise:
     state.restore(data, last_included_index)
     engine.set(SnapshotData, data)
     log.reset_with_snapshot(index, term)   // clear Entries, set commit/first/last
     membership.apply_entry(...)
4. InstallSnapshotResponse { last_included_index }
```

### 4.3 Leader receives the response

```text
progress.advance(last_included_index)
maybe_send_append continues sending subsequent Entries (log after the snapshot point)
```

---

## 5. Data flow diagram

```text
Leader applies many entries
  → maybe_snapshot → BitCask: SnapshotData + delete old Entries

New node / very stale Follower
  → next_index < first_index
  → InstallSnapshot whole packet
  → Follower restores Kv + resets the log
  → then Append incremental Entries
```

---

## 6. Restart and snapshots

`raft-node` startup:

```text
open BitCask
read SnapshotData; if SnapshotMeta.index > 0 → state.restore
Log::new loads first/commit/term
Node::new → maybe_apply replays the tail of first..commit not yet applied to the state machine
```

See the restart section in [13](./13-fault-scenarios-and-tests.md).

---

## 7. Limitations (teaching implementation)

| Point | Current status |
|----|------|
| Chunked transfer | **No**, large state sent in one go |
| Snapshot vs apply concurrency | single-threaded step, no concurrency |
| Data sent | current `state.snapshot()`, relies on local applied being at least the snapshot point |

---

## 8. Source anchors

| Behavior | Location |
|------|------|
| Local truncation | `maybe_snapshot` / `Log::compact_to` |
| Sending snapshot | first half of `maybe_send_append` |
| Installation | Follower `InstallSnapshot` |
| Keys | `Key::SnapshotMeta` / `SnapshotData` |

---

> **Nav:** [← 10. Membership and Leadership Transfer](./10-membership-and-leadership-transfer.md) · [12. Config, Deploy, and Runtime →](./12-config-deploy-and-runtime.md) · [Index](./README.md)
