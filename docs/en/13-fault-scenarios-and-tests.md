# 13. Fault Scenarios and Tests

> **Nav:** [← 12. Config, Deploy, and Runtime](./12-config-deploy-and-runtime.md) · [14. FAQ and Limits →](./14-faq-and-limits.md) · [Index](./README.md)

Maps **real faults** to the **tests** in the repo, so you can verify behavior against code. In-process faults mostly use `src/cluster`; real TCP is in `tests/multi_process.rs`.

---

## 1. Scenario matrix

| Scenario | Expected behavior | Main test |
|------|----------|----------|
| Normal three-node read/write | elect leader, put/get | `tests/cluster.rs` |
| Kill Leader | new leader elected; committed data remains | `fault_leader.rs` |
| Minority partition | minority cannot commit; majority can write; consistent after heal | `fault_partition.rs` |
| Pre-vote isolated node | minority cannot raise an effective leader, less disturbance | `prevote_checkquorum.rs` |
| CheckQuorum | isolated leader steps down | same as above |
| Packet loss / reorder | eventually can still elect and write | `transport_unreliable.rs` |
| Concurrent clients | no panic; keys eventually visible | `concurrent_clients.rs` |
| Read after write | read after a successful write returns the value | `linearizable_read.rs` |
| Membership change | add/remove nodes; data preserved | `membership.rs` |
| Remove current leader | old leader steps down; new leader can write | `remove_leader_steps_down` |
| Restart (BitCask) | reopen the same path, data present | `restart_and_snapshot.rs` |
| Restart after snapshot | full reads still work after compaction | same as above |
| Real multi-process TCP | child raft-node + CLI protocol | `multi_process.rs` |
| Session dedup | same seq not executed twice; old seq does not mix results | `session_and_file.rs` |
| Deterministic election | step/tick without network threads | `node_unit.rs` |

---

## 2. Killing the Leader (Leader crash)

```text
1. cluster is normal, put already committed
2. stop(leader) or kill the raft-node process
3. remaining nodes: election timeout → Pre-vote → Campaign
4. the new Leader's log contains the committed prefix (guaranteed by the election restriction)
5. get still returns the committed value; put can continue
```

In-process: `Cluster::stop(id)`.
Multi-process: kill the corresponding OS process; the data is in `data_dir`, and a restart can rejoin (as long as the membership still includes it).

---

## 3. Network partition

```text
partition {1} | {2,3}

Minority 1:
  - if it was the leader → CheckQuorum steps down or cannot commit with a majority
  - put to 1 → Abort/timeout
Majority 2,3:
  - can elect a new leader or keep the current one
  - put succeeds

After heal_all:
  - logs align; minority writes not committed by a majority do not appear
  - keys committed by the majority are globally visible
```

Pre-vote: the partitioned node repeatedly pre-votes, and **without a majority** it won't pointlessly raise its term to disturb the stable majority (see [03](./03-election-process.md)).

---

## 4. Log divergence (conceptual)

The old leader appends uncommitted entries in a minority partition → the new leader writes a different term at the same index → after heal:

```text
Append/probe → splice truncates the old tail → the new leader's log wins
```

See [07](./07-log-replication-and-conflicts.md) for details. The integration side verifies more that "after heal, it agrees with the majority"; the low-level unit tests can construct divergence.

---

## 5. Restart recovery

### 5.1 No snapshot / not compacted

```text
the BitCask file keeps Entry + TermVote + CommitIndex
startup → Log::new loads
→ empty Kv + SessionState
→ maybe_apply replays from applied(0) to commit
→ business keys recovered
```

### 5.2 With SnapshotData

```text
restore(SnapshotData, last_included)
→ maybe_apply only replays the tail of first_index..=commit
```

Tests: `restart_replays_log_and_restores_kv`, `snapshot_compact_then_restart`.

---

## 6. Packet loss and reorder (in-process)

| API | Effect |
|-----|------|
| `set_drop_rate(p)` | randomly drop Envelope |
| `set_reorder(true)` | simple reorder buffer |
| `partition` / `heal` | disconnect / restore |

Expected: eventually still elect a leader and converge (no real-time latency guarantee).

---

## 7. Real multi-process test notes

```bash
cargo build --bin raft-node
cargo test --test multi_process
```

The test will:

- write a temp YAML + data_dir
- spawn `target/debug/raft-node`
- do status/put/get over TCP `WireMsg`
- kill the child processes on Drop

---

## 8. Run the full suite

```bash
cargo build --bins
cargo test
```

---

> **Nav:** [← 12. Config, Deploy, and Runtime](./12-config-deploy-and-runtime.md) · [14. FAQ and Limits →](./14-faq-and-limits.md) · [Index](./README.md)
