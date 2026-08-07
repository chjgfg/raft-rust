# 07. Log Replication and Conflicts

> **Nav:** [← 06. BitCask and Log Keyspace](./06-bitcask-and-log-keyspace.md) · [08. Linearizable Reads →](./08-linearizable-reads.md) · [Index](./README.md)

Doc 04 covered Append on the success path. This article covers **mismatch, rejection, probing, and truncating the conflicting tail**. Source: `Leader::maybe_send_append`, the `Follower`'s `Append` branch, and `Log::splice` (`src/raft/node.rs`, `log.rs`).

---

## 1. Each Follower's Progress

The Leader maintains `Progress` for each peer:

| Field | Meaning |
|------|------|
| `match_index` | Highest index confirmed to match the Leader (initial value 0) |
| `next_index` | Next index to send to that peer (initial value `last_index+1`) |
| `read_seq` | The highest read sequence this peer has confirmed |

Invariant (asserted in the implementation):

```text
match_index <= last_index
match_index < next_index <= last_index + 1
```

---

## 2. Steady State: Direct Append

A client write → Leader `append` obtains `index = L`:

```text
if peer.next_index == L:
  send Append {
    base_index: L-1,
    base_term:  Entry(L-1).term  (when L==1, base=0,0),
    entries:    [Entry(L), ...]  // at most max_append_entries
  }
  optimistically advance next_index to the end of the batch + 1
```

Follower:

```text
if base_index==0 || log.has(base_index, base_term):
  splice(entries)
  AppendResponse { match_index: last_of_batch, reject_index: 0 }
else:
  AppendResponse { match_index: 0, reject_index: min(base, last+1) }
```

On a success response the Leader sets `match_index = max(...)` and `next_index = max(next, match+1)`, then runs `maybe_commit_and_apply`, and may continue sending subsequent batches.

---

## 3. Why Do Conflicts Happen?

Typical scenarios:

1. An old Leader appended but **uncommitted** entries
2. After a network partition, a new Leader writes entries with a **different term** at the same index
3. The Follower's log is shorter / forked

The paper guarantees: the **new Leader's log wins**; conflicting uncommitted tails are overwritten (committed entries are never overwritten).

---

## 4. Rejection and Regress (reject)

The Leader receives:

```text
AppendResponse { match_index: 0, reject_index: R }  // R > 0
```

Handling:

```text
If R <= current match_index → stale response, ignore
else regress_next(R):
  next_index = max(R, match_index+1) and < old next
  then maybe_send_append(peer, probe=true)
```

---

## 5. Probe: Empty Append

When `probe=true`:

```text
Append {
  base_index: next_index - 1,
  base_term:  ...,
  entries:    []      // empty
}
```

Meaning: it only asks "does your base match?" and sends no data.

- Match → `match_index = base`, then switch to sending real entries
- No match → keep decreasing `next_index` and probe again

It can also be triggered by **HeartbeatResponse.match_index==0**: set `next_index` near `last_index` and start probing.

```text
probe next_index from high to low
    │
    ▼
find common base ──► consecutive Appends catch up ──► match_index == leader.last
```

---

## 6. splice on the Follower (Truncating the Conflicting Tail)

Key points of `Log::splice(entries)`:

1. Skip the local prefix that overlaps and has the **same term**
2. Starting from the index of the **first term conflict**:
   - delete the Entry keys in the local `index..old_last` range
   - write the new entries
3. **Forbidden** to write at or below `commit_index` (committed entries are immutable)
4. After success, update `last_index/last_term`

So "conflict repair" = **on the Follower, replace the local uncommitted suffix with the Leader's uncommitted suffix**.

---

## 7. Relation to Commit

- Only when `match_index` forms a **quorum of the current configuration**, and the entry's **term == current leader term**, can `commit_index` be raised
- Entries from previous terms cannot be committed merely by replicating to a majority (paper 5.4.2); they are committed indirectly through the **noop** appended at election

---

## 8. Falling Behind the Local Log: Snapshots

If `next_index < log.first_index()` (the local log has already been compacted):

```text
send InstallSnapshot { last_included_*, data, membership }
```

See [11. Snapshots and Catch-up](./11-snapshots-and-catch-up.md).

---

## 9. Timeline: Fork Repair Sketch

```text
Leader log:  ... 10@T5, 11@T6, 12@T6
Follower:    ... 10@T5, 11@T5 (old master, uncommitted)

Append base=11 term=6 entries=[12...]  → rejected (term of 11 is not 6)
probe base=10 ...
match 10 → Append entries=[11@T6, 12@T6]
Follower splice: delete old 11@T5, write new 11, 12
```

---

## 10. Source Anchors

| Behavior | Location |
|------|------|
| Send append/probe | `maybe_send_append` |
| Reject and regress | Leader `AppendResponse` reject branch |
| Accept and advance | `Progress::advance` |
| Local install | Follower `Append` + `Log::splice` |

---

> **Nav:** [← 06. BitCask and Log Keyspace](./06-bitcask-and-log-keyspace.md) · [08. Linearizable Reads →](./08-linearizable-reads.md) · [Index](./README.md)
