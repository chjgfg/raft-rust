# 08. Linearizable Reads

> **Nav:** [← 07. Log Replication and Conflicts](./07-log-replication-and-conflicts.md) · [09. Session and CLI Idempotency →](./09-session-and-cli-idempotency.md) · [Index](./README.md)

Doc 04 briefly described that reads must go through the leader. This article explains **why reads do not enter the log**, **when `State::read` is allowed**, and **how the `read_seq` quorum confirmation works**. Source: the `Leader`'s `ClientRequest::Read`, `maybe_read`, and the `Heartbeat`/`Read` messages.

---

## 1. Goal Semantics

**Linearizable read**: the value read must appear in the same total order as writes on a single machine — you must not read stale state from a "former leader that no longer holds leadership", nor read before this term's state has caught up.

This implementation:

- Reads do **not** write to the Raft log (no "read consensus round")
- `State::read` executes **only on the Leader**
- Before executing, **a majority of nodes must confirm** that the leader still holds leadership in this term (via the monotonically increasing `read_seq`)

There is no leader lease: each read may trigger a majority RPC round (which can piggyback on Heartbeat).

---

## 2. End-to-End Steps

```text
1. Client Request::Read(bincode(Get/Scan))
2. Any node → forward to the Leader (same as writes)
3. Leader:
     role.read_seq += 1
     reads.push_back({ seq, from, id, command })
     broadcast Message::Read { seq: read_seq }
     // single node: call maybe_read directly
4. Follower (recognizes the current leader):
     reply ReadResponse { seq }
     // or carry the heartbeat's read_seq back in HeartbeatResponse
5. Leader advances progress[peer].read_seq
6. maybe_read():
     latches pass and a majority has confirmed >= some seq
     → state.read in order → ClientResponse::Read
```

---

## 3. The Two Latches (`maybe_read`)

Before counting the majority:

```text
(commit_term, commit_index) = log.get_commit_index()
applied = state.get_applied_index()

if commit_term < current term:   don't read yet   // nothing committed in this term yet
if applied < commit_index:       don't read yet   // state machine lags behind the committed log
```

### Why must "this term have a commit"?

When a new leader is elected it appends a **noop** via `propose(None)`. Only after the noop (or another entry of this term) commits, `commit_term == current term`, which guarantees:

- The leader's log already contains the committed prefix from previous terms
- Local apply has at least caught up to a safe point, avoiding reading a stale state machine that "does not include old commits"

This matches the motivation behind **ReadIndex / leader-term commit** in the paper and common implementations.

---

## 4. How quorum_read_seq Is Computed

```text
matched_seq = [(self, role.read_seq)]
            + [(peer, progress[peer].read_seq) for peer]

over all seqs seen, from largest to smallest:
  let S = { node | its confirmed read_seq >= seq }
  if membership.has_quorum(S):
    quorum_read_seq = seq; break
```

Then:

```text
while the head of reads.seq <= quorum_read_seq:
  pop → state.read(command) → return ClientResponse
```

The read queue is ordered by seq; smaller ones complete first.

---

## 5. The Relation Between Heartbeat and Read Messages

| Message | Role |
|------|------|
| `Message::Read { seq }` | Broadcast specifically for read confirmation (eager) |
| `Heartbeat { ..., read_seq }` | Periodically piggybacks the latest read_seq; the Follower carries it back verbatim in `HeartbeatResponse` |

A lost `Read` can be "re-confirmed" by a subsequent heartbeat, preventing the read from being permanently stuck.

A Follower only responds to Read / Heartbeat when it **recognizes the sender as leader**.

---

## 6. Roles on the Read Path

| Role | Does | Doesn't |
|------|----|------|
| CLI / any node | sends the Read request | does not scan local disk as the result |
| Follower | forwards; replies ReadResponse | does not `state.read` for the client |
| Candidate | Abort | — |
| Leader | assigns seq, waits for the majority, `state.read` | does not write the read into an Entry |

---

## 7. Interleaving with Writes

```text
after put commits and applies
  → applied_index advances
  → a subsequent get, once the latches and the majority confirmation pass
  → is guaranteed to read that put (linearizable: a read started after a write completes)
```

If get and put are concurrent, the result may be before or after the put, but there will be no linearizability violation of "a put that succeeded yet is permanently unreadable" (as long as the leader is stable and messages are eventually delivered).

---

## 8. Single Node

`cluster_size()==1`: after broadcasting Read, immediately `maybe_read`; its own single vote is a majority.

---

## 9. Source Anchors

| Logic | Location |
|------|------|
| Receive read request | Leader `Request::Read` |
| Latches + dequeue | `maybe_read` |
| Follower confirmation | `Message::Read` / Heartbeat response |
| State machine read-only | `Kv::read` / `State::read` |

---

> **Nav:** [← 07. Log Replication and Conflicts](./07-log-replication-and-conflicts.md) · [09. Session and CLI Idempotency →](./09-session-and-cli-idempotency.md) · [Index](./README.md)
