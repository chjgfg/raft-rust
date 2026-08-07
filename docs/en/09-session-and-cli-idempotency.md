# 09. Session and CLI Idempotency

> **Nav:** [← 08. Linearizable Reads](./08-linearizable-reads.md) · [10. Membership and Leadership Transfer →](./10-membership-and-leadership-transfer.md) · [Index](./README.md)

A write may be **executed twice** because of retries after a timeout. This implementation uses a **client session (client_id + seq)** for at-most-once application. Source: `src/raft/session.rs`, `src/bin/raft_cli.rs`, and the Leader's `WriteSession`.

---

## 1. The Problem

```text
CLI put a=1 → Leader has already committed + applied
the response is lost on the way / the CLI times out
CLI puts a=1 again → without deduplication → another log entry, applied again
```

For an overwriting `Put` the result may look the same; for commands like "increment a counter" it would be wrong. This library's CLI is mostly Put, but the protocol is designed for general idempotency.

---

## 2. Protocol Shape

### 2.1 Client Request

```text
Request::WriteSession {
  client_id: Uuid,   // client session ID
  seq: u64,          // monotonically increasing within this session
  command: Vec<u8>,  // usually bincode(kv::Command::Put)
}
```

Compare: `Request::Write(bytes)` has **no** session; used by tests or self-use, with no retry-safety guarantee.

### 2.2 The Bytes That Enter the Raft Log

The Leader does not store the three fields separately; instead:

```text
encode_session(client_id, seq, command)
  = b"SESS" ‖ bincode(SessionCommand { client_id, seq, payload: command })
```

The whole segment becomes `Entry.command`. This way, **replaying the log can rebuild the session table** (the table itself can also go into a snapshot).

---

## 3. The apply-time Rule (`SessionState`)

For each `client_id`, only the **last successful** `(last_seq, cached_response_bytes)` is kept.

| Incoming seq | Behavior |
|------------|------|
| **> last** (or first) | Execute `payload` on inner; update the cache to this response |
| **== last** | Treated as a retry of the same request: inner goes **noop** (only advances applied_index); **return the cached response** |
| **< last** | Stale sequence: inner noop; **return empty bytes** (do not return the cache of a newer write, to prevent cross-contamination of results) |

Membership changes / pure noop entries: `command=None`, handed directly to inner, not routed through the session.

---

## 4. How the CLI Maintains the Session

File: `.raft-cli-session` in the **current working directory where `raft-cli` runs**:

```text
client_id=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
last_seq=3
```

| Operation | Behavior |
|------|------|
| First put | If no file, new UUID; `seq=1`; save |
| Subsequent put | `seq = last_seq+1`; save before sending (the sequence never regresses after a crash) |
| get/scan/status | Does **not** modify the session file |
| `--client-id UUID` | Overrides client_id; still increments from the file's last_seq |

Retries (Abort / timeout to another peer): the **same put within the same process** re-sends with the **same** already-assigned `(client_id, seq)`, without advancing seq first.

Note: the file is created only when a **`put` successfully reaches `session.save()`**; running only a node or only status will **not** produce this file.

---

## 5. Data Flow (Writes with Session)

```text
CLI: (id, seq=5) + Put
  → Leader: Entry.command = SESS|…|Put
  → replicate & commit
  → each node SessionState::apply
       first time seq=5 → Kv put; cache the response
  → the response is lost
CLI retries (id, seq=5)
  → another log entry, or if not re-proposed, only a retried RPC
  → apply sees seq==last → returns the same Put response bytes; Kv's value is not modified a second time
```

If the Leader crashes after the first write reached the log: the retry may propose another **new index for the same session**; when nodes apply the second entry they still deduplicate by seq, so the state stays correct.

---

## 6. Interaction with State Machine Snapshots

`SessionState::snapshot` serializes the inner snapshot together with all `(client_id, seq, resp)` entries.
After a restart, `restore` keeps the deduplication table, avoiding problems with "replaying historical seqs after a snapshot" (the subsequent log replay still follows the rules).

---

## 7. Source Anchors

| Point | File |
|----|------|
| Encoding | `encode_session` / `decode_session` |
| Dedup apply | `SessionState::apply` |
| CLI file | `raft_cli.rs` `SessionStore` |
| Leader entry | `Request::WriteSession` |

---

> **Nav:** [← 08. Linearizable Reads](./08-linearizable-reads.md) · [10. Membership and Leadership Transfer →](./10-membership-and-leadership-transfer.md) · [Index](./README.md)
