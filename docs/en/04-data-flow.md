# 04. Data Flow

> **Nav:** [← 03. Election Process](./03-election-process.md) · [05. Node Roles →](./05-node-roles.md) · [Index](./README.md)

This document answers: **when a client does put/get, how does the data travel from the "connected follower" to the "leader", and then into the Raft log, BitCask, and the Kv state machine.**  
Main source path: `raft_cli` → `net` → `raft_node` → `node.rs` → `log.rs` → `session.rs` / `kv.rs` → `bitcask.rs`.

---

## 0. Build a Mental Model First

There are three layers of "data" — do not mix them up:

| Layer | Contents | Location |
|------|------|------|
| **A. Client command** | `put b banana` | CLI memory → TCP |
| **B. Raft log** | `Entry{index, term, command bytes}` | `Key::Entry(i)` in BitCask on **every node** |
| **C. Application state** | `b → banana` | `Kv.data` (BTreeMap) in the process on **every node** |

The client thinks it is "writing to a database"; what Raft actually does is:

1. First turn the command into a **log entry** and replicate it to a quorum of nodes;  
2. Then **apply it in order** into the state machine;  
3. Reads read the state machine after the leader confirms its identity.

**The business string key `b` is not stored directly as a user key in BitCask** (in this teaching KV). BitCask mainly holds Raft metadata + Entry.

---

## 1. Scenario Setup

```text
Node 1 Follower  :7001
Node 2 Leader    :7002
Node 3 Follower  :7003

The user runs:
  raft-cli --peers 127.0.0.1:7003 put b banana
  raft-cli --peers 127.0.0.1:7001 get b
```

That is: **the write goes to follower node 3, and the read goes to follower node 1.**

---

## 2. Write Path Overview (Follower → Leader → Storage)

```text
┌─────────┐  WireMsg::Client   ┌──────────────┐
│ raft-cli│ ─────────────────► │ node3:7003   │  Follower
└─────────┘                    │ raft-node    │
                               └──────┬───────┘
                                      │ ① Inject local ClientRequest
                                      │ ② Discover it is a Follower
                                      │ ③ Forward ClientRequest to Leader=2
                                      ▼
                               ┌──────────────┐
                               │ node2:7002   │  Leader
                               │              │
                               │ ④ propose    │──► Log.append ──► BitCask set Entry
                               │ ⑤ Append ────┼──► node1, node3
                               │ ⑥ quorum ACK │
                               │ ⑦ commit     │──► BitCask set CommitIndex
                               │ ⑧ apply      │──► SessionState → Kv.data["b"]="banana"
                               │ ⑨ ClientResp │──► back to node3 ──► back to CLI
                               └──────────────┘
                                      │
                                      │ Append + later Heartbeat(commit_index)
                                      ▼
                               node1 / node3: splice log + apply → local also has b=banana
```

---

## 3. Write Path Step by Step

### Step ① CLI Encoding

Source: `src/bin/raft_cli.rs`

1. Read/write `.raft-cli-session`: obtain `client_id`, `seq = last_seq+1`  
2. `kv::encode(Command::Put { key: "b", value: "banana" })` → `cmd_bytes`  
3. Construct:

```text
Request::WriteSession {
  client_id: <uuid>,
  seq: N,
  command: cmd_bytes,   // bincode of the Put only, SESS not yet added
}
```

4. TCP connect to **only** `127.0.0.1:7003` (you only listed one peer)  
5. Send frame:

```text
WireMsg::Client {
  id: <connection-level uuid, used only to match replies on this TCP connection>,
  request: WriteSession { ... },
}
```

**The CLI does not care who the Leader is.**

---

### Step ② node3 Receives the Packet and Injects It into Raft

Source: `src/net/transport.rs` connection thread → `Inbound::Client`  
`src/bin/raft_node.rs` main loop:

```text
Receive Client { request, reply_channel }
  req_id = Uuid::new_v4()          // internal Raft RequestID, ≠ WireMsg.id
  client_replies[req_id] = reply_channel
  Envelope {
    from: 3,                       // must be this node's id
    to:   3,
    term: node.term(),
    message: ClientRequest { id: req_id, request: WriteSession{...} },
  }
  node = node.step(envelope)
```

**At this point the data is still the `Request` in node3's process memory; nothing has been written to BitCask.**

---

### Step ③ Follower Forwards to Leader (Follower → Leader)

Source: `Follower`'s handling of `ClientRequest` (`node.rs`)

```text
assert/check from == self.id
if role.leader == Some(2):
  forwarded.insert(req_id)
  send(to=2, message=the same ClientRequest)   // via node_tx → PeerOutbox → TCP WireMsg::Raft
if no leader:
  ClientResponse{ Abort } to self
```

On the wire, node3 → node2:

```text
Envelope {
  from: 3, to: 2, term: T,
  message: ClientRequest {
    id: req_id,
    request: WriteSession { client_id, seq, command: put_bytes },
  }
}
```

| Node | Role | What it does in this step | What it does not do |
|------|------|--------------|----------|
| 3 | Follower | Forwards; remembers `forwarded` | does **not** append to the log; does **not** modify Kv |
| 2 | Leader | Processes it in the next step | — |
| 1 | Follower | Not aware | — |

**This is how data goes from the follower to the leader: the whole ClientRequest is sent to the Leader over TCP inside a Raft Envelope.**

---

### Step ④ Leader propose: The Command Enters the Raft Log (→ BitCask)

Source: `Leader` handles `WriteSession` → `encode_session` → `propose`

#### 4.1 Session Wrapping

```text
log_command = b"SESS" ‖ bincode(SessionCommand {
  client_id,
  seq,
  payload: put_bytes,   // still the bincode of kv::Command::Put
})
```

#### 4.2 Log::append

```text
Entry {
  index: last_index + 1,   // e.g., 42
  term:  current leader term,    // e.g., 6
  command: Some(log_command),
  membership: None,
}
engine.set(Key::Entry(42).encode(), bincode(Entry))
if fsync: engine.flush()
```

**BitCask layer:**

1. Logical key: `[0x00] + 42u64.to_be_bytes()`  
2. Logical value: the bincode'd `Entry`  
3. Physically: **append** a record `key_len | value_len | key | value` at the **end of `data/node2/bitcask.log`**, and update the in-memory keydir  

```text
┌─────────────────────────────────────────────┐
│  data/node2/bitcask.log (appended)           │
│  ...old records...                           │
│  [new] key=Entry(42)  value=Entry struct     │
└─────────────────────────────────────────────┘
        ▲
        │ Log::append
        │
   Leader in-memory last_index=42
```

#### 4.3 Register the Client Wait

```text
writes[42] = Write { from: 3, id: req_id }
```

Meaning: once this log entry is committed and applied, the response must go back to **from=3** (the node that injected it), which then returns it to the CLI.

#### 4.4 Try to Replicate Immediately

For each peer, if `next_index == 42`, send `Append`.

---

### Step ⑤ Replicate to Followers (Leader → Follower)

#### 5.1 The Leader Sends

```text
Envelope {
  from: 2, to: 1, term: 6,
  message: Append {
    base_index: 41,
    base_term:  <the term of entry 41>,
    entries: [ Entry{index:42, term:6, command:Some(SESS|Put), ...} ],
  }
}
// also sent to to: 3
```

#### 5.2 Follower Handles Append

```text
if base matches the local log:
  log.splice(entries)     // writes the local BitCask Entry keys
  if it contains membership, switch configuration immediately
  reply AppendResponse { match_index: 42, reject_index: 0 }
otherwise:
  AppendResponse { match_index: 0, reject_index: ... }
```

| Node | BitCask | Kv.data |
|------|---------|---------|
| 2 Leader | Already has Entry(42) | has **not** applied yet (waiting for commit) |
| 1 Follower | Has Entry(42) after splice | not applied yet |
| 3 Follower | Has Entry(42) after splice | not applied yet |

**Note:** successful log replication ≠ client success; you still need **quorum + commit in this term + apply**.

---

### Step ⑥ Quorum Confirmation and Commit

The Leader receives `match_index >= 42` from at least a quorum of nodes (including itself), and `Entry(42).term == current term`:

```text
log.commit(42)
  → engine.set(Key::CommitIndex, bincode((42, 6)))
  // commit may skip fsync
```

Commit rule details: only entries created **in the current term** are committed directly (entries from older terms are committed indirectly via the noop).

---

### Step ⑦ apply: Log → State Machine (Still on the Leader)

```text
for entry in scan_apply(applied+1 ..= commit):
  SessionState::apply(entry)
    decode SESS → if seq was already executed, return the cached result
    otherwise Kv::apply
      data.insert("b", "banana")
      applied_index = 42
      return encode(Response::Put(42))
  look up writes[42] → ClientResponse {
    id: req_id,
    response: Ok(Response::Write(put_result_bytes))
  } send to from=3
```

**Leader in-memory state:**

```text
Kv.data = { ..., "b": "banana" }
```

**Leader BitCask:** still holds Entry/metadata; there is **no** separate `b` user key.

---

### Step ⑧ The Response Returns to the CLI

```text
node2 outbound Envelope to=3 ClientResponse
  → TCP → node3
node3 Follower:
  if id is in forwarded → send a ClientResponse to to=self again
node3 main loop:
  client_replies[req_id].send(Ok(Response::Write(...)))
  → connection thread WireMsg::ClientReply
  → CLI prints the Put index or its Display
```

---

### Step ⑨ When Does the Follower's State Machine Have Data?

Two paths:

1. **Heartbeat** carries a larger `commit_index`, and the follower's log already matches `last_index`  
   → `log.commit` + `maybe_apply` → the local `Kv` is also updated  
2. Any later path that calls `maybe_apply`  

Therefore:  
- The **log** reaches the follower at Append time;  
- The **Kv business state** reaches the follower only after the commit notification.  

A client `get` does not read the follower's local Kv; it goes through the leader read path instead (see the next section).

---

## 4. Read Path Overview (Follower → Leader → Read the State Machine)

```text
CLI get b → TCP → node1 (Follower)
  → ClientRequest{ Read(bincode(Get{b})) }
  → forward to Leader node2
  → Leader:
       read_seq += 1
       enqueue this read in the reads queue
       broadcast Message::Read { seq }   // or carry read_seq via Heartbeat
  → Follower replies ReadResponse / HeartbeatResponse
  → after quorum confirms read_seq:
       require commit_term == current term and applied has caught up
       Kv::read(Get{b}) → Some("banana")
  → ClientResponse back to node1 → CLI
```

| Aspect | Write | Read |
|------|----|----|
| Writes a Raft log entry | Yes | **No** |
| Modifies a BitCask Entry | Yes | No |
| Modifies Kv | On apply | **No** (read-only) |
| What quorum is used for | Committing the log | Confirming "I'm still the leader" |

---

## 5. Scan and Status

| Command | Path |
|------|------|
| `scan` | Same as read: `Request::Read(Scan)` → Leader `Kv::read` → encode and return the whole `BTreeMap` table |
| `status` | `Request::Status` → only the Leader fills `Status{leader,term,match_index,commit,applied,voters,storage}` |

A Follower that receives these also **forwards** them to the Leader.

---

## 6. Single-Node Simplified Path

When `peers: []`:

```text
CLI → node1(Leader)
  propose → append Entry → quorum=1 → commit+apply immediately
  → respond to CLI
```

No forwarding, no Append network traffic.

---

## 7. End-to-End Data Change Table (Writing `b=banana`)

| Time | node3 log | node2 log | node1 log | Kv on each node |
|------|------------|------------|------------|-----------|
| CLI just connected to 3 | Old | Old | Old | No b |
| After 3 forwards | Old | Old | Old | No b |
| After 2 appends | Old | **+Entry42** | Old | No b |
| After 1 and 3 ACK the append | **+Entry42** | Entry42 | **+Entry42** | No b |
| After 2 commits+applies | Entry42 | Entry42+commit | Entry42 | **2 has b** |
| After Heartbeat advances | commit+apply | same as left | commit+apply | **everyone has b** |
| CLI receives success | — | — | — | Success visible to the user |

---

## 8. Failures and Abort

| Case | Result |
|------|------|
| No Leader / election in progress | `Error::Abort`; the CLI switches peers and retries |
| Leader changed after forwarding | `abort_forwarded`, Abort |
| Write applied but the response was lost | Retry the same `WriteSession(client_id,seq)` → Session returns the cached result, no double write |
| Only connected to a dead peer | CLI times out and fails (recommend listing multiple addresses in `--peers`) |

---

## 9. Source Anchors

| Step | Location |
|------|------|
| CLI put | `src/bin/raft_cli.rs` |
| TCP Client | `src/net/transport.rs` `run_client_request` / `handle_conn` |
| Injection | `src/bin/raft_node.rs` |
| Forwarding | `Follower` `ClientRequest` branch |
| Writing the log | `Leader` `propose` / `Log::append` |
| Replication | `maybe_send_append` / Follower `Append` |
| Commit/apply | `maybe_commit_and_apply` |
| Session/Kv | `session.rs` / `kv.rs` |
| Read | `Leader` `Read` + `maybe_read` |

---

> **Nav:** [← 03. Election Process](./03-election-process.md) · [05. Node Roles →](./05-node-roles.md) · [Index](./README.md)
