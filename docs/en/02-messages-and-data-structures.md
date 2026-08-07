# 02. Messages and Data Structures

> **Nav:** [← 01. Architecture Overview](./01-architecture-overview.md) · [03. Election Process →](./03-election-process.md) · [Index](./README.md)

This document lists the main types that appear **on the wire** and **inside a node**, so you can cross-reference them against the flow diagrams in 03/04. Source: `src/raft/message.rs`, `log.rs`, `kv.rs`, `session.rs`, `net/codec.rs`.

---

## 1. TCP Frames and WireMsg

### 1.1 Frame Format

```text
┌─────────────┬────────────────────────┐
│u32 LE length│ bincode(WireMsg)  body │
└─────────────┴────────────────────────┘
```

Length = number of bytes in the body; upper limit 64MiB.

### 1.2 WireMsg

```rust
enum WireMsg {
    Raft(Envelope),                              // inter-node protocol
    Client { id: Uuid, request: Request },       // CLI → node
    ClientReply { id: Uuid, response: Result<Response, Error> },
}
```

| Variant | Direction | Purpose |
|------|------|------|
| `Raft` | peer ↔ peer | Election, replication, heartbeat, snapshot |
| `Client` | cli → node | Business request; `id` is only relevant to this connection |
| `ClientReply` | node → cli | Matches `Client.id` |

**Note:** `WireMsg::Client`'s `id` is **not the same** as the `RequestID` inside Raft's `Message::ClientRequest`. The node process generates another `Uuid` as the internal `RequestID`.

---

## 2. Envelope (Protocol Envelope)

```rust
struct Envelope {
    from: NodeID,   // u8
    term: Term,     // u64, the sender's current term (the intended term during PreCampaign)
    to: NodeID,
    message: Message,
}
```

Rules (implementation side):

- `to` must be this node, otherwise assert/drop  
- Unknown `from` (not in the current member set) → drop and warn  
- Messages with an older `term` are usually dropped; a higher `term` makes the node fall back to Follower and update its term  

---

## 3. Message (Inter-node Messages)

| Variant | Sent by | Handled by | Persists vote? |
|------|------|--------|--------------|
| `PreCampaign { last_index, last_term }` | Candidate(PreVote) | Follower/Candidate | **No** |
| `PreCampaignResponse { vote }` | Voter | Candidate(PreVote) | **No** |
| `Campaign { last_index, last_term }` | Candidate(Election) | Follower | **Yes** (if granted) |
| `CampaignResponse { vote }` | Voter | Candidate(Election) | Already persisted when voting |
| `Heartbeat { last_index, commit_index, read_seq }` | Leader | Follower | No |
| `HeartbeatResponse { match_index, read_seq }` | Follower | Leader | No |
| `Append { base_index, base_term, entries }` | Leader | Follower | Entries written to the local log |
| `AppendResponse { match_index, reject_index }` | Follower | Leader | No |
| `Read { seq }` | Leader | Follower | No |
| `ReadResponse { seq }` | Follower | Leader | No |
| `ClientRequest { id, request }` | Injected locally or forwarded | Leader/Follower | See request |
| `ClientResponse { id, response }` | Leader or forwarding return trip | Local event loop | No |
| `InstallSnapshot { ... }` | Leader | Follower | Resets log/state machine |
| `InstallSnapshotResponse { last_included_index }` | Follower | Leader | No |

### Append and Probing

- `entries` non-empty: real replication  
- `entries` empty: probe the common `base` (`next_index` regresses on log divergence)  
- On rejection: `reject_index != 0`, `match_index == 0`  
- On acceptance: `match_index` is the highest matched index, `reject_index == 0`  

---

## 4. Request / Response (Client Semantics)

### Request

| Variant | Meaning | Enters the Raft log? |
|------|------|------------------|
| `Read(Vec<u8>)` | State-machine read command (bincode of `kv::Command`) | **No** |
| `Write(Vec<u8>)` | Write without a session | **Yes** |
| `WriteSession { client_id, seq, command }` | Idempotent write | **Yes** (wraps SESS around the command) |
| `Status` | Cluster status | No |
| `ChangeMembership { voters }` | Membership change | **Yes** (membership field) |

### Response

| Variant | Contents |
|------|------|
| `Read(Vec<u8>)` | bincode of `kv::Response` |
| `Write(Vec<u8>)` | Same (e.g., `Put(index)`) |
| `Status(Status)` | leader/term/commit/match_index/voters… |
| `ChangeMembership { index }` | The proposed log index (usually a Joint entry) |

Errors: the `Err` of `Result`; commonly `Error::Abort` (no leader, switching roles) → the client retries.

---

## 5. Entry (Raft Log Entry)

```rust
struct Entry {
    index: Index,                      // starting from 1
    term: Term,
    command: Option<Vec<u8>>,          // None = noop or membership-change placeholder
    membership: Option<MembershipEntry>, // mutually exclusive with command
}
```

| Scenario | command | membership |
|------|---------|------------|
| Leader's noop on election | `None` | `None` |
| Business Put | `Some(encoded command or SESS+command)` | `None` |
| Membership change | `None` (treated as noop to advance applied at apply time) | `Some(Joint/Simple)` |

---

## 6. Log Keys on the Engine (Summary)

See [06](./06-bitcask-and-log-keyspace.md).

| Key | Encoding prefix | Value |
|-----|----------|-----|
| `Entry(i)` | `0x00 ‖ i.to_be_bytes()` | bincode(Entry) |
| `TermVote` | `0x01` | bincode((term, Option\<NodeID\>)) |
| `CommitIndex` | `0x02` | bincode((index, term)) |
| `SnapshotMeta` | `0x03` | bincode((last_included_index, term)) |
| `SnapshotData` | `0x04` | State-machine snapshot bytes |

---

## 7. Application-level Kv Commands

```rust
enum kv::Command {
    Get { key: String },
    Put { key: String, value: String },
    Scan,
}
enum kv::Response {
    Get(Option<String>),
    Put(Index),      // applied log index
    Scan(BTreeMap<String, String>),
}
```

When a Session writes to the log:

```text
b"SESS" ‖ bincode(SessionCommand { client_id, seq, payload })
```

`payload` contains the bincode of `kv::Command::Put`.

---

## 8. Internal Tracking Structures in a Node (Not on the Wire)

| Structure | Role | Purpose |
|------|------|------|
| `Follower.forwarded` | Follower | Forwarded RequestIDs, for return routing and Abort |
| `Candidate.votes` / `phase` | Candidate | Set of pre-vote or real-vote ballots |
| `Leader.progress` | Leader | Per-peer match/next/read_seq |
| `Leader.writes` | Leader | Log index → clients awaiting a response |
| `Leader.reads` | Leader | Read queue awaiting quorum confirmation |
| `Leader.peer_seen` | Leader | CheckQuorum liveness |

---

> **Nav:** [← 01. Architecture Overview](./01-architecture-overview.md) · [03. Election Process →](./03-election-process.md) · [Index](./README.md)
