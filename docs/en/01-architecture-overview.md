# 01. Architecture Overview

> **Nav:** [Documentation Index](./README.md) · [02. Messages and Data Structures →](./02-messages-and-data-structures.md) · [Index](./README.md)

This document describes raft-rust's process layout, software layering, and the overall path of "client → any node → leader → storage". Elections and data access are covered in more detail in 03 and 04.

---

## 1. One-Sentence Positioning

**A teaching-oriented Raft + string KV, deployable as multiple processes.**

- Protocol core: `Node::step` / `Node::tick` (`src/raft/node.rs`)
- Persistence: `Log` writes term/vote/entries into `storage::Engine` (default: **BitCask**)
- Application state: `SessionState` wraps `Kv` (an in-memory `BTreeMap`, snapshot-capable)
- Deployment: one node per OS process (`raft-node`), TCP between nodes; client `raft-cli`

No production-grade TLS, no chunked snapshot streaming, no full operations surface.

---

## 2. Processes and Roles

### 2.1 Deployment Processes

```text
┌─────────────┐     TCP WireMsg      ┌─────────────┐
│ raft-node 1 │◄───────────────────►│ raft-node 2 │
│ :7001       │                      │ :7002       │
│ data/node1  │                      │ data/node2  │
└──────▲──────┘                      └──────▲──────┘
       │ Client / ClientReply                │
       └──────────── raft-cli ───────────────┘
                         │
                    (can also connect to node3)
```

| Process | Responsibility |
|------|------|
| `raft-node` | Loads YAML, opens BitCask, listens on TCP, drives `tick`/`step`, forwards peer messages |
| `raft-cli` | Encodes put/get, maintains the local `.raft-cli-session`, retries against the peers list |

Single node: `peers: []`; it becomes the **Leader** directly at `Node::new`.

### 2.2 Raft Roles (Logical)

Within the same physical process, the role is one variant of the `Node` enum:

| Role | When it appears | Serves external clients? |
|------|----------|------------------|
| **Follower** | Default; follows while there is a leader | Only **forwards** ClientRequest, does not write the log locally |
| **Candidate** | Election timeout; two phases: PreVote / Election | **Aborts** ClientRequest outright |
| **Leader** | Wins the real election with quorum votes | The **only** one that performs Write replication and linearizable Read |

---

## 3. Software Layering

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│  Entry                                                                           │
│  · raft-cli          src/bin/raft_cli.rs                                         │
│  · raft-node         src/bin/raft_node.rs                                        │
│  · In-process test cluster   src/cluster/                                        │
└─────────────────────────────────────────┬────────────────────────────────────────┘
                                          │
┌──────────────────────────────────────────────────────────────────────────────────┐
│  Network                                                                         │
│  · Frame: u32 LE length + bincode(WireMsg)   src/net/codec.rs                    │
│  · Listener / PeerOutbox                    src/net/transport                    │
└─────────────────────────────────────────┬────────────────────────────────────────┘
                                          │ Envelope / Client injection
┌──────────────────────────────────────────────────────────────────────────────────┐
│  Raft core  src/raft/node.rs                                                     │
│  · Follower / Candidate / Leader                                                 │
│  · Election, replication, commit, read confirmation, membership change, snapshot │
└──────────────────────┬─────────────────────────────────────┬─────────────────────┘
                       │                                     │
                       ▼                                     ▼
┌─────────────────────────┐           ┌─────────────────────────┐
│  Log  src/raft/log.rs   │           │  State                  │
│  Entry / TermVote /     │           │  SessionState → Kv      │
│  CommitIndex / Snapshot*│           │  apply / read / snapshot│
└─────────────┬────────────┘           └─────────────────────────┘
                       │
                       ▼
      ┌─────────────────────────────────┐
      │  storage::Engine                │
      │  BitCask  data_dir/bitcask.log  │
      └─────────────────────────────────┘
```

**Key decoupling:**

- The Raft log ≠ the application KV table. The log lives in BitCask; `Kv.data` lives in process memory and is restored via apply / snapshot.
- Client byte commands enter through `Request`; what the log stores is `Entry.command` (possibly wrapped in a Session).

---

## 4. Two Kinds of "Log" — Don't Mix Them Up

| Name | What it is | Where it lives |
|------|--------|------|
| **Raft log** | The consensus-replicated command sequence `Entry` | BitCask key `Key::Entry(index)` |
| **BitCask file log** | The storage engine's append-only file | `data_dir/bitcask.log`, appended to as a whole file |

`Log::append` calls `engine.set(Entry key, bincode(Entry))`, so **one Raft entry = one set on BitCask (possibly appending another segment to the file)**.

After an application-level `put a=apple` succeeds:

1. The Raft log gains one Put command;  
2. **A quorum of nodes** have `a→apple` in their `Kv.data`;  
3. BitCask holds mostly Raft metadata and entries, **not** business strings stored directly under the `a` key (business data lives in the state machine's memory).

---

## 5. Time and Driving

| Concept | Meaning |
|------|------|
| `TICK_INTERVAL` | 100ms, one logical tick |
| `tick()` | Advances election timeout / heartbeat / CheckQuorum |
| `step(Envelope)` | Handles one inbound message (the role may change) |

`raft-node` main loop (simplified):

```text
select!
  tick        → node = node.tick()
  inbound     → Raft: step(env)  /  Client: wrap into ClientRequest then step
  node_rx outbound → if to==self, reply to CLI; otherwise PeerOutbox.send_raft
```

---

## 6. Relationship to the Later Documents

| Question | Where to read |
|------|--------|
| What are the message fields | [02](./02-messages-and-data-structures.md) |
| Who moves first in an election, how term changes | [03](./03-election-process.md) |
| put/get step-by-step down to disk | [04](./04-data-flow.md) |
| The list of role prohibitions | [05](./05-node-roles.md) |
| Key encoding and BitCask layout | [06](./06-bitcask-and-log-keyspace.md) |

---

> **Nav:** [Documentation Index](./README.md) · [02. Messages and Data Structures →](./02-messages-and-data-structures.md) · [Index](./README.md)
