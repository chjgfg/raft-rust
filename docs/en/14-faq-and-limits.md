# 14. FAQ and Limits

> **Nav:** [← 13. Fault Scenarios and Tests](./13-fault-scenarios-and-tests.md) · [Index](./README.md)

---

## 1. Is this production-grade Raft?

**No.** It is for learning and local prototyping. It lacks TLS, an operations surface, chunked snapshots, mature flow control, and auditing.

---

## 2. Where is business data stored on disk?

- **Raft log / term / commit / snapshot metadata and snapshot body**: `{data_dir}/bitcask.log` (BitCask)
- **String KV table**: in-process `Kv` in memory; recovered via log replay or `SnapshotData`

It does **not** store `put b banana` as a BitCask user key `"b"`. See [04](./04-data-flow.md), [06](./06-bitcask-and-log-keyspace.md).

---

## 3. Why can I put/get even when connected to a follower?

The Follower **forwards** `ClientRequest` to the Leader; only the Leader actually writes the log and performs linearizable reads. See [04](./04-data-flow.md).

---

## 4. What is `.raft-cli-session`? Where is it?

The CLI's **client_id + last_seq**, used for write idempotency. Path = `.raft-cli-session` under the **cwd where the CLI runs**.  
Only **`put`** creates/updates it; just starting a node or only running `status` **won't**. See [09](./09-session-and-cli-idempotency.md).

---

## 5. What is Abort?

`Error::Abort`: no leader, in election, leadership change, forwarding cancelled, etc.  
It is **not** a business error. The CLI switches peer and retries; a put with a session retries the same seq.

---

## 6. How do I switch between single node and three nodes?

- Single node: `peers: []` (`config/single.yaml`)
- Three nodes: three YAML files pointing at each other, start three `raft-node`s

You can't "conjure up" the other two processes by only changing one config file.

---

## 7. How do I add a fourth node?

1. write `node4.yaml` and start the process (empty data, or catch up from a snapshot)
2. CLI: `members 1,2,3,4`
3. wait for Joint→Simple; the new node catches up on log/snapshot

See [10](./10-membership-and-leadership-transfer.md), [11](./11-snapshots-and-catch-up.md).

---

## 8. How do I remove the current leader?

The `members` target set **does not include** the current leader → after Simple is committed the old leader steps down, and the remaining nodes elect a new one.  
There is no separate TransferLeadership RPC.

---

## 9. Is there TLS?

**No.** Plaintext TCP. In production you can do TLS termination in front, or wrap it yourself.

---

## 10. Are snapshots chunked?

**No.** `InstallSnapshot` carries the full `data` in one go.

---

## 11. How does the in-process `cluster` differ from multi-process?

| | `src/cluster` | `raft-node`×N |
|--|---------------|----------------|
| Transport | in-memory channel | TCP |
| Purpose | unit tests / fault injection | real multi-process |
| Config | node id list in code | YAML per process |

---

## 12. How do I use it as an embedded library?

```rust
// in-process test cluster
use raft_rust::cluster::{Cluster, wait_for_leader};
let c = Cluster::spawn(&[1,2,3]);
let mut client = c.client();
wait_for_leader(&mut client)?;
client.put("k","v")?;

// or build your own Node + manage the transport yourself
// Node::new / step / tick + Sender<Envelope>
```

Implementing your own `State` / `Engine` can swap the state machine and storage.

---

## 13. Docs conflict with code?

**The code wins.**

---

## 14. Deliberately not implemented capabilities

- leader lease reads
- complex sessions beyond the client library's infinite retry (only the CLI file session)
- full observability (metrics/tracing system)
- secure cross-machine communication

---

## 15. Recommended reading order (full text)

```text
01 architecture → 02 messages → 03 election → 04 data access
05 roles → 06 BitCask
07 replication conflicts → 08 linearizable reads → 09 Session
10 membership change → 11 snapshots
12 deploy/runtime → 13 fault tests → 14 FAQ
```

---

> **Nav:** [← 13. Fault Scenarios and Tests](./13-fault-scenarios-and-tests.md) · [Index](./README.md)
