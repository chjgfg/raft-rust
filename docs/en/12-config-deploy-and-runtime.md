# 12. Config, Deploy, and Runtime

> **Nav:** [← 11. Snapshots and Catch-up](./11-snapshots-and-catch-up.md) · [13. Fault Scenarios and Tests →](./13-fault-scenarios-and-tests.md) · [Index](./README.md)

This document explains the **YAML config**, how to start **single / multi-process**, and the **internal loops of the `raft-node` / `raft-cli` processes**. Source: `src/config.rs`, `src/bin/raft_node.rs`, `src/bin/raft_cli.rs`, `src/net/`.

---

## 1. Config file shape

One YAML per **OS process** (not one file managing the whole cluster).

### 1.1 Fields

| Field | Meaning |
|------|------|
| `node.id` | `NodeID` (u8), unique within the cluster |
| `node.listen` | local `ip:port`, receives peer + CLI |
| `node.data_dir` | data directory; the BitCask file is `{data_dir}/bitcask.log` |
| `peers` | `[{id, addr}, ...]`; **empty = single node** |
| `options.*` | see the table below |

### 1.2 options

| Key | Meaning | Common value |
|----|------|--------|
| `heartbeat_interval` | heartbeat interval (tick) | 2 |
| `election_timeout_min/max` | election timeout random range | 5 / 10 |
| `max_append_entries` | max entries per Append | 100 |
| `pre_vote` | pre-vote | true |
| `check_quorum` | leader steps down when losing quorum | true |
| `snapshot_threshold` | how many applies before compact; 0 disables | 1000 |

A logic tick ≈ 100ms (`TICK_INTERVAL`).

### 1.3 Example paths

| File | Purpose |
|------|------|
| `config/single.yaml` | `peers: []`, becomes Leader immediately |
| `config/node1.yaml` … `node3.yaml` | three nodes pointing at each other |

---

## 2. Deployment modes

### 2.1 Single node

```bash
cargo run --bin raft-node -- --config config/single.yaml
# or default-run
cargo run -- --config config/single.yaml

cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a 1
```

### 2.2 Three nodes

Three terminals:

```bash
cargo run --bin raft-node -- --config config/node1.yaml
cargo run --bin raft-node -- --config config/node2.yaml
cargo run --bin raft-node -- --config config/node3.yaml
```

It is recommended to write the full peer list in the CLI so you can switch nodes on Abort:

```bash
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 status
```

You can also connect to just one address (followers will forward).

---

## 3. raft-node runtime

### 3.1 Startup

```text
1. parse --config
2. NodeFileConfig::load
3. create_dir_all(data_dir)
4. BitCask::new(data_dir/bitcask.log)
5. read SnapshotData; restore state machine (if any)
6. Log::new(engine)
7. SessionState::new(Kv::new())
8. Node::new(id, peer_ids, log, state, node_tx, opts)
9. spawn_listener(listen) → Inbound channel
10. PeerOutbox(peers address table)
11. enter the select! loop
```

### 3.2 Main loop

```text
select!
  tick:
    node = node.tick()
      → election timeout / heartbeat / CheckQuorum

  inbound:
    Raft(env) → node.step(env)
    Client { request, reply } →
      generate RequestID
      client_replies[id] = reply
      step( ClientRequest{ from=to=self, request } )

  node_rx (protocol outbound Envelope):
    to == self && ClientResponse → complete the reply channel → TCP ClientReply
    otherwise PeerOutbox.send_raft → WireMsg::Raft
```

### 3.3 Thread model (simplified)

| Thread | Work |
|------|------|
| Main thread | tick / step / outbound routing |
| accept thread | accept TCP |
| One thread per connection | read frames; Client requests wait synchronously for the reply before writing back |

Outbound to peer: lazy connection + write timeout + on failure clear the connection and retry once.

---

## 4. raft-cli runtime

```text
parse --peers / --client-id / subcommand
SessionStore::load(.raft-cli-session)   // next_seq + save on put
loop over the peers list:
  TCP connect → WireMsg::Client → wait for ClientReply
Abort/IO → switch to next peer / next round (at most ~40 rounds)
```

| Command | Request |
|------|---------|
| put | `WriteSession` |
| get / scan | `Read` |
| status | `Status` |
| members | `ChangeMembership` |

The session file path = **CLI process cwd** + `.raft-cli-session` (not data_dir).

---

## 5. In-process cluster (for tests)

`src/cluster/`: does **not** go over TCP, uses crossbeam + optional partition/packet drop.

| API | Purpose |
|-----|------|
| `Cluster::spawn` | multi-threaded nodes |
| `client()` | locally inject Request |
| `stop` / `start` | simulate crash / new node |
| `partition_*` / `set_drop_rate` | fault injection |

Separated from the production deployment path; heavily used by `cargo test`. See [13](./13-fault-scenarios-and-tests.md).

---

## 6. Network frame (recap)

```text
u32 LE len ‖ bincode(WireMsg::{Raft, Client, ClientReply})
```

The same `listen` port serves **peer protocol** and **CLI** at once.

---

## 7. Source anchors

| Topic | Path |
|------|------|
| Config | `src/config.rs`, `config/*.yaml` |
| Node process | `src/bin/raft_node.rs` |
| CLI | `src/bin/raft_cli.rs` |
| TCP | `src/net/codec.rs`, `transport.rs` |
| In-process | `src/cluster/mod.rs` |

---

> **Nav:** [← 11. Snapshots and Catch-up](./11-snapshots-and-catch-up.md) · [13. Fault Scenarios and Tests →](./13-fault-scenarios-and-tests.md) · [Index](./README.md)
