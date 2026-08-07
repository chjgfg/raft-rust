# raft-rust

> **For learning only.** This project is a teaching implementation of Raft consensus plus an in-process / multi-process KV demo. It is **not** a production-grade coordination service.

Standalone **Raft** implementation in Rust:  
**election → log replication → commit → state machine apply**, persisted by **BitCask**; deployed as **one YAML config + one `raft-node` process + TCP** per node.

---

## Documentation

| Language | Entry |
|----------|--------|
| **English topics** | **[en/README.md](./docs/en/README.md)** — numbered learning path 01–14 |
| **Chinese topics** | **[zh/README.md](./docs/zh/README.md)** — numbered learning path 01–14 |
| Chinese project overview | **[docs/README.md](./docs/README.md)** — Chinese twin of this page |

Start here if you want the full story in English:

1. [01 Architecture overview](./docs/en/01-architecture-overview.md)  
2. [04 Data flow end to end](./docs/en/04-data-flow.md)  
3. Then pick topic docs (election, replication, snapshots, …) from the [English index](./docs/en/README.md).

---

## Features

| Capability | Notes |
|---|---|
| Consensus | Leader election, log replication, majority commit |
| Reads | Leader linearizable reads (majority confirmation) |
| Pre-vote / CheckQuorum | Toggleable via `Options`; reduces disruption from partitions |
| Membership change | Joint consensus |
| Leadership transfer | `ChangeMembership` can remove the current leader; the old leader steps down after the `Simple` entry commits |
| Write dedup | `WriteSession(client_id, seq)` + `SessionState` |
| Snapshots | `snapshot` / `InstallSnapshot` / log `compact_to` |
| Storage | **BitCask** log-structured engine (`data_dir/bitcask.log`) |
| Network | TCP framing: `WireMsg::{Raft, Client, ClientReply}` |
| In-process cluster for tests | `cluster::Cluster` (channels + fault injection) |

### Intentional limits (so the code stays teachable)

- Plain-text TCP, **no TLS / auth** (put TLS termination in front in production)  
- Snapshots sent whole, not chunked/streamed  
- No automatic graceful leader-migration RPC (removing the leader via membership change is how step-down happens)  
- No production-grade ops (monitoring, live config hot-reload, …)  

More Q&A: [English FAQ](./docs/en/14-faq-and-limits.md) · [中文 FAQ](./docs/zh/14-FAQ与能力边界.md).

---

## Dependencies

- Rust (edition 2024)
- Build: `cargo build --bins`

---

## Single node (fastest start)

With **`peers: []`** in the config (or no peers), the process **becomes Leader immediately on startup**.

```bash
# Terminal 1: node
cargo run --bin raft-node -- --config config/single.yaml

# Terminal 2: CLI (connect to a single address)
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 status
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 get a
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 scan
```

`config/single.yaml`:

```yaml
node:
  id: 1
  listen: "127.0.0.1:7001"
  data_dir: "data/single"

peers: []

options:
  heartbeat_interval: 2
  election_timeout_min: 5
  election_timeout_max: 10
  max_append_entries: 100
  pre_vote: true
  check_quorum: true
  snapshot_threshold: 1000
```

Data directory: `data/single/bitcask.log` (`data/` is already git-ignored).

You can also run `cargo run -- --config config/single.yaml` (`default-run = raft-node`).

---

## Multi-node (one config per node)

Three configs, three processes, e.g. `config/node1.yaml` / `node2.yaml` / `node3.yaml`.

```bash
# Start one process per terminal
cargo run --bin raft-node -- --config config/node1.yaml
cargo run --bin raft-node -- --config config/node2.yaml
cargo run --bin raft-node -- --config config/node3.yaml

# CLI: pass multiple peers; retries another node on failure/Abort
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 status
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put b banana
cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 get b
```

`config/node1.yaml` example:

```yaml
node:
  id: 1
  listen: "127.0.0.1:7001"
  data_dir: "data/node1"

peers:
  - { id: 2, addr: "127.0.0.1:7002" }
  - { id: 3, addr: "127.0.0.1:7003" }

options:
  heartbeat_interval: 2
  election_timeout_min: 5
  election_timeout_max: 10
  max_append_entries: 100
  pre_vote: true
  check_quorum: true
  snapshot_threshold: 1000
```

Nodes 2 and 3 are the same idea: change `id` / `listen` / `data_dir`, and list the other two addresses in `peers`.

---

## raft-cli commands

```text
raft-cli --peers host:port[,host:port...] [--client-id UUID] <command>
```

| Command | Description |
|---|---|
| `put <key> <value>` | Session-bearing write (idempotent sequence number) |
| `get <key>` | Linearizable read |
| `scan` | Scan all KV entries |
| `status` | Leader, term, commit, voters, etc. |
| `members <id,id,...>` | Membership change (target voting set) |

### Client session file

- Path: `.raft-cli-session` in the **current working directory when the CLI runs**
- Contents: `client_id` + `last_seq` (incremented on every `put`)
- Purpose: retrying the same write after a timeout sends the same `(client_id, seq)`, avoiding double execution
- Override the id with `--client-id <uuid>`; the file is git-ignored

This is not cluster config and not node data; deleting it just makes the CLI renumber as a new client.

---

## Config fields

| Field | Meaning |
|---|---|
| `node.id` | Node ID (`u8`, unique in the cluster) |
| `node.listen` | Local `ip:port` to listen on |
| `node.data_dir` | Data directory (BitCask + snapshot metadata) |
| `peers` | Other nodes `{ id, addr }`; **empty = single node** |
| `options.heartbeat_interval` | Heartbeat interval (ticks) |
| `options.election_timeout_min/max` | Election timeout range (ticks) |
| `options.max_append_entries` | Max entries per Append |
| `options.pre_vote` | Whether to use Pre-vote |
| `options.check_quorum` | Leader steps down if it loses a quorum |
| `options.snapshot_threshold` | Compact after this many applies since last snapshot; `0` disables |

Logical time: `TICK_INTERVAL` = 100ms.

---

## Tests

```bash
cargo build --bins    # true multi-process tests need the raft-node binary
cargo test
```

Coverage includes:

- In-process: election, forwarding, partition, leader kill, drop/reorder, concurrent clients  
- Pre-vote / CheckQuorum  
- Membership change, **leader removal (leadership transfer)**  
- Session dedup, BitCask  
- **Restart recovery**, **restart after snapshot compaction**  
- **True multi-process** (`tests/multi_process.rs` spawns child processes)  

In-process demo (not a deployment path):

```bash
cargo run --example kv_cluster
```

---

## Embed as a library (optional)

```rust
use raft_rust::cluster::{wait_for_leader, Cluster};

let cluster = Cluster::spawn(&[1, 2, 3]); // in-process cluster for tests
let mut client = cluster.client();
wait_for_leader(&mut client)?;
client.put("k", "v")?;
```

Or use `Node` directly with your own transport (see `src/lib.rs` / `src/net`).

---

## Project layout

```text
src/
  bin/raft_node.rs     node process (one member of a single- or multi-node cluster)
  bin/raft_cli.rs      command-line client
  net/                 TCP + bincode
  cluster/             in-process cluster (tests / example)
  config.rs            node YAML loading
  raft/                protocol: node / log / membership / session / kv
  storage/             Engine + BitCask
config/
  single.yaml          single node
  node1.yaml … node3.yaml
tests/                 unit and integration tests
examples/kv_cluster.rs in-process three-node demo
```

---

## Known limitations

- Plain-text TCP, **no TLS / auth** (terminate TLS in front in production)  
- Snapshots sent whole, not chunked/streamed  
- No automatic graceful leader-migration RPC (removing the old leader via membership change is how step-down happens)  
- Non-production-grade ops (monitoring, live config hot-reload, etc. not done)  

---

## License

Apache-2.0. The implementation follows the [Raft paper](https://raft.github.io/raft.pdf) closely, with extensions for membership change, Pre-vote, snapshots, and multi-process deployment.
