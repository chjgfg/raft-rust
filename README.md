# raft-rust

Standalone **Raft consensus** library extracted from
[erikgrinaker/toydb](https://github.com/erikgrinaker/toydb).

The original Raft module is educational and closely follows the
[Raft paper](https://raft.github.io/raft.pdf). This crate peels it out of toyDB
so you can embed it in your own project with a simple in-memory store
(HashMap / BTreeMap) or your own storage engine.

## Features

| Piece | What you get |
|---|---|
| Consensus | Leader election, log replication, linearizable reads (quorum confirm) |
| Log storage | Pluggable `storage::Engine`; ships with `storage::Memory` (BTreeMap) |
| State machine | Pluggable `raft::State` (apply / read arbitrary `Vec<u8>` commands) |
| Transport | **Not included** — you deliver `Envelope` messages yourself |

### Not included (same caveats as toyDB)

No membership changes, snapshots, log truncation, leader leases, pre-vote, or
automatic client retries. Fine for learning and prototypes; not a production
Raft.

## Quick start

```bash
# Run the 3-node in-process demo (Memory storage + channel transport)
cargo run --example kv_cluster
```

### Embed in your code

```rust
use std::collections::HashSet;
use crossbeam::channel;
use raft_rust::raft::{Log, Node, Options, State};
use raft_rust::storage::Memory;

// 1. Storage for the Raft log (in-memory BTreeMap — swap for your own Engine)
let log = Log::new(Box::new(Memory::new()))?;

// 2. Your deterministic state machine
let state: Box<dyn State> = Box::new(MyState::new());

// 3. Outbound message channel — deliver these Envelopes to peers / clients
let (tx, rx) = channel::unbounded();

// 4. Create the node (single-node cluster becomes leader immediately)
let peers: HashSet<_> = [2, 3].into_iter().collect();
let mut node = Node::new(1, peers, log, state, tx, Options::default())?;

// 5. Drive the node
// node = node.tick()?;           // advance logical time (~every 100ms)
// node = node.step(envelope)?;   // process an inbound message
```

Implement `raft::State` for your app:

```rust
use raft_rust::raft::{Entry, Index, State};
use raft_rust::error::Result;

struct MyState { applied: Index /* + your data */ }

impl State for MyState {
    fn get_applied_index(&self) -> Index { self.applied }

    fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        // entry.command is None for Raft no-ops (leader election)
        // otherwise decode & apply deterministically on every node
        self.applied = entry.index;
        Ok(vec![])
    }

    fn read(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        // local read on the leader only — must not mutate state
        Ok(vec![])
    }
}
```

Implement `storage::Engine` for durable storage (BitCask-style, RocksDB, …).
`Memory` is enough to experiment:

```rust
use raft_rust::storage::{Engine, Memory};
let engine = Memory::new(); // BTreeMap<Vec<u8>, Vec<u8>>
```

## Crate layout

```
src/
  lib.rs            public re-exports
  error.rs          Error / Result
  raft/
    mod.rs          protocol docs + constants
    node.rs         Follower / Candidate / Leader state machine
    log.rs          replicated log on top of Engine
    message.rs      Envelope, Message, Request, Response
    state.rs        State trait
  storage/
    engine.rs       Engine trait (ordered KV)
    memory.rs       in-memory BTreeMap engine
  encoding/
    bincode.rs      value encoding
    keycode.rs      order-preserving key encoding
examples/
  kv_cluster.rs     3-node cluster demo
```

## Driving the node

Raft is **synchronous and single-threaded** inside a node:

1. Call `tick()` roughly every `TICK_INTERVAL` (100 ms) — heartbeats & elections.
2. Call `step(envelope)` for every inbound peer/client message.
3. Read outbound messages from the `Sender<Envelope>` you passed to `Node::new`
   and deliver them (TCP, channels, …). `ClientResponse` goes back to clients;
   everything else goes to the peer in `envelope.to`.

See `examples/kv_cluster.rs` for a complete router.

## License

Apache-2.0 (same as toyDB). Core Raft logic is Erik Grinaker's work from toyDB;
this repository only reorganizes it as a standalone library.
