# raft-rust documentation index (English)

> **Language:** [English topic index (this page)](./README.md) · [中文专题索引](../zh/README.md) · [Project README (EN)](../../README.md) · [项目说明（中文）](../README.md)

This directory is the **English topic documentation** (01–14). Standalone Raft implementation — **one YAML config + one `raft-node` process + TCP** per node, persisted by BitCask.

**Read files in order by the `01` → `14` filename prefix.**

Each document has **Nav** under the title and at the end: `← previous · next → · Index`.

Writing style: explain **business meaning** and **data flow**, not just type names.

---

## Read in order

| # | Document | One line |
|---|----------|----------|
| 01 | [Architecture Overview](./01-architecture-overview.md) | Processes, roles, layering |
| 02 | [Messages and Data Structures](./02-messages-and-data-structures.md) | Envelope / Message / Request / Entry / framing |
| 03 | [Election Process](./03-election-process.md) | Pre-vote → real election → win; how the node changes |
| 04 | [Data Flow](./04-data-flow.md) | Follower → leader → log → Kv |
| 05 | [Node Roles](./05-node-roles.md) | What Follower / Candidate / Leader each do |
| 06 | [BitCask and Log Keyspace](./06-bitcask-and-log-keyspace.md) | What keys/values look like on disk |
| 07 | [Log Replication and Conflicts](./07-log-replication-and-conflicts.md) | Append / reject / probe / splice |
| 08 | [Linearizable Reads](./08-linearizable-reads.md) | read_seq quorum confirmation and gates |
| 09 | [Session and CLI Idempotency](./09-session-and-cli-idempotency.md) | client_id / seq / `.raft-cli-session` |
| 10 | [Membership and Leadership Transfer](./10-membership-and-leadership-transfer.md) | Joint → Simple; removing the leader steps it down |
| 11 | [Snapshots and Catch-up](./11-snapshots-and-catch-up.md) | compact, InstallSnapshot |
| 12 | [Config, Deploy, and Runtime](./12-config-deploy-and-runtime.md) | YAML, raft-node/cli main loops |
| 13 | [Fault Scenarios and Tests](./13-fault-scenarios-and-tests.md) | partition / kill leader / restart ↔ tests |
| 14 | [FAQ and Limits](./14-faq-and-limits.md) | Common questions and what’s deliberately not done |

```text
Intro              01–02
Core paths         03–04
Roles & storage    05–06
Protocol deep-dive 07–11
Engineering        12–14
```

---

## Quick start

```bash
# Single node
cargo run --bin raft-node -- --config config/single.yaml
cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple

# Three nodes: one terminal each
cargo run --bin raft-node -- --config config/node1.yaml
cargo run --bin raft-node -- --config config/node2.yaml
cargo run --bin raft-node -- --config config/node3.yaml
```

Config and CLI details live in the project [README](../../README.md), or [12 Config, Deploy, and Runtime](./12-config-deploy-and-runtime.md).

Source `//!` comments complement these docs; **code wins** when they diverge.
