# 05. Node Roles

> **Nav:** [← 04. Data Flow](./04-data-flow.md) · [06. BitCask and Log Keyspace →](./06-bitcask-and-log-keyspace.md) · [Index](./README.md)

A reference table: what each of the three roles does / does not do in **election, replication, client handling, and storage**.

---

## 1. Overview Table

| Responsibility | Follower | Candidate | Leader |
|------|:--------:|:---------:|:------:|
| Initiate Pre-vote / real election | Becomes Candidate after timeout | **Yes** | No |
| Persist the vote | When granting a real vote | When voting for self | Clears it on a higher term |
| Send Heartbeat / Append | No | No | **Yes** |
| Accept and write the replicated log | **Yes** (splice) | Becomes Follower first upon a valid leader message | Appends itself |
| Commit the log (set commit_index) | Follows leader notifications | No | **Yes** (quorum) |
| Apply the state machine | **Yes** (per commit) | `maybe_apply` before switching | **Yes** |
| Handle client writes | Forwards only | Abort | **propose** |
| Handle client reads | Forwards only | Abort | **read after read_seq quorum** |
| Step down under CheckQuorum | No | No | **Yes** |
| Serve linearizable reads | No (does not answer from local state) | No | **Yes** |

---

## 2. Follower in Detail

### 2.1 What It Does

1. **Timing**: `leader_seen`; on timeout → Candidate  
2. **Follow the leader**: Heartbeat / Append / Read / InstallSnapshot  
3. **Vote**: when Campaign (real election) arrives, vote per the rules and persist the vote  
4. **Pre-vote**: no persistence on PreCampaign  
5. **Write the log**: `splice` into the local BitCask on a successful `Append`  
6. **Apply**: `State::apply` after commit advances  
7. **Forward client requests**: if there is a `leader`, send the `ClientRequest` to it  

### 2.2 What It Does Not Do

- Does not `append` client writes to the log on its own  
- Does not decide the cluster `commit_index` (only follows)  
- Does not return the local `Kv` directly as a linearizable read result  
- Does not "guess" a write succeeded when there is no leader (aborts directly)  

### 2.3 Internal Data

| Field | Meaning |
|------|------|
| `leader` | The leader it currently recognizes |
| `leader_seen` | Ticks since the last leader message |
| `election_timeout` | The timeout threshold |
| `forwarded` | RequestIDs forwarded but not yet completed |

---

## 3. Candidate in Detail

### 3.1 What It Does

| Phase | Behavior |
|------|------|
| **PreVote** | Broadcast PreCampaign; count pre-votes; does **not** change the persisted term |
| **Election** | `term+1`, vote for self, broadcast Campaign; count real votes |
| Timeout | Re-run pre_campaign or campaign |
| Valid leader message | Become Follower and process the message |

### 3.2 What It Does Not Do

- Does not replicate business logs (is not the leader)  
- Does not accept client reads/writes (always Abort)  
- Does not `set_term_vote` in the PreVote phase  

### 3.3 Internal Data

| Field | Meaning |
|------|------|
| `votes` | The set of nodes that have voted for it (including itself) |
| `phase` | PreVote / Election |
| `election_duration` / `election_timeout` | The timer for this election round |

---

## 4. Leader in Detail

### 4.1 What It Does

1. **Heartbeat**: maintain authority, advance follower commit, carry read_seq  
2. **Write**: `propose` → local log → Append → quorum → commit → apply → reply to the client  
3. **Read**: assign a `read_seq`, then `state.read` after quorum confirmation  
4. **Progress**: `match_index` / `next_index` per peer; probe on conflict  
5. **Membership change**: propose Joint, automatically become Simple after commit; may step down  
6. **Snapshot**: compact at the threshold; InstallSnapshot if a peer is too far behind  
7. **CheckQuorum**: step down and Abort in-flight requests if a quorum goes silent  

### 4.2 What It Does Not Do

- Does not process same-term Heartbeats claiming another leader (drops/warns; this implementation does not panic)  
- Does not mark a write as successful before it reaches a quorum  
- Does not return write success to the client before apply  

### 4.3 Internal Data

| Field | Meaning |
|------|------|
| `progress` | The replication watermark |
| `writes` / `membership_writes` | Clients waiting on apply |
| `reads` / `read_seq` | The linearizable-read pipeline |
| `peer_seen` | CheckQuorum |
| `since_heartbeat` | Heartbeat cadence |

---

## 5. Who Touches Data, By Scenario

### 5.1 During an Election

| Data | Follower | Candidate | Leader (old) |
|------|----------|-----------|------------|
| TermVote on disk | May be written when voting | Writes when voting for self | Cleared on partition or higher term |
| Entry log | Usually unchanged | Unchanged | May still append (if not stepped down) |
| Kv | Applies only committed entries | Applies before switching | Same as left |

### 5.2 Steady-State Write

| Data | Leader | Follower |
|------|--------|----------|
| Entry | Written first | Written after Append |
| CommitIndex | Written after quorum | Follows after Heartbeat |
| Kv | Updated after apply | Applies after commit |

### 5.3 Steady-State Read

| Data | Leader | Follower |
|------|--------|----------|
| Log | Untouched | Untouched |
| Kv | **Read-only** | Does not answer (only returns ReadResponse to confirm the leader) |

---

## 6. Physical Process vs. Logical Role

The `raft-node` **process** always runs the event loop; the **role** switches among the `Node` enum variants:

```text
same process:
  select! tick / inbound / outbound
  node = node.step(...)  // may be Follower→Candidate→Leader
```

Clients always connect to a **TCP port**, not bound to a role; role changes only affect the message-handling branch.

---

> **Nav:** [← 04. Data Flow](./04-data-flow.md) · [06. BitCask and Log Keyspace →](./06-bitcask-and-log-keyspace.md) · [Index](./README.md)
