# 03. Election Process

> **Nav:** [← 02. Messages and Data Structures](./02-messages-and-data-structures.md) · [04. Data Flow →](./04-data-flow.md) · [Index](./README.md)

This document explains, in **chronological order**, how a node goes from a Follower timing out, through pre-vote and the real election, to becoming Leader; and how **term / votes / log** change along the way. Source: `src/raft/node.rs`, `log.rs`, `membership.rs`.

---

## 1. Prerequisites and the Clock

| Concept | This implementation |
|------|--------|
| Logical clock | `tick()`, roughly every 100ms (`TICK_INTERVAL`) |
| Election timeout | Randomized within `election_timeout_range` at node startup (often 5..10 ticks in config) |
| Heartbeat interval | The Leader broadcasts Heartbeat every `heartbeat_interval` ticks (often 2 in config) |
| Persistence | The real election's term/vote is written to `Key::TermVote` and **always fsync'd** |

Single node (`peers` empty): `Node::new` goes straight to `into_candidate(false)` → `into_leader()`, **skipping the Pre-vote wait**.

---

## 2. Role State Machine (Overview)

```text
                     election timeout
         ┌──────────────────────────┐
         │                          ▼
    ┌────┴─────┐ pre-vote wins  ┌──────────┐ election wins ┌────────┐
    │ Follower │ ─────────────► │Candidate │ ────────────► │ Leader │
    │          │                │ PreVote  │               │        │
    └────▲─────┘                │    ↓     │               └────┬───┘
         │                      │ Election │                    │
         │   valid Leader msg   │          │                    │
         │   or higher term     └────┬─────┘                    │
         │                            │ retry on fail/timeout  │
         └────────────────────────────┴────────────────────────┘
              higher term / CheckQuorum steps down (Leader → leaderless Follower)
```

The `Candidate` internal phase:

```rust
enum ElectionPhase { PreVote, Election }
```

---

## 3. Stage 0: Stable Operation (With a Leader)

Assume a three-node cluster `{1,2,3}` with **Leader=2, term=5**.

### 3.1 What Each Node Is Doing

| Node | Role | Each tick | On a message |
|------|------|---------|----------|
| 2 | Leader | `since_heartbeat++`; `heartbeat()` when the interval is reached; CheckQuorum checks `peer_seen` | Handles Append/read/write responses |
| 1,3 | Follower | `leader_seen++`; if still below the timeout, does **not** campaign | Heartbeat: resets `leader_seen`, may advance commit and apply |

### 3.2 The Heartbeat the Leader Sends (Structure)

```text
Envelope {
  from: 2, to: 1, term: 5,
  message: Heartbeat {
    last_index:   leader's last log index,
    commit_index: committed index,
    read_seq:     current read sequence,
  }
}
```

The Follower replies:

```text
HeartbeatResponse { match_index, read_seq }
```

- If the local log's term at `last_index` matches → `match_index = last_index`  
- Otherwise `match_index = 0` (triggers Leader probe/replication)

### 3.3 Data Changes in This Stage

| Data | Change |
|------|------|
| term/vote (disk) | **Unchanged** |
| Raft log | Grows only when the Leader appends new client entries; Followers catch up via Append |
| Kv memory | `apply` runs as commit advances |
| Role | Stays the same |

---

## 4. Stage 1: Follower Election Timeout

Assume node **1** has not received messages from 2 for a while (network partition or Leader crash).

### 4.1 The tick Sequence

```text
tick: leader_seen = 1, 2, 3, ... 
until leader_seen >= election_timeout
  → into_candidate(use_prevote = opts.pre_vote)
```

`into_candidate` will:

1. **`abort_forwarded()`**: all client requests currently being forwarded → `ClientResponse{Abort}`  
2. **`maybe_apply()`**: apply committed log entries as much as possible  
3. If `pre_vote`: enter the **PreVote** phase and call `pre_campaign()`  
4. If pre_vote is disabled: go straight to `campaign()` for the real election

### 4.2 Node Changes

| Item | Before | After |
|------|--------|--------|
| Role | Follower(leader=Some(2)) | Candidate(phase=PreVote) |
| term (persisted) | 5 | **Still 5** (pre-vote does not raise the term) |
| vote | Possibly Some(2), or already voted this term | **Not changed by pre-vote** |
| forwarded | Possibly non-empty | Cleared (already Aborted) |

---

## 5. Stage 2: Pre-vote

### 5.1 What Node 1 Sends

```text
// one for each of peers 2 and 3
Envelope {
  from: 1,
  to: 2 or 3,
  term: 6,                    // intended = current term + 1, carried only on the envelope
  message: PreCampaign {
    last_index: 1's last log index,
    last_term:  1's last log term,
  }
}
// votes initially includes self {1}
```

**Important:** the term on disk is **still 5**. `Envelope.term=6` only means "the term I intend to campaign for".

### 5.2 When Other Nodes Receive a PreCampaign

Handler: `Follower::step_prevote` (it does **not** switch terms on a higher term).

Conditions for granting the pre-vote (all must hold):

1. **Not in contact with the current leader**  
   - If `leader.is_some()` and `leader_seen < election_timeout` → **reject** (the leader is still alive)  
2. **Log is fresh enough**  
   - The candidate's `(last_term, last_index)` ≥ the local last entry  
3. **`msg.term > self.term()`**  
   - The intended term must be greater than the locally persisted term  

Response:

```text
PreCampaignResponse { vote: true/false }
```

| Node behavior | Persists to disk? | Changes role? |
|----------|--------|----------|
| Grant pre-vote | **No** | **No** |
| Reject pre-vote | No | No |

### 5.3 Node 1 Collects Enough Pre-vote Ballots

```text
votes ⊇ quorum (including self) → log "Pre-vote won" → campaign()
```

If the pre-vote fails: after the next election timeout, `pre_campaign()` runs again (still without raising the persisted term, or only refreshing `intended`).

### 5.4 What Pre-vote Does Not Do

- Does **not** call `set_term_vote`  
- Does **not** replicate the log  
- Does **not** serve clients (a Candidate Aborts every ClientRequest)  
- A lagging node partitioned away cannot reach quorum in the pre-vote → it **cannot raise the term to harass** the stable majority (this is the point of Pre-vote)

---

## 6. Stage 3: Real Election (Campaign)

### 6.1 `campaign()` Step by Step

```text
1. term := term + 1          // e.g., 5 → 6
2. phase := Election
3. votes := {self}
4. log.set_term_vote(6, Some(1))   // persist + fsync
5. broadcast Campaign { last_index, last_term }  // Envelope.term = 6
```

### 6.2 Disk Data Changes (Node 1)

| Key | Old value (example) | New value |
|----|------------|------|
| `TermVote` `[0x01]` | `(5, Some(2))` or `(5, None)` | **`(6, Some(1))`** |

### 6.3 A Follower Receives the Campaign

For the same term, or after first becoming a Follower due to a higher term:

```text
if already voted for another this term → CampaignResponse{false}
if local log is more up to date        → CampaignResponse{false}
otherwise:
  set_term_vote(6, Some(1))   // persist
  CampaignResponse{true}
```

**Log freshness rule** (reject if):

```text
local.last_term > cand.last_term
or (equal and local.last_index > cand.last_index)
```

### 6.4 Nodes 2 and 3 If They Still Think 2 Is the Leader

- If they **keep receiving** 2's heartbeats, they may already have rejected 1 during the pre-vote phase  
- If 2 is dead: they will update term=6 and may vote for 1, or time out and enter an election themselves

### 6.5 Winning Quorum → `into_leader()`

```text
votes reach quorum
  → into_leader()
  → propose(None)           // append a noop Entry for this term
  → maybe_commit_and_apply  // a single node can commit the noop immediately
  → heartbeat()             // announce leadership
```

---

## 7. Stage 4: Data and Roles After the Election

### 7.1 Inside the New Leader (Node 1)

| Structure | Initial value |
|------|------|
| `progress[peer].next_index` | `last_index+1` |
| `progress[peer].match_index` | `0` |
| `progress[peer].read_seq` | `0` |
| `peer_seen[peer]` | `0` |
| Log | One extra `Entry{term=6, command=None}` (noop) |

### 7.2 The Other Nodes Become Followers

On receiving a Heartbeat/Append with term=6:

```text
into_follower(6, Some(1)), or first become leaderless then acknowledge the leader
leader = Some(1)
leader_seen = 0
```

### 7.3 Comparison: Before vs. After the Election

| | Before (example) | After (example) |
|--|--------------|--------------|
| Leader | 2 | **1** |
| term | 5 | **6** |
| Node 1 role | Follower → Candidate | **Leader** |
| Node 2 role | Leader or unreachable | Follower or offline |
| Node 3 role | Follower | Follower(leader=1) |
| Committed log | Unchanged (safety) | Can keep appending |
| Uncommitted divergence | May be overwritten by the new leader | Repaired by splice |

---

## 8. CheckQuorum: The Leader Steps Down on Its Own

No need to wait for someone else to start an election. Every tick, the Leader:

```text
peer_seen[p] += 1
if check_quorum and node count > 1:
  window = upper bound of election_timeout
  active set = {self} ∪ {p | peer_seen[p] < window}
  if the active set does not form a quorum:
    into_follower(current term)   // does not raise the term
    Abort all in-flight client requests
```

When a valid response is received from a peer, `peer_seen[p]=0`.

**Effect:** after a network partition, the **old Leader in the minority** steps down on its own, avoiding a situation where it keeps accepting writes but cannot commit them (commit itself requires a quorum; stepping down keeps the semantics cleaner).

---

## 9. Overall Timeline (Three Nodes, Node 1 Elected)

```text
time →

N1 Follower          Candidate(PreVote)     Candidate(Election)      Leader
   | leader_seen++         |                      |                      |
   | timeout               | PreCampaign(t=6)     |                      |
   |                       |--------------------► |                      |
   |                       |  PreCampaignResp     |                      |
   |                       |◄-------------------- |                      |
   |                       | pre-vote quorum    |                      |
   |                       | campaign() term=6    |                      |
   |                       | persist vote=1      |                      |
   |                       | Campaign             |                      |
   |                       |--------------------► |                      |
   |                       |  CampaignResp true   |                      |
   |                       |◄-------------------- |                      |
   |                       | real-vote quorum    |                      |
   |                       | into_leader          |                      |
   |                       | noop + heartbeat     |                      |
   |                       |--------------------------------------------►|
   |                       |                      |   Heartbeat term=6   |
N3 Follower ─────────────────────────────────────► Follower(leader=1)    |
```

---

## 10. Messages vs. Disk Writes (Election-specific)

| Step | Network message | Disk TermVote | Disk Entry |
|------|----------|---------------|------------|
| Pre-vote sent | PreCampaign | Unchanged | Unchanged |
| Pre-vote response | PreCampaignResponse | Unchanged | Unchanged |
| Real election starts | Campaign | **term+1, vote=self** | Unchanged |
| Ballot received | CampaignResponse | The voter may already have written vote | Unchanged |
| Elected | Heartbeat / Append(noop) | Unchanged | **Appends noop** |

---

## 11. Source Anchors

| Step | Function (approx.) |
|------|------------|
| Follower timeout | `RawNode<Follower>::tick` → `into_candidate` |
| Pre-vote | `pre_campaign` / `step_prevote` |
| Real election | `campaign` / `Campaign` handling |
| Elected | `into_leader` |
| Step down | `Leader::tick` CheckQuorum; or `into_follower` on a higher term |

---

> **Nav:** [← 02. Messages and Data Structures](./02-messages-and-data-structures.md) · [04. Data Flow →](./04-data-flow.md) · [Index](./README.md)
