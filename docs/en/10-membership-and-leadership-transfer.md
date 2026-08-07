# 10. Membership and Leadership Transfer

> **Nav:** [← 09. Session and CLI Idempotency](./09-session-and-cli-idempotency.md) · [11. Snapshots and Catch-up →](./11-snapshots-and-catch-up.md) · [Index](./README.md)

This implementation uses **Joint Consensus** for membership changes and supports **removing the current leader via configuration** to step down. Source: `src/raft/membership.rs`, and the Leader's `propose_membership_change` / `maybe_propose_simple_after_joint`.

---

## 1. Configuration Types

```text
Membership { voters: BTreeSet<NodeID> }

MembershipEntry::Simple(Membership)           // single configuration
MembershipEntry::Joint { old, new }           // joint: both majorities must be satisfied
```

| Phase | Quorum |
|------|----------|
| Simple(C) | strict majority of C |
| Joint(Cold, Cnew) | **majority of Cold and majority of Cnew** |

`MembershipState`:

- `active`: the currently effective entry (**switches as soon as it is appended to the log**, no need to wait for commit)
- `change_pending`: whether there is an in-flight change (only one allowed at a time)

---

## 2. Client API

```text
Request::ChangeMembership { voters: HashSet<NodeID> }  // target C_new
Response::ChangeMembership { index }                   // usually the Joint entry index
```

CLI:

```bash
cargo run --bin raft-cli -- --peers ... members 1,2,3,4
```

---

## 3. Change Flow (Leader)

```text
1. Rejection conditions:
   - already change_pending / already in Joint
   - voters is empty
   - same as the current Simple
2. take old = the current Simple
3. propose Joint { old, new: C_new } → append_membership
4. apply_entry(Joint)  // immediately dual majority
5. replicate; Joint commits on dual majority
6. after on_commit(Joint), automatically maybe_propose_simple_after_joint
7. propose Simple(C_new) → effective on append
8. Simple commits → change_pending=false
9. if self ∉ C_new → step_down (leadership transfer complete)
```

### The Entries in the Log

```text
Entry {
  command: None,
  membership: Some(Joint { old, new }),
}
// then
Entry {
  command: None,
  membership: Some(Simple(new)),
}
```

At apply time these are **noop** for the state machine (advancing `applied_index`); the client can receive `ChangeMembership{index}` as soon as the **Joint commits** (an implementation choice).

---

## 4. When Does It Take Effect?

| Event | Configuration pointer |
|------|----------|
| Joint **appended** to the local log | immediately use dual majority for election/commit/read confirmation |
| Joint **commits** | automatically propose Simple next |
| Simple **appended** | immediately use only C_new |
| Simple **commits** | pending cleared; may step down |

The Follower calls `maybe_apply_membership_from_entries` after `splice` in `Append`, switching **as soon as the entry appears in the log**, just like the Leader.

---

## 5. Adding / Removing Nodes

### 5.1 Adding

1. Ops starts a new `raft-node` (an empty data_dir, or one that catches up the log)
2. The client's `members` includes all old and new ids
3. The leader replicates the log to the new node (InstallSnapshot if too far behind, see 11)
4. After entering C_new, the new node participates in majorities

In-process tests use `Cluster::start(id)` to simulate starting an empty-log node.

### 5.2 Removing (non-leader)

After `members` drops an id → Joint → Simple, that id no longer counts toward the quorum; the implementation stops replicating to its progress (`sync_progress_with_membership`).

### 5.3 Removing the Current Leader (Leadership Transfer)

```text
C_old = {1,2,3}, Leader=1
ChangeMembership {2,3}
  → Joint dual majority (may still need 1's vote, since old includes 1)
  → after Simple{2,3} commits
  → node 1: self ∉ voters → into_follower (steps down in the same term)
  → {2,3} elect a new leader
```

**This implementation has no separate TimeoutNow / leader-handoff RPC**; it relies on the membership change to move the old leader out of the configuration.

---

## 6. Roles During a Change

| Role | Behavior |
|------|------|
| Leader | The only one to propose configuration entries; maintains the changing set of progress |
| Follower | Replicates configuration entries; changes active after append |
| Candidate | Uses the **current active** (possibly Joint) to compute whether the votes satisfy dual majority |

---

## 7. Test Mapping

| Test | Verifies |
|------|------|
| `tests/membership.rs` add/remove | 3→4, 4→3 |
| `remove_leader_steps_down` | new leader and data after removing the leader |
| `overlapping_change_rejected` | a second change while one is in flight |

---

## 8. Source Anchors

| Step | Location |
|------|------|
| Propose Joint | `propose_membership_change` |
| Automatic Simple | `maybe_propose_simple_after_joint` |
| Step down after commit | `maybe_commit_and_apply` returns step_down |
| Quorum | `MembershipEntry::has_quorum` |

---

> **Nav:** [← 09. Session and CLI Idempotency](./09-session-and-cli-idempotency.md) · [11. Snapshots and Catch-up →](./11-snapshots-and-catch-up.md) · [Index](./README.md)
