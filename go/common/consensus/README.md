# consensus

Pure state machine library for managing durability configuration and primary selection in a
Multigres PostgreSQL cluster.

## Two-path design

Cluster changes fall into two categories that follow fundamentally different paths:

### Normal path — WAL-driven policy changes

Changes to cohort membership or the durability policy are written as ordinary postgres
transactions to the current primary. The primary's postgres instance propagates the change to
replicas via WAL. No coordinator election is needed.

`DurabilityRules` is the unit of change. It contains the full cohort membership list
(as `[]CohortMember`, bundling node ID with static properties) and the ack policy, and is
written using **compare-and-swap on a monotonic sequence number**: the writer sets
`Seq = current.Seq + 1`. The primary rejects any write whose `Seq` is not exactly one ahead of
its current rules, so only one coordinator can successfully write at a time.

```
CoordNode sees observer
  → writes DurabilityRules to primary (Seq = current.Seq + 1)
  → primary commits WAL record; propagates to sync replicas
  → replicas apply WAL, commit updated Rules
  → CoordNode observes updated status; expansion complete
```

#### Adding a cohort member (AnyN(k) → AnyN(k+1))

When the new ack threshold is higher than the old one, the primary must update
`synchronous_standby_names` to require acks from both the old and new sync-replica sets
before writing the policy record. This ensures the write is durable under both the old and
new policy. After a successful write, the primary relaxes its sync settings to the new quorum.

When the new threshold is equal to or lower (e.g. AnyN(0) → AnyN(1) when adding the first
required replica), the primary can update sync settings after the write.

**Key invariant:** at no point does `synchronous_standby_names` require acks from a set that
cannot satisfy both the old and new `AckPolicy`. This is checked by the simulation on every tick.

### Emergency path — coordinator elections

Coordinator elections (`Revoke → Establish`) are used **only** when the primary is unreachable
and a failover is needed. The elected coordinator revokes the old primary and establishes a new
one; after that, normal WAL-driven changes resume.

## Package structure

```
go/common/consensus/
  consensus.go         — shared types (NodeID, NodeProperties, CohortMember, AckPolicy,
                         DurabilityRules, PoolerPersistentState, PoolerStorage)
  durability.go        — AnyNPolicy implementation
  pooler.go            — PoolerNode state machine (production-compatible)
  coord.go             — CoordNode state machine (production-compatible)
  requests.go          — request types emitted by nodes
  indicators.go        — indicator types consumed by nodes
  simulation/          — simulation-only code; not imported by production paths
    sim_pooler.go      — WAL buffer, LSN tracking, write-quorum enforcement, replica pull loop
    sim_coord.go       — SimCoordNode wrapping CoordNode; reconciles discovery state each tick
    handler.go         — routes requests to the correct node as indicators
    mem_storage.go     — in-memory PoolerStorage for tests
    types.go           — simType alias (dstsim.Simulator parameterised with consensus types)
    *_test.go          — simulation tests
```

Production-compatible code (types, interfaces, state machines) lives in the top-level package.
Simulation-only code lives in `simulation/` and is not imported by anything outside of tests.

## WAL simulation model

WAL is modelled as a **grow-only buffer per primary** that replicas pull from each tick. This
mirrors how real postgres streaming replication works (each replica maintains its own
`primary_conninfo` connection) and avoids cluttering the consensus indicator types with WAL
details.

Each `SimPooler` has:

- A **WAL buffer** (primary only): append-only slice of `walEntry{pos lsn, record *DurabilityRules}`.
  User transactions (`record == nil`) advance the LSN without affecting policy state.
- A **received LSN** (replica): the highest LSN pulled from the primary. On graceful switchover
  this is preserved — the replica resumes from where it left off against the new primary.
- A **replica ACK map** (primary only): tracks the highest LSN each sync standby has received,
  used to evaluate write quorum.
- A **pending apply** (primary only): the policy record written to WAL but not yet durable.
  Becomes durable when `syncPolicy.IsWriteQuorum(ackingReplicas)` returns true.

Each replica calls `pullWAL()` at the start of its own `Step()`, reading new entries directly
from the primary's buffer. This means ACKs flow back to the primary within the same tick, so
write quorum can be satisfied on the tick immediately after the replica pulls.

`SimPooler` intercepts `PolicyRecordApplyRequest` (emitted by `PoolerNode`) before it reaches
the `Handler`, simulates the postgres SQL transaction and sync-settings update, and queues a
`PolicyRecordAppliedIndicator` for the next `PoolerNode.Step` call once write quorum is met.

## Key types

| Type                    | Description                                                                                                       |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `NodeProperties`        | Static per-node attributes (e.g. zone). Frozen into `DurabilityRules.Members` at write time.                      |
| `CohortMember`          | `{ID NodeID, Properties NodeProperties}`. Bundles identity and properties so policy evaluation is self-contained. |
| `DurabilityRules`       | Versioned WAL record: `{Seq int64, Members []CohortMember, Policy AckPolicy}`. CAS on monotonic `Seq`.            |
| `AckPolicy`             | Interface: `IsWriteQuorum([]CohortMember)` and `IsAchievable([]CohortMember)`.                                    |
| `AnyNPolicy`            | `AckPolicy` implementation: write is durable when at least N sync replicas have ACK'd.                            |
| `PoolerPersistentState` | Durable pooler state: `{Role, Primary, Rules *DurabilityRules}`. Saved before any ack.                            |
| `PoolerStorage`         | Durable storage interface. Simulation uses `MemStorage`; production uses atomic write-rename + fsync.             |
| `PoolerNode`            | Pooler state machine. Handles WAL-driven policy updates and emergency failover ballots.                           |
| `CoordNode`             | Coordinator state machine. Drives WAL writes for normal changes; runs elections for emergency failover.           |

### TODO: remove `Rules` from `PoolerPersistentState`

`PoolerPersistentState.Rules` is currently stored on disk, but it shouldn't need to be. The
authoritative copy of `DurabilityRules` lives in the WAL (and therefore in postgres itself).
Recovery should work by reading the rules directly from postgres rather than from a separate
persistent file. The persistent state then only needs to store identity and role information,
plus the `EstablishmentGrant` field described in the Stage 3 TODO (which also covers revocation
tracking). Removing `Rules` from persistent state simplifies the crash-recovery path and
eliminates a potential source of inconsistency between the postgres WAL copy and the
file-based copy.

## Running the simulation tests

```bash
# All consensus simulation tests
/mt-dev unit ./go/common/consensus/...

# Run with race detector
/mt-dev unit -race ./go/common/consensus/...

# Check for flakiness
/mt-dev unit -count=10 ./go/common/consensus/...
```

## Tracing

Set `DSTSIM_TRACE=1` to dump a per-tick trace to stderr when a test fails.

## What's next

### Stage 1 — Normal path: cohort expansion via WAL ✅ implemented

`TestCohortExpansion` models the coordinator driving cohort expansion without any coordinator
election, using only WAL-driven `DurabilityRules` compare-and-swap writes.

**Setup:** one node pre-initialized as primary with rules `{Seq: 1, Members: [node1], Policy: AnyN(0)}`.

The test expands the cohort from 1 to 4 nodes in three stages (→2, →3, →4). At each stage it
asserts that every pooler has:

- Committed the expected rules with the correct `AnyN(k)` ack threshold
- For the primary: `syncStandbys` = cohort minus self, `syncPolicy` = `AnyN(k)`
- For each replica: `primaryConnInfo` = the committed primary

**Safety invariant checked on every tick:**

- For each primary: every node in `syncStandbys` must be a known replica currently
  streaming from that primary (no phantom sync requirements).

### Stage 2 — Bootstrap

Initialize a cluster from scratch. The provisioner designates a single bootstrap-eligible node;
the coordinator establishes it as primary in a 1-node cohort with `AnyN(0)`. No election needed
for bootstrap — a single node always forms a valid quorum with `AnyN(0)`. Subsequent cohort
expansion uses Stage 1.

The bootstrap-eligible flag starts `true` on fresh nodes and flips to `false` once the node has
committed its first `DurabilityRules`. This prevents an accidentally recycled node from
joining a fresh-bootstrap cluster that would overwrite an existing cohort.

### Stage 3 — Emergency failover

When the primary is unreachable, the coordinator runs a two-phase protocol:

```
Revoke → Establish
```

The election is purely about appointing a new primary. Cohort membership and ack policy are not
changed — they are already in the `DurabilityRules` on disk.

After establishment, the new primary writes a `DurabilityRules` confirming the post-failover
state (same cohort, updated primary field), and normal WAL-driven operations resume.

#### TODO: design direction — no coordinator term numbers

Rather than coordinator term numbers, the unit that coordinators compete for is
**permission to establish the next `DurabilityRules`**:

**Revoke phase (non-exclusive):**
A coordinator asks nodes to revoke all primary rights under any rules with `Seq ≤ N`. A node
confirms revocation to any coordinator that asks — there is nothing to fight about. Revocation
just records "I will not serve as primary under these old rules".

**Establish phase (exclusive, Paxos-style promise):**
A coordinator tells nodes: "I am starting from rules Seq N and want to establish new rules
with Seq M (where M > N). Grant me permission." A node:

1. Rejects if its current `DurabilityRules.Seq ≠ N` (node has already advanced past this base).
2. Rejects if it has already committed to a `proposedSeq > M` (won't backtrack to a lower proposal).
3. Accepts and records `(currentSeq=N, coordID, proposedSeq=M)` in durable state.

A coordinator that proposes a higher M can supersede a stale promise at the same N, analogous to
a Paxos prepare with a higher ballot beating an older one.

**Why M ≠ N+1 in general:**
If a previous failover attempt failed mid-way (revoke succeeded, establish did not reach quorum),
the winning coordinator's `proposedSeq` counter has advanced but no rules were written. The next
coordinator proposes M = N+2 (or higher), superseding the stale promise. Normal WAL-driven
changes always write `Seq = currentSeq + 1`, but emergency establishment can jump by any amount
to skip over failed proposals.

**Durable state required:**
`PoolerPersistentState` needs a commitment field — something like
`EstablishmentGrant{AtRulesSeq int64, CoordID NodeID, ProposedSeq int64}` — so a restarted node
honours its promise. This replaces the `RevokedThrough` field mentioned in the
`PoolerPersistentState` TODO above; the `AtRulesSeq` captures the same "I participated in
revoking rules through N" information.

### Stage 4 — Graceful primary replacement

Planned maintenance: the stepping-down primary writes a `DurabilityRules` removing itself
from the quorum before shutdown, eliminating the revoke-replicas step and reducing the failover
window to near zero.

### Stage 5 — Additional durability policies

`ZoneAwarePolicy` — conjunctive requirement: at least one ACK from the same zone and one from a
different zone. `AnyNPolicy` cannot express this; requires a new `AckPolicy` implementation.

### Stage 6 — Metrics and observability

Structured event emission from `CoordNode` and `PoolerNode`, aggregated by the simulator for
per-tick assertions: appointment latency, term escalation rate, failover counts.
