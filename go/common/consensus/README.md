# consensus

Pure state machine library for managing durability configuration and primary selection in a
Multigres PostgreSQL cluster.

## Two-path design

Cluster changes fall into two categories that follow fundamentally different paths:

### Normal path — WAL-driven policy changes

Changes to cohort membership or the durability policy are written as ordinary postgres
transactions to the current primary. The primary's postgres instance propagates the change to
replicas via WAL. No coordinator election is needed.

A `DurabilityPolicyRecord` is the unit of change. It contains the full cohort membership list
and the ack policy, and is written using **compare-and-swap**: the writer provides the `ID` of
the policy it believes is currently active (`PreviousID`). If `PreviousID` does not match the
primary's current record, the write is rejected and the writer must refresh its view before
retrying.

```
CoordNode sees observer
  → writes DurabilityPolicyRecord to primary (PreviousID = current ID)
  → primary commits WAL record; propagates to sync replicas
  → replicas apply WAL, update their PolicyVersion
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

Coordinator elections (`Begin → Revoke → Establish`) are used **only** when the primary is
unreachable and a failover is needed. The elected coordinator revokes the old primary and
establishes a new one; after that, normal WAL-driven changes resume.

## Package structure

```
go/common/consensus/
  consensus.go         — shared types (NodeID, PolicyID, DurabilityPolicy, DurabilityPolicyRecord,
                         PoolerPersistentState, PoolerStorage)
  durability.go        — AnyNPolicy implementation
  pooler.go            — PoolerNode state machine (production-compatible)
  coord.go             — CoordNode state machine (production-compatible)
  requests.go          — request types emitted by nodes
  indicators.go        — indicator types consumed by nodes
  simulation/          — simulation-only code; not imported by production paths
    wal_sim.go         — WAL ring buffer, LSN tracking, replica apply loop
    mem_storage.go     — in-memory PoolerStorage for tests
    harness.go         — test harness wiring (request→indicator routing)
    *_test.go          — simulation tests
```

Production-compatible code (types, interfaces, state machines) lives in the top-level package.
Simulation-only code lives in `simulation/` and is not imported by anything outside of tests.

## WAL simulation model

Rather than routing WAL as indicators through the dstsim delivery pipeline, WAL is modelled as
a **ring buffer per replication slot** that is advanced by a separate simulation loop. This keeps
WAL simulation close to how real postgres streaming replication works and avoids cluttering the
consensus indicator types with WAL protocol details.

Each `simPooler` has:

- A **write buffer** (primary only): uncommitted WAL entries, flushed when acks arrive
- A **receive buffer** (replica): entries received from the primary but not yet applied
- An **apply pointer**: the LSN up to which entries have been applied to the in-memory state

The simulation loop, driven by the dstsim tick, copies entries from the primary's write buffer
to each replica's receive buffer through a configurable delivery policy (same
`IndicatorDeliveryPolicy` used for consensus messages: reliable fast delivery or unreliable
chaos). Entries in a replica's receive buffer are applied on the same tick they arrive (or on
the next tick under the chaos policy).

LSN is an integer that increments monotonically with each WAL entry. Replicas report their
applied LSN in status updates; the coordinator uses this to decide which replicas are eligible
candidates for promotion.

## Key types

| Type                     | Description                                                                                               |
| ------------------------ | --------------------------------------------------------------------------------------------------------- |
| `PolicyID`               | Unique string identifier for a `DurabilityPolicyRecord` version. Used for compare-and-swap writes.        |
| `DurabilityPolicyRecord` | Versioned WAL record: `{ID, PreviousID, CohortMembers, Policy}`. Written to the primary as a transaction. |
| `DurabilityPolicy`       | Interface: `IsWriteQuorum(ackingReplicas)` and `IsAchievable(numCohortMembers)`.                          |
| `AnyNPolicy`             | `DurabilityPolicy` implementation: write is durable when at least N sync replicas have ACK'd.             |
| `PoolerPersistentState`  | Durable pooler state: `{Role, Primary, PolicyVersion}`. Saved before any ack.                             |
| `PoolerStorage`          | Durable storage interface. Simulation uses `memStorage`; production uses atomic write-rename + fsync.     |
| `PoolerNode`             | Pooler state machine. Handles WAL-driven policy updates and emergency failover ballots.                   |
| `CoordNode`              | Coordinator state machine. Drives WAL writes for normal changes; runs elections for emergency failover.   |

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

### Stage 1 — Normal path: cohort expansion via WAL (current focus)

The first simulation models the coordinator driving cohort expansion without any coordinator
election, using only WAL-driven `DurabilityPolicyRecord` compare-and-swap writes.

**Setup:** one node pre-initialized as primary with policy `{ID: "v1", Cohort: [node1], Policy: AnyN(0)}`.

**Test:** `TestCohortExpansion`

1. Coordinator discovers node2 as an observer (replicating from node1 but not in cohort)
2. Coordinator writes `{ID: "v2", PreviousID: "v1", Cohort: [node1, node2], Policy: AnyN(1)}` to primary
3. Primary commits and propagates WAL to node2
4. node2 applies WAL; reports `PolicyVersion: "v2"` in status
5. Coordinator sees node2 reporting v2 — expansion complete; repeats for node3

**Invariant checked on every tick:**

- At most one node has `Role=Primary` with `applied=true` and a write quorum satisfied
- Primary's effective `synchronous_standby_names` never requires acks from a set that cannot
  satisfy both the old and new `AckPolicy`

This test runs under both fast delivery and chaos (unreliable network + node crashes).

### Stage 2 — Bootstrap

Initialize a cluster from scratch. The provisioner designates a single bootstrap-eligible node;
the coordinator establishes it as primary in a 1-node cohort with `AnyN(0)`. No election needed
for bootstrap — a single node always forms a valid quorum with `AnyN(0)`. Subsequent cohort
expansion uses Stage 1.

The bootstrap-eligible flag starts `true` on fresh nodes and flips to `false` once the node has
committed its first `DurabilityPolicyRecord`. This prevents an accidentally recycled node from
joining a fresh-bootstrap cluster that would overwrite an existing cohort.

### Stage 3 — Emergency failover

When the primary is unreachable, the coordinator runs a three-phase election:

```
Begin → Revoke → Establish
```

The election is purely about appointing a new primary. Cohort membership and ack policy are not
changed by the election — they are in the `DurabilityPolicyRecord` already on disk. The
coordinator carries the `PolicyVersion` of the last known-durable policy record; replicas reject
any ballot whose `PolicyVersion` is behind their own.

After establishment, the new primary writes a `DurabilityPolicyRecord` confirming the post-failover
state (same cohort, potentially updated primary), and normal WAL-driven operations resume.

### Stage 4 — Graceful primary replacement

Planned maintenance: the stepping-down primary writes a `DurabilityPolicyRecord` removing itself
from the quorum before shutdown, eliminating the revoke-replicas step and reducing the failover
window to near zero.

### Stage 5 — Additional durability policies

`ZoneAwarePolicy` — conjunctive requirement: at least one ACK from the same zone and one from a
different zone. `AnyNPolicy` cannot express this; requires a new `DurabilityPolicy` implementation.

### Stage 6 — Metrics and observability

Structured event emission from `CoordNode` and `PoolerNode`, aggregated by the simulator for
per-tick assertions: appointment latency, term escalation rate, failover counts.
