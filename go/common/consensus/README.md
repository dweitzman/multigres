# consensus

Pure state machine library for managing durability configuration and primary selection in a
Multigres PostgreSQL cluster.

## Overview

Two node types participate:

- **PoolerNode** — manages a single PostgreSQL instance. Persists its committed state durably via
  `PoolerStorage`. The primary `PoolerNode` is the source of truth for the cluster's
  `DurabilityRules`; replicas learn about rule changes via WAL replication.

- **CoordNode** — coordinator (orchestration service). Stateless across restarts. Watches pooler
  status, discovers observer replicas (non-cohort poolers), and drives durability rule changes by
  writing `DurabilityRules` updates to the primary. Uses emergency failover only when the primary
  is unreachable.

The consensus system is responsible for one thing: keeping a **`DurabilityRules` record** that
defines exactly which postgres instance is primary, which nodes form the cohort, and how many
sync-replica ACKs constitute a durable write — and transitioning safely between successive records.

## DurabilityRules

`DurabilityRules` is the unit of configuration. Every change to cohort membership, ack policy, or
primary identity is expressed as a new `DurabilityRules` record written to the WAL:

```go
type DurabilityRules struct {
    Seq     int64          // monotonically increasing; serves as logical clock
    Primary NodeID         // the postgres primary for this shard at this rule version
    Members []CohortMember // full cohort; each member carries its static NodeProperties
    Policy  AckPolicy      // determines how many sync-replica ACKs constitute a durable write
}
```

Writes are **compare-and-swap on `Seq`**: the primary accepts a write only if
`incoming.Seq == committed.Seq + 1`. `Seq 0` is reserved for the zero value ("no rules yet");
the first real record always has `Seq 1`. A higher `Seq` always means more recent rules.

## AckPolicy

```go
type AckPolicy interface {
    // IsWriteQuorum returns true if the acknowledging replicas satisfy this policy.
    IsWriteQuorum(ackingReplicas []CohortMember) bool

    // IsAchievable returns true if this policy can be satisfied with the given cohort.
    IsAchievable(cohort []CohortMember) bool

    // IsRevoked returns true when every leader in leaders has had its leadership
    // revoked by the recruited set. Pass a single leader to check one specific
    // leadership; pass all cohort members to check all possible leaderships.
    //
    // A leader's leadership is revoked when either:
    //   - the leader is in recruited (it will not write unilaterally), or
    //   - no subset of non-recruited replicas can satisfy this policy (making
    //     a durable write impossible without the coordinator's knowledge).
    IsRevoked(allMembers, recruited, leaders []CohortMember) bool
}
```

`AnyNPolicy(n)` is the only implementation today. A write is durable when at least `n` sync
replicas have ACK'd it. `AnyN(0)` requires no replicas — suitable for a 1-node bootstrap cluster.

## Normal path — leader-driven rule change

When the primary is reachable, all rule changes flow through it as ordinary postgres transactions.

**Protocol:**

1. A write request is submitted to the primary specifying new `DurabilityRules`
   (with `Seq = committed.Seq + 1`).
2. The primary validates:
   - **CAS**: `incoming.Seq == committed.Seq + 1` — rejects stale coordinator writes.
   - **Achievability**: `incoming.Policy.IsAchievable(incoming.Members)` — rejects policies that
     cannot be satisfied by the proposed cohort.
   - **No prior commitment**: the primary has not already committed to a coordinator for any rule
     range overlapping the proposed `Seq`. (This check is a no-op today; it becomes meaningful
     once the emergency path is implemented.)
3. The primary writes a single WAL entry encoding the new rules. **This WAL entry must be
   durable under both the outgoing and incoming `AckPolicy` before the transition is complete.**
   This is the fundamental safety invariant of every rule transition: a write that changes the
   rules must itself be witnessed by enough replicas that neither the old nor the new policy can
   be violated.
4. Once durable under both policies, all subsequent WAL entries are governed solely by the new
   rules.

**Sidecar abstraction:** `PoolerNode` emits a `PolicyRecordApplyRequest` when it is ready to
write a rule transition. The sidecar (`SimPooler` in simulation, the real postgres driver in
production) performs the actual SQL and GUC work and responds with an
`ApplyRulesResponseIndicator` once the write is durable. This decouples the state machine from
the postgres execution environment.

```
CoordNode sees observer
  → writes DurabilityRules to primary (Seq = current.Seq + 1)
  → PoolerNode emits PolicyRecordApplyRequest
  → SimPooler/driver writes WAL entry; enforces both-policy quorum
  → ApplyRulesResponseIndicator delivered once durable
  → PoolerNode commits; WAL propagates to replicas
  → replicas apply WAL, update committed Rules
  → CoordNode observes updated status; expansion complete
```

## Emergency path — coordinator-led rule change

When the primary is unreachable, a coordinator can drive a rule change without the primary's
participation. The challenge is **staleness and concurrent coordinators**: multiple coordinators
may attempt the same failover without knowing about each other.

A coordinator-led change has two modes depending on what is discovered during recruitment:

- **Propagating a partial leader-led change**: if a recruited node has a WAL entry for a newer
  rule version that never reached write quorum, the coordinator propagates those entries to
  a quorum of the recruited nodes. The propagated rule may include cohort or policy changes that
  the leader had already encoded in WAL.
- **Initiating a fresh rule change**: if no partial change is found, the coordinator creates a new
  rule itself. In this case it **can only update the `Primary` field**, because writing WAL entries
  that encode cohort or policy changes requires a running postgres primary — which is absent by
  definition.

### Recruitment and commitments

A pooler can be _recruited_ by a coordinator to participate in a rule change that transitions
from rule version `N` to version `N+X`. Recruitment is a **durable commitment**: the pooler
persists this commitment to `PoolerPersistentState` so it survives crashes. A restarted pooler
must honour its prior commitments before accepting any new instructions. The commitment records
that the pooler will not accept instructions from any other coordinator (including the original
primary) for any rule version in the committed range.

`Seq` serves double duty: a CAS key when WAL is advancing, and a **logical clock** for
sequencing coordinator attempts when WAL is stopped. Because WAL does not advance during an
emergency rule change, `Seq` is the only mechanism coordinators have to order their attempts
relative to each other.

A pooler committed to a transition `(N, N+X)` accepts a new proposal `(N', N'+X')` if and only
if **both**:

- `N' >= N` — the coordinator has at least as current a view of the rules (not stale), and
- `N'+X' > N+X` — the coordinator is targeting strictly beyond the committed endpoint.

This lets a coordinator that loses a race retry with a higher target (`N+X+1`) using the same
base `N` — the retry supersedes the earlier attempt without requiring WAL to advance. Examples
for a pooler committed to `(N, N+X)`:

| Proposal      | Outcome                                                        |
| ------------- | -------------------------------------------------------------- |
| `N → N+X-1`   | Reject — target too low                                        |
| `N → N+X`     | Reject — same target, not strictly higher                      |
| `N-1 → N+X+1` | Reject — stale base; pooler has already seen rule N            |
| `N → N+X+1`   | Accept — same base, higher target (retry with incremented seq) |
| `N+1 → N+X+1` | Accept — higher base means coordinator knows a later rule      |

### Two quorum requirements

The coordinator must recruit enough poolers to satisfy two independent quorums:

1. **Revocation quorum**: enough nodes have been recruited that the current ack policy for rule N
   can no longer be satisfied by any non-recruited nodes. `AckPolicy.IsRevoked` captures this
   check — it returns true when the policy can no longer be satisfied without the coordinator's
   knowledge.

2. **Discovery quorum**: enough nodes have been recruited that no other coordinator could have
   established a different rule version (`N+1`, `N+2`, …) without this coordinator learning about
   it. If the current ack policy for rule N was satisfied before recruitment, then any committed
   rule `N+1` must have been ACK'd by at least one node in the revocation set. The coordinator
   queries recruited nodes for any WAL entries representing rule successors they hold.

   **If a successor WAL entry is discovered** (a partially-committed leader-led change), the
   coordinator propagates those entries to the recruited nodes and attempts to make the write
   durable under both the old rule N ack policy and the new rule's ack policy. Successfully
   reaching that quorum means no other coordinator could have already established that rule
   version against a different set — any other coordinator that recruits the same nodes will now
   see the newly-propagated rule and must use it as their base.

   Once propagated:
   - If the new rule's `Primary` is healthy: fix replication GUCs on replicas and resume normal
     WAL-driven operations under the newly-established rule.
   - If the new rule's `Primary` is still unavailable: restart the entire coordinator-led process
     using the newly-propagated rule as the new base.

   **If no successor WAL entry is found**: proceed to establish a fresh rule (see below).

### Fresh coordinator-initiated changes

When no successor WAL entry is found, the coordinator creates a new rule from scratch. Because
there is no running primary to write arbitrary WAL entries, the only field the coordinator can
change is `Primary` — cohort membership and ack policy remain identical to rule N.

As a consequence, a coordinator racing against another coordinator that also found no successor
knows that any rule version the other coordinator could have established must involve the same
cohort and ack policy. The coordinator can enumerate all possible primaries within the known
cohort and attempt to form a revocation set for each, letting `AckPolicy.IsRevoked` guide
whether a sufficient set has been recruited for each candidate.

### Establishing the new rule

Once both quorums are satisfied and no unknown successor exists:

1. Choose the new primary from within the cohort.
2. Start the new primary with both the outgoing and incoming ack policies applied — exactly as in
   the leader-driven case.
3. Write the new rules WAL entry (`Primary` updated, cohort and policy unchanged,
   `Seq = N+X`).
4. Once durable under both policies, switch the GUC to the incoming policy only.

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
the `Handler`, simulates the postgres SQL transaction and sync-settings update, and queues an
`ApplyRulesResponseIndicator` for the next `PoolerNode.Step` call once write quorum is met.

## Key types

| Type                    | Description                                                                                                       |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `NodeProperties`        | Static per-node attributes (e.g. zone). Frozen into `DurabilityRules.Members` at write time.                      |
| `CohortMember`          | `{ID NodeID, Properties NodeProperties}`. Bundles identity and properties so policy evaluation is self-contained. |
| `DurabilityRules`       | Versioned WAL record: `{Seq, Primary, Members, Policy}`. CAS on monotonic `Seq`.                                  |
| `AckPolicy`             | Interface: `IsWriteQuorum`, `IsAchievable`, `IsRevoked`.                                                          |
| `AnyNPolicy`            | `AckPolicy` implementation: write is durable when at least N sync replicas have ACK'd.                            |
| `PoolerPersistentState` | Durable pooler state: `{Role, Primary, Rules *DurabilityRules}`. Saved before any ack.                            |
| `PoolerStorage`         | Durable storage interface. Simulation uses `MemStorage`; production uses atomic write-rename + fsync.             |
| `PoolerNode`            | Pooler state machine. Handles WAL-driven policy updates and emergency failover ballots.                           |
| `CoordNode`             | Coordinator state machine. Drives WAL writes for normal changes; runs elections for emergency failover.           |

## Running the simulation tests

```bash
# All consensus simulation tests
/mt-dev unit ./go/common/consensus/...

# Run with race detector
/mt-dev unit ./go/common/consensus/... -race

# Check for flakiness
/mt-dev unit ./go/common/consensus/... -count=10
```

## Tracing

Set `DSTSIM_TRACE=1` to dump a per-tick trace to stderr when a test fails.

## What's next

### Stage 1 — Normal path: cohort expansion via WAL ✅ implemented

`TestCohortExpansion` and `TestCohortChange` model the coordinator driving cohort expansion and
policy changes without any coordinator election, using only WAL-driven `DurabilityRules`
compare-and-swap writes.

**Setup:** one node pre-initialized as primary with rules `{Seq: 1, Primary: node1, Members: [node1], Policy: AnyN(0)}`.

`TestCohortExpansion` expands the cohort from 1 to 4 nodes in three stages (→2, →3, →4). At each
stage it asserts that every pooler has:

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

### Stage 3 — Emergency failover

When the primary is unreachable, the coordinator runs the recruitment + two-quorum protocol
described in the [Emergency path](#emergency-path--coordinator-led-rule-change) section above.
If a partial leader-led change is discovered in WAL, the coordinator propagates it to quorum
(which may include cohort or policy changes). If no partial change exists, the coordinator
initiates a fresh rule change updating only `Primary`.

A coordinator-initiated leader change likely requires that the newly-established quorum immediately write a rule change entry before
the leader change is considered finished, since coordinators can't easily
write WAL while postgres is stopped.

After establishment, normal WAL-driven operations resume.

**Durable state required:** `PoolerPersistentState` needs a commitment field —
`EstablishmentGrant{AtRulesSeq int64, CoordID NodeID, ProposedSeq int64}` — so a restarted node
honours its prior commitments to coordinators. The `AtRulesSeq` field captures "I participated
in revoking rules through N", and `ProposedSeq` records the highest target this node has already
committed to.

### Stage 3.5 — Coordinator cluster state tracking

For the coordinator to act quickly during emergency failover it needs a continuously maintained
view of the cluster: which poolers exist, which are healthy, what rule version each is on, and
which is currently primary. This requires a discovery and health-monitoring mechanism so the
coordinator arrives at a failover decision with enough information already cached, rather than
needing to re-query the entire cohort under time pressure.

### Stage 4 — Graceful primary replacement

Planned maintenance: the stepping-down primary writes a `DurabilityRules` updating `Primary`
before shutdown, eliminating the need for emergency failover and reducing the switchover window
to near zero.

### Stage 5 — Additional durability policies

`ZoneAwarePolicy` — conjunctive requirement: at least one ACK from the same zone and one from a
different zone. `AnyNPolicy` cannot express this; requires a new `AckPolicy` implementation.

### Stage 6 — Metrics and observability

Structured event emission from `CoordNode` and `PoolerNode`, aggregated by the simulator for
per-tick assertions: appointment latency, term escalation rate, failover counts.
