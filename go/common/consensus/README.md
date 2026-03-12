# consensus

Pure state machine library for managing durability configuration and primary selection in a
Multigres PostgreSQL cluster.

## Overview

Two node types participate:

- **PoolerNode** — manages a single PostgreSQL instance. Persists its committed state durably via
  `PoolerStorage`. The primary `PoolerNode` is the source of truth for the cluster's
  `Term`; replicas learn about rule changes via WAL replication.

- **CoordNode** — coordinator (orchestration service). Stateless across restarts. Watches pooler
  status, discovers observer replicas (non-cohort poolers), and drives durability rule changes by
  writing `Term` updates to the primary. Uses a coordinator-led term change when the primary
  is unreachable (or for a planned primary replacement).

The consensus system is responsible for one thing: keeping a **`Term` record** that
defines exactly which postgres instance is primary, which nodes form the cohort, and how many
sync-replica ACKs constitute a durable write — and transitioning safely between successive records.

## Term

`Term` is the unit of configuration. Every change to cohort membership, ack policy, or
primary identity is expressed as a new `Term` record written to the WAL:

```go
type Term struct {
    Seq     int64            // monotonically increasing; serves as logical clock
    Primary NodeID           // the postgres primary for this shard at this term
    Members []CohortMember   // full cohort; each member carries its static NodeProperties
    Policy  DurabilityPolicy // determines how many sync-replica ACKs constitute a durable write
}
```

Writes are **compare-and-swap on `Seq`**: the primary accepts a write only if
`incoming.Seq == committed.Seq + 1`. `Seq 0` is reserved for the zero value ("no term yet");
the first real record always has `Seq 1`. A higher `Seq` always means a more recent term.

## DurabilityPolicy

```go
type DurabilityPolicy interface {
    // IsDurable returns true if the acknowledging cohort members constitute a
    // durable write quorum under this policy.
    IsDurable(cohortMembers, ackingMembers []CohortMember) bool

    // IsAchievable returns true if this policy can ever be satisfied with the
    // proposed cohort.
    IsAchievable(proposedCohort []CohortMember) bool

    // RevokesAndSamplesAllRevocationSets returns true when the recruited set
    // both revokes all possible write quorums (no non-recruited subset can
    // satisfy IsDurable) and samples at least one member from every minimal
    // quorum (so no durable write could have occurred without the coordinator
    // learning about it from some recruited node).
    RevokesAndSamplesAllRevocationSets(cohortMembers, recruitedMembers []CohortMember, primary CohortMember) bool
}
```

`AtLeastPolicy(n)` is the only implementation today. A write is durable when at least `n`
cohort members have ACK'd it (primary counts as one ACK). `AtLeast(1)` requires no replicas —
suitable for a 1-node bootstrap cluster.

## Normal path — leader-driven rule change

When the primary is reachable, all rule changes flow through it as ordinary postgres transactions.

**Protocol:**

1. A write request is submitted to the primary specifying new `Term`
   (with `Seq = committed.Seq + 1`).
2. The primary validates:
   - **CAS**: `incoming.Seq == committed.Seq + 1` — rejects stale coordinator writes.
   - **Achievability**: `incoming.Policy.IsAchievable(incoming.Members)` — rejects policies that
     cannot be satisfied by the proposed cohort.
   - **No prior commitment**: the primary has not already committed to a coordinator for any rule
     range overlapping the proposed `Seq`. (This check is a no-op today; it becomes meaningful
     once the emergency path is implemented.)
3. The primary writes a single WAL entry encoding the new term. **This WAL entry must be
   durable under both the outgoing and incoming `DurabilityPolicy` before the transition is
   complete.** This is the fundamental safety invariant of every term transition: a write that
   changes the term must itself be witnessed by enough replicas that neither the old nor the new
   policy can be violated.
4. Once durable under both policies, all subsequent WAL entries are governed solely by the new
   rules.

**Sidecar abstraction:** `PoolerNode` emits a `PolicyRecordApplyRequest` when it is ready to
write a rule transition. The sidecar (`SimPooler` in simulation, the real postgres driver in
production) performs the actual SQL and GUC work and responds with an
`ApplyRulesResponseIndicator` once the write is durable. This decouples the state machine from
the postgres execution environment.

```
CoordNode sees observer
  → writes Term to primary (Seq = current.Seq + 1)
  → PoolerNode emits PolicyRecordApplyRequest
  → SimPooler/driver writes WAL entry; enforces both-policy quorum
  → ApplyRulesResponseIndicator delivered once durable
  → PoolerNode commits; WAL propagates to replicas
  → replicas apply WAL, update committed Term
  → CoordNode observes updated status; expansion complete
```

## Coordinator-led term change

When the primary is unreachable (or for a planned replacement), a coordinator can drive a term
change without the primary's participation. The challenge is **staleness and concurrent coordinators**: multiple coordinators
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
   can no longer be satisfied by any non-recruited nodes. `DurabilityPolicy.RevokesAndSamplesAllRevocationSets`
   captures this check — it returns true when the policy can no longer be satisfied without the
   coordinator's knowledge.

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
cohort and attempt to form a revocation set for each, letting `DurabilityPolicy.RevokesAndSamplesAllRevocationSets`
guide whether a sufficient set has been recruited for each candidate.

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
  consensus.go         — shared types (NodeID, NodeProperties, CohortMember, DurabilityPolicy,
                         Term, PoolerPersistentState, PoolerStorage)
  durability.go        — AtLeastPolicy implementation
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

- A **WAL buffer** (primary only): append-only slice of `walEntry{pos lsn, record *Term}`.
  User transactions (`record == nil`) advance the LSN without affecting policy state.
- A **received LSN** (replica): the highest LSN pulled from the primary. On graceful switchover
  this is preserved — the replica resumes from where it left off against the new primary.
- A **replica ACK map** (primary only): tracks the highest LSN each sync standby has received,
  used to evaluate write quorum.
- A **pending apply** (primary only): the policy record written to WAL but not yet durable.
  Becomes durable when `syncPolicy.IsDurable(cohortMembers, ackingReplicas)` returns true.

Each replica calls `pullWAL()` at the start of its own `Step()`, reading new entries directly
from the primary's buffer. This means ACKs flow back to the primary within the same tick, so
write quorum can be satisfied on the tick immediately after the replica pulls.

`SimPooler` intercepts `PolicyRecordApplyRequest` (emitted by `PoolerNode`) before it reaches
the `Handler`, simulates the postgres SQL transaction and sync-settings update, and queues an
`ApplyRulesResponseIndicator` for the next `PoolerNode.Step` call once write quorum is met.

## Key types

| Type                    | Description                                                                                                                         |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `NodeProperties`        | Static per-node attributes (e.g. zone). Frozen into `Term.Members` at write time.                                                   |
| `CohortMember`          | `{ID NodeID, Properties NodeProperties}`. Bundles identity and properties so policy evaluation is self-contained.                   |
| `Term`                  | Versioned WAL record: `{Seq, Primary, Members, Policy}`. CAS on monotonic `Seq`.                                                    |
| `DurabilityPolicy`      | Interface: `IsDurable`, `IsAchievable`, `RevokesAndSamplesAllRevocationSets`.                                                       |
| `AtLeastPolicy`         | `DurabilityPolicy` implementation: write is durable when at least N cohort members have ACK'd.                                      |
| `PoolerPersistentState` | Durable pooler state: `{Role, Primary, Term *Term, Commitment *RecruitmentCommitment}`. Saved before any ack.                       |
| `PoolerStorage`         | Durable storage interface. Simulation uses `MemStorage`; production uses atomic write-rename + fsync.                               |
| `PoolerNode`            | Pooler state machine. Handles WAL-driven policy updates and coordinator-led term changes.                                           |
| `CoordNode`             | Coordinator state machine. Drives WAL writes for normal changes; runs coordinator-led term changes when the primary is unreachable. |

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
policy changes without any coordinator election, using only WAL-driven `Term`
compare-and-swap writes.

**Setup:** one node pre-initialized as primary with rules `{Seq: 1, Primary: node1, Members: [node1], Policy: AtLeast(1)}`.

`TestCohortExpansion` expands the cohort from 1 to 4 nodes in three stages (→2, →3, →4). At each
stage it asserts that every pooler has:

- Committed the expected rules with the correct `AtLeast(k)` ack threshold
- For the primary: `syncStandbys` = cohort minus self, `syncPolicy` = `AtLeast(k)`
- For each replica: `primaryConnInfo` = the committed primary

**Safety invariant checked on every tick:**

- For each primary: every node in `syncStandbys` must be a known replica currently
  streaming from that primary (no phantom sync requirements).

### Stage 2 — Bootstrap

Initialize a cluster from scratch. The provisioner designates a single bootstrap-eligible node;
the coordinator establishes it as primary in a 1-node cohort with `AtLeast(1)`. No election needed
for bootstrap — a single node always forms a valid quorum with `AtLeast(1)`. Subsequent cohort
expansion uses Stage 1.

### Stage 3 — Coordinator-led term change

When the primary is unreachable, the coordinator runs the recruitment + two-quorum protocol
described in the [Coordinator-led term change](#coordinator-led-term-change) section above.
If a partial leader-led change is discovered in WAL, the coordinator propagates it to quorum
(which may include cohort or policy changes). If no partial change exists, the coordinator
initiates a fresh rule change updating only `Primary`.

A coordinator-initiated leader change requires that the newly-established quorum immediately
write a new Term entry before the failover is considered complete, since the coordinator
cannot append to postgres WAL while postgres is stopped.

**Shadow WAL:** Because postgres is stopped during coordinator-led term change, the coordinator cannot
append to the real WAL. Instead, term transition commitments are recorded in a per-node
_commitment file_ — essentially a shadow WAL narrow enough to fsync safely without a running
postgres. The commitment file is written before the node acks the recruiter, so the coordinator
only learns of the commitment after it is durable. Once the new primary is promoted, it copies
shadow WAL entries directly into real postgres WAL before accepting any other transactions,
making the real and shadow WAL consistent representations of the same ground truth.

After establishment and shadow-WAL migration, normal WAL-driven operations resume.

**Durable state required:** `PoolerPersistentState.Commitment` (`RecruitmentCommitment{AtTermSeq,
CoordID, ProposedSeq}`) — so a restarted node honours its prior commitments to coordinators.
The `AtTermSeq` field captures "I participated in revoking from term N", and `ProposedSeq`
records the highest target this node has already committed to. This field is already implemented.

### Stage 3.5 — Coordinator cluster state tracking

For the coordinator to act quickly during coordinator-led term change it needs a continuously maintained
view of the cluster: which poolers exist, which are healthy, what rule version each is on, and
which is currently primary.

The coordinator tracks two complementary views of the cluster's durability state:

- **Highest quorum rules** (`ClusterView.HighestQuorumTerm`): the highest `Term.Seq`
  for which the coordinator has confirmed a write quorum. Quorum is confirmed when enough
  non-primary cohort members have reported applying that Seq (or a later one) to satisfy the
  term's `DurabilityPolicy.IsDurable` check. This is the last known-good state of the cluster.

- **Highest seen rules** (`ClusterView.HighestSeenTerm`): the highest `Term.Seq`
  reported by any pooler, regardless of whether it reached write quorum. This may be higher than
  `HighestQuorumTerm` if a leader-driven rule change was in progress when the primary went down.

When `HighestSeenTerm.Seq > HighestQuorumTerm.Seq`, the coordinator knows a partial rule
change exists and must propagate it to quorum before establishing a new primary. When the two
are equal, the cluster is in a clean state and the coordinator can elect a new primary within
the existing cohort without propagating any partial write.

`ClusterView` also carries `PrimaryHealthy`: true when the primary is currently reachable
(postgres running, and not stale under the configured health timeout). The "best-known" primary
is `HighestQuorumTerm.Primary` when a quorum-confirmed version exists, falling back to
`HighestSeenTerm.Primary` otherwise. A coordinator uses this to decide whether to enter the
emergency path: normal writes proceed when `PrimaryHealthy` is true; coordinator-led term change begins
when it is false.

**Health timeout**: the coordinator can be configured with a `healthTimeoutTicks` value. If a
pooler has not sent a status update within that many ticks, it is considered unreachable even
if its last known `pgStatus` was `Running`. Zero (the default) disables timeout-based staleness —
suitable for simulation tests where poolers only broadcast on state change.

### Stage 4 — Graceful primary replacement

Planned maintenance: the stepping-down primary writes a `Term` updating `Primary`
before shutdown, eliminating the need for coordinator-led term change and reducing the switchover window
to near zero.

### Stage 5 — Additional durability policies

`ZoneAwarePolicy` — conjunctive requirement: at least one ACK from the same zone and one from a
different zone. `AtLeastPolicy` cannot express this; requires a new `DurabilityPolicy` implementation.

### Stage 6 — Metrics and observability

Structured event emission from `CoordNode` and `PoolerNode`, aggregated by the simulator for
per-tick assertions: appointment latency, term escalation rate, failover counts.

## Open design questions

### Naming: leader-driven vs coordinator-driven request types

The normal-path request types (`WritePolicyRequest`, `WritePolicyResponseIndicator`, etc.) do
not have a distinguishing prefix, while the coordinator-led-path types (`RecruitRequest`,
`WriteShadowWALRequest`, etc.) do. Consider adding a "leader" or "primary" prefix to the
normal-path types so the origin of each message is self-evident at the call site
(e.g. `LeaderWritePolicyRequest`).

### Abandoned operation follow-up

Several operations can be abandoned mid-flight, each requiring different cleanup:

- **Timed-out leader-driven write**: if a `WritePolicyRequest` is abandoned due to timeout,
  the coordinator should consider whether the primary is in fact unreachable and retry via the
  coordinator-led path rather than waiting indefinitely for a status update.

- **Coordinator-led change abandoned early**: if the coordinator decides to stop before
  completing the propose phase (e.g. it discovers the primary is healthy again), it should
  release recruited nodes by sending a "release" message so they can resume normal quorum
  participation. Without a release, recruited nodes stay withdrawn from write quorum until they
  are recruited again or restart.

- **Divergent timeline recovery**: if a recruited node is found to have a WAL timeline that
  diverged from the current primary (e.g. it was a primary that accepted writes after quorum was
  lost), it cannot simply resume streaming. The coordinator should direct it to either
  `pg_rewind` back to the current primary's timeline or reconnect to a known-good base. This
  requires a new indicator type and pooler handling ("rejoin primary" / "rewind to primary").

### Resume message for stuck nodes

A node may be stuck and unable to make progress without the coordinator telling it the current
quorum term. At least three cases need a "resume" message:

1. **Recruited but no propose received**: the node completed revocation and is waiting in stopped
   state, but the propose phase message never arrived (coordinator crashed or message dropped).
2. **Recruited but no new propose needed**: during the propagate phase the coordinator discovers
   that a previously-started proposal already satisfies the quorum. No fresh propose is required;
   the node just needs to be told the current term so it can apply and resume replication.
3. **Never recruited, lost primary connection**: a node that was not reachable during revocation
   (and so was never recruited) may have lost its connection to the primary in the meantime. It
   is neither receiving WAL nor term updates and needs to be pointed at the current primary.

The resume message should carry the current quorum term so the node can validate and update
`primary_conninfo` if necessary.

### Pruning stale commitment observations

`CoordNode.highestKnownCommitments` accumulates entries for each `atTermSeq` seen. Entries whose
`atTermSeq` is strictly below the highest quorum-confirmed term can never be acted upon and should
be pruned to bound memory growth.

### In-flight postgres state changes

`PoolerNode` currently has no mechanism to prevent multiple concurrent requests that change
postgres state (e.g. `PolicyRecordApplyRequest`, `ApplyWALTermRequest`,
`RevokeParticipationRequest`). Serialising these at the `PoolerNode` level — tracking a single
in-flight state-change request — would be cleaner than relying on the sidecar (`SimPooler`) to
enforce exclusion implicitly.

### LSN visibility in simulation traces

When `DSTSIM_TRACE=1` is set, node state dumps do not include the current WAL LSN. Adding the
LSN (and received LSN for replicas) to the trace output would make it easier to diagnose
replication lag and coordinator-led term change correctness in failing tests.
