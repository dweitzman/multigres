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
     range overlapping the proposed `Seq`.
3. The primary writes a single WAL entry encoding the new term. **This WAL entry must be
   durable under both the outgoing and incoming `DurabilityPolicy` before the transition is
   complete.** This is the fundamental safety invariant of every term transition: a write that
   changes the term must itself be witnessed by enough replicas that neither the old nor the new
   policy can be violated.
4. Once durable under both policies, all subsequent WAL entries are governed solely by the new
   rules.

**Sidecar abstraction:** `PoolerNode` emits a `SidecarApplyLeaderPolicyRequest` when it is ready to
write a rule transition. The sidecar (`SimPooler` in simulation, the real postgres driver in
production) performs the actual SQL and GUC work and responds with an
`SidecarApplyResponseIndicator` once the write is durable. This decouples the state machine from
the postgres execution environment.

```
CoordNode sees observer
  → writes Term to primary (Seq = current.Seq + 1)
  → PoolerNode emits SidecarApplyLeaderPolicyRequest
  → SimPooler/driver writes WAL entry; enforces both-policy quorum
  → SidecarApplyResponseIndicator delivered once durable
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

`SimPooler` intercepts `SidecarApplyLeaderPolicyRequest` (emitted by `PoolerNode`) before it reaches
the `Handler`, simulates the postgres SQL transaction and sync-settings update, and queues an
`SidecarApplyResponseIndicator` for the next `PoolerNode.Step` call once write quorum is met.

## Key types

| Type                    | Description                                                                                                                            |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `NodeProperties`        | Static per-node attributes (e.g. zone). Frozen into `Term.Members` at write time.                                                      |
| `CohortMember`          | `{ID NodeID, Properties NodeProperties}`. Bundles identity and properties so policy evaluation is self-contained.                      |
| `Term`                  | Versioned WAL record: `{Seq, Primary, Members, Policy}`. CAS on monotonic `Seq`.                                                       |
| `DurabilityPolicy`      | Interface: `IsDurable`, `IsAchievable`, `RevokesAndSamplesAllRevocationSets`.                                                          |
| `AtLeastPolicy`         | `DurabilityPolicy` implementation: write is durable when at least N cohort members have ACK'd.                                         |
| `PoolerPersistentState` | Durable pooler state: `{Role, Primary, CachedTerm *Term, Commitment *RecruitmentCommitment, ShadowWAL []*Term}`. Saved before any ack. |
| `PoolerStorage`         | Durable storage interface. Simulation uses `MemStorage`; production uses atomic write-rename + fsync.                                  |
| `PoolerNode`            | Pooler state machine. Handles WAL-driven policy updates and coordinator-led term changes.                                              |
| `CoordNode`             | Coordinator state machine. Drives WAL writes for normal changes; runs coordinator-led term changes when the primary is unreachable.    |

## Message reference

The consensus protocol uses a **request → indicator** split: nodes emit `Request` values from `Step()`, and the `RequestHandler` converts them to `Indicator` values delivered to the target node's next `Step()` call.

Two communication boundaries exist:

- **Network** — messages crossing process boundaries between `PoolerNode` and `CoordNode` instances.
- **Sidecar** — messages staying within a single pooler host, between `PoolerNode` and its local postgres driver (`SimPooler` in tests; a driver goroutine in production).

---

### Network messages — Pooler ↔ Coordinator

#### Write term (normal path): `LeaderWritePolicyRequest` / `LeaderWritePolicyResponseRequest`

Coordinator requests a new `Term` write on the primary. The primary validates the CAS and responds once the WAL entry is durable.

| Direction      | Request type                 | Indicator type                 | Can fail?                                                  |
| -------------- | ---------------------------- | ------------------------------ | ---------------------------------------------------------- |
| Coord → Pooler | `LeaderWritePolicyRequest`         | `LeaderWritePolicyIndicator`         | Yes — `FromSeq` mismatch or primary has a prior commitment |
| Pooler → Coord | `LeaderWritePolicyResponseRequest` | `LeaderWritePolicyResponseIndicator` | —                                                          |

`LeaderWritePolicyResponseIndicator`: `Accepted=true` once durable; `Accepted=false` with `CurrentSeq` so the coord can retry with the correct seq.

In production: a SQL transaction written by multiorch on the primary postgres instance via the multipooler API.

---

#### Recruit pooler (coordinator-led path): `RecruitRequest` / `RecruitResponseRequest`

Coordinator recruits a pooler into a coordinator-led term change covering `(AtTermSeq, ProposedSeq)`. The pooler durably persists its commitment and revokes quorum participation before responding.

| Direction      | Request type             | Indicator type             | Can fail?                                               |
| -------------- | ------------------------ | -------------------------- | ------------------------------------------------------- |
| Coord → Pooler | `RecruitRequest`         | `RecruitIndicator`         | Yes — stale base or superseded by competing coordinator |
| Pooler → Coord | `RecruitResponseRequest` | `RecruitResponseIndicator` | —                                                       |

`RecruitResponseIndicator`: `Accepted=true` with `Position` (LSN + TermSeq at revocation); `Accepted=false` if rejected. Both carry `Commitment` so the coordinator can detect competing coordinators and bump its `ProposedSeq` if needed.

In production: a gRPC call from multiorch to multipooler.

---

#### Propose (write shadow WAL): `ProposeRequest` / `ProposeAckedRequest`

After reaching revocation quorum, the coordinator asks each recruited pooler to persist the new `Term` to shadow WAL. `BaseLSN` anchors the entry to the real WAL timeline. `ApplyNow=true` collapses this and the subsequent GUC-apply step into a single round-trip.

| Direction      | Request type                 | Indicator type                 | Can fail?                                              |
| -------------- | ---------------------------- | ------------------------------ | ------------------------------------------------------ |
| Coord → Pooler | `ProposeRequest`      | `ProposeIndicator`      | Yes — `receivedLSN < BaseLSN` or commitment superseded |
| Pooler → Coord | `ProposeAckedRequest` | `ProposeAckedIndicator` | —                                                      |

`ProposeAckedIndicator`: `Accepted=true` once the shadow WAL entry is durably fsynced.

In production: fsync of the pooler's commitment file; the response is sent over the same gRPC connection.

---

#### Propagate history from another node: `PropagatePositionRequest` / `PropagatePositionAckedRequest`

Coordinator instructs a pooler to replicate `SourceNode`'s committed history up through `TargetPosition`, replacing its own divergent history. Used when a recruited node has fallen behind the promotion candidate.

| Direction      | Request type                    | Indicator type                    | Can fail?                                    |
| -------------- | ------------------------------- | --------------------------------- | -------------------------------------------- |
| Coord → Pooler | `PropagatePositionRequest`      | `PropagatePositionIndicator`      | Yes — `SourceNode` unreachable or copy fails |
| Pooler → Coord | `PropagatePositionAckedRequest` | `PropagatePositionAckedIndicator` | —                                            |

`PropagatePositionAckedIndicator`: `Accepted=true` once durably applied.

In production: `pg_rewind` followed by WAL streaming from `SourceNode`; in simulation the sidecar copies the WAL buffer directly.

---

#### Resume stale node: `ResumeRequest`

Coordinator brings a stuck or stale node up to the quorum-confirmed term. Fire-and-forget: no ack.

| Direction      | Request type    | Indicator type    | Notes       |
| -------------- | --------------- | ----------------- | ----------- |
| Coord → Pooler | `ResumeRequest` | `ResumeIndicator` | No response |

Used when a node missed the propose phase or was unreachable during recruitment and needs to catch up without a full term-change round. In production: a best-effort gRPC call from multiorch; delivery is not confirmed.

---

#### Pooler status broadcast: `PoolerStatusUpdateRequest`

Pooler broadcasts its full state to all coordinators whenever committed state changes (rules write, WAL apply, postgres stop, restart). No response expected.

| Direction      | Request type                | Indicator type          | Notes       |
| -------------- | --------------------------- | ----------------------- | ----------- |
| Pooler → Coord | `PoolerStatusUpdateRequest` | `PoolerStatusIndicator` | No response |

Carries `PoolerPersistentState`, `PostgresStatus`, and `NodeProperties`. In production: a push update on the existing multipooler → multiorch gRPC stream.

---

#### Discovery updates: `PoolerMembershipRequest`

Discovery driver notifies coordinators when poolers register or deregister.

| Direction      | Request type              | Indicator type                                         | Notes       |
| -------------- | ------------------------- | ------------------------------------------------------ | ----------- |
| Driver → Coord | `PoolerMembershipRequest` | `PoolerDiscoveredIndicator` / `PoolerRemovedIndicator` | No response |

In production: driven by an etcd watch stream in multiorch.

---

### Local messages — Pooler ↔ Sidecar

`PoolerNode` emits these to its local sidecar. `SimPooler` intercepts them before they reach the `RequestHandler` and queues a response indicator for the next tick. In production a dedicated driver goroutine handles them.

---

#### Apply leader policy (normal path): `SidecarApplyLeaderPolicyRequest`

Primary asks the sidecar to commit the new `Term` SQL transaction and update `synchronous_standby_names`. `FromSeq` guards against stale applies.

| Direction        | Type                          | Can fail?                                            |
| ---------------- | ----------------------------- | ---------------------------------------------------- |
| Pooler → Sidecar | `SidecarApplyLeaderPolicyRequest`    | Yes — WAL already has a term at or beyond `Term.Seq` |
| Sidecar → Pooler | `SidecarApplyResponseIndicator` | —                                                    |

`SidecarApplyResponseIndicator`: `Accepted=true` once durable under both old and new policies; `Accepted=false` if the SQL transaction failed. The same indicator type is also delivered by the WAL watcher on replicas (always `Accepted=true`).

In production: a goroutine that executes the SQL transaction and waits for WAL confirmation from sync standbys.

---

#### Apply term settings (coordinator path): `SidecarApplyTermSettingsRequest`

When a `ProposeIndicator` arrives with `ApplyNow=true`, the pooler asks the sidecar to apply the `Term`'s GUC settings as though the term had arrived via the real WAL stream. Allows the coordinator to complete shadow-write + GUC-apply in a single round-trip.

| Direction        | Type                          | Can fail?                        |
| ---------------- | ----------------------------- | -------------------------------- |
| Pooler → Sidecar | `SidecarApplyTermSettingsRequest`         | Yes — if postgres is unreachable |
| Sidecar → Pooler | `SidecarApplyResponseIndicator` | —                                |

In production: `ALTER SYSTEM SET ...` + `pg_reload_conf()`.

---

#### Revoke quorum participation (sidecar): `SidecarRevokeParticipationRequest`

Pooler asks the sidecar to withdraw this node from write quorum. Emitted after accepting a `RecruitIndicator`, before responding to the coordinator.

| Direction        | Type                                   | Can fail?                        |
| ---------------- | -------------------------------------- | -------------------------------- |
| Pooler → Sidecar | `SidecarRevokeParticipationRequest`           | Yes — if postgres is unreachable |
| Sidecar → Pooler | `SidecarRevokeResponseIndicator` | —                                |

`SidecarRevokeResponseIndicator`: `Accepted=true` with `LSN` and `TermSeq` captured at the moment of revocation; `Accepted=false` if postgres cannot be put into the required state.

Replica sidecar: stops forwarding WAL ACKs to the primary. Primary sidecar: sets `default_transaction_read_only = on`.

---

#### Graceful shutdown: `TerminateRequest`

Driver signals a pooler to shut down gracefully. The pooler records `PostgresStopped` and emits a final status update.

| Direction       | Request type       | Indicator type       | Notes       |
| --------------- | ------------------ | -------------------- | ----------- |
| Driver → Pooler | `TerminateRequest` | `TerminateIndicator` | No response |

In production: SIGTERM delivered to the multipooler process.

---

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

### Stage 3 — Coordinator-led term change ✅ implemented

When the primary is unreachable, the coordinator runs the recruitment + two-quorum protocol
described in the [Coordinator-led term change](#coordinator-led-term-change) section above.
If a partial leader-led change is discovered in WAL, the coordinator propagates it to quorum
(which may include cohort or policy changes). If no partial change exists, the coordinator
initiates a fresh rule change updating only `Primary`.

**Shadow WAL:** Because postgres is stopped during coordinator-led term change, the coordinator
cannot append to the real WAL. Instead, term transition commitments are recorded per node in
`PoolerPersistentState.ShadowWAL` — a narrow append-only log that can be fsynced safely
without a running postgres. The shadow WAL entry is written before the node acks the recruiter,
so the coordinator only learns of the commitment after it is durable. Once the new primary is
promoted, it copies shadow WAL entries into real postgres WAL before accepting any transactions,
making the real and shadow WAL consistent representations of the same ground truth.

After establishment and shadow-WAL migration, normal WAL-driven operations resume.

`TestCoordLedTermChange` verifies the full protocol under continuous chaos (crashes, drops,
duplicates, reordering). It asserts that the maximum committed `PolicySeq` across all poolers
never decreases and that the coordinator completes at least 500 primary changes.

### Stage 3.5 — Coordinator cluster state tracking ✅ implemented

For the coordinator to act quickly during coordinator-led term change it needs a continuously maintained
view of the cluster: which poolers exist, which are healthy, what rule version each is on, and
which is currently primary.

The coordinator tracks two complementary views of the cluster's durability state:

- **Highest quorum rules** (`ShardStatus.HighestQuorumTerm`): the highest `Term.Seq`
  for which the coordinator has confirmed a write quorum. Quorum is confirmed when enough
  non-primary cohort members have reported applying that Seq (or a later one) to satisfy the
  term's `DurabilityPolicy.IsDurable` check. This is the last known-good state of the cluster.

- **Highest seen rules** (`ShardStatus.HighestSeenTerm`): the highest `Term.Seq`
  reported by any pooler, regardless of whether it reached write quorum. This may be higher than
  `HighestQuorumTerm` if a leader-driven rule change was in progress when the primary went down.

When `HighestSeenTerm.Seq > HighestQuorumTerm.Seq`, the coordinator knows a partial rule
change exists and must propagate it to quorum before establishing a new primary. When the two
are equal, the cluster is in a clean state and the coordinator can elect a new primary within
the existing cohort without propagating any partial write.

`ShardStatus` also carries per-node `NodeHealth`. The "best-known" primary is
`HighestQuorumTerm.Primary` when a quorum-confirmed version exists, falling back to
`HighestSeenTerm.Primary` otherwise. A coordinator uses `NeedsLeaderFailover(status)` to
decide whether to enter the emergency path: normal writes proceed when the primary is healthy;
coordinator-led term change begins when it is not.

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

### Stage 7 — Unsafe term changes

**Scenario:** Too many cohort members have been lost simultaneously, making it impossible to form a
write quorum under the previous term's ack policy. For example, with `AtLeast(3)` and five nodes,
if three nodes are permanently unavailable, no set of survivors can satisfy the outgoing policy, so
the normal coordinator-led protocol cannot safely proceed.

**Protocol:** The coordinator is given an explicit operator override specifying:

- The term to transition off (`AtTermSeq`)
- A declared quorum — a subset of surviving cohort members that the operator asserts is sufficient
  to represent the outgoing write quorum for this specific transition

The coordinator uses this declared set in place of the policy's normal `RevokesAndSamplesAllRevocationSets`
check when verifying the revocation set. The operator is taking explicit responsibility for data loss
if the declared members do not in fact cover all committed writes.

**Recovery paths — operator's choice:**

1. **Preserve durability (safest):** Add new poolers to the cohort and keep the original ack policy.
   The new cohort members bring the term back to the original quorum requirement automatically as
   they join and start ACKing.

2. **Restore availability at lower durability:** Reduce the ack policy to match the surviving cohort
   size (e.g., `AtLeast(2)` instead of `AtLeast(3)`). The coordinator can be configured with a
   `targetPolicy` — once enough cohort members are available again, the coordinator automatically
   escalates the policy back to the target without further operator action. The reduced policy is a
   temporary concession, not a permanent downgrade.

Unsafe term changes require explicit operator authorisation and should be logged prominently, since
they are inherently lossy when the unavailable nodes held committed writes not covered by the
declared quorum.

### Stage 8 — Provisioning lineage

**Scenario:** Scaling a shard to zero — or replacing the entire cohort — requires a clean handoff
between two independent generations of provisioning. Without lineage tracking, a stale provisioner
from a previous generation could instruct a new cohort to replicate from an incompatible base backup.

**Scale down:** The current provisioner revokes write access via the coordinator-led protocol and takes
an up-to-date backup at a known LSN. This backup LSN becomes the _handoff LSN_ — the minimum safe
base for any future cohort.

**Scale up:** A new provisioner must either:

- Start all nodes from a backup at or beyond the handoff LSN, so they can stream WAL from the new
  primary and converge to a consistent state, or
- Run `initdb` on all nodes (valid when no prior data needs to be carried forward — essentially a
  fresh cluster).

The resource/pod provisioner is the only entity that knows which path is required and what the
minimum safe backup LSN is. This cannot be encoded safely in the consensus term alone.

**Proposal:** Each provisioner generates a random _provisioning ID_ at startup and stamps it on all
policy instructions it issues — for example, "observers joining this cohort must restore from a
backup at least as new as LSN X." Poolers validate the provisioning ID before accepting such advice,
ignoring instructions from a different generation. A new provisioner explicitly supersedes the
previous one by committing a term update that installs the new provisioning ID, revoking the
previous generation's authority.

This bounds the blast radius of a stale or restarted provisioner: its instructions are ignored by
any pooler that has already enrolled in the new generation.

## Open design questions

### Abandoned operation follow-up

Several operations can be abandoned mid-flight, each requiring different cleanup:

- **Timed-out leader-driven write**: if a `LeaderWritePolicyRequest` is abandoned due to timeout,
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
postgres state (e.g. `SidecarApplyLeaderPolicyRequest`, `SidecarApplyTermSettingsRequest`,
`SidecarRevokeParticipationRequest`). Serialising these at the `PoolerNode` level — tracking a single
in-flight state-change request — would be cleaner than relying on the sidecar (`SimPooler`) to
enforce exclusion implicitly.

### Pre-flight feasibility check before coordinator-led term change ✅ implemented

Before starting recruitment (which revokes quorum participation from nodes), the coordinator
verifies that it has enough healthy nodes to complete the failover. Revoking nodes that turn
out to be reachable can make the cluster temporarily worse than it already was.

`CanAttemptFailover(status ShardStatus) bool` in `HighAvailabilityStrategy` performs two checks:

1. **Determine the effective post-failover durability policy.** If `HighestSeenTerm.Seq >
HighestQuorumTerm.Seq`, a partial leader-led change exists: the coordinator will be obligated
   to propagate it to quorum, which means the post-failover policy is `HighestSeenTerm.Policy`.
   Otherwise the post-failover policy is `HighestQuorumTerm.Policy`.

2. **Check achievability against currently healthy nodes.** Calls
   `policy.IsAchievable(healthyNodes)` where `healthyNodes` is the subset of the post-failover
   cohort whose `NodeHealth` the coordinator considers reachable. If the policy cannot be
   satisfied by the known-healthy nodes alone, the coordinator skips recruitment —
   it has no evidence that the failover can succeed, and premature revocation could prevent
   the cluster from recovering on its own once connectivity is restored.

`TestCoordDoesNotRecruitWhenFailoverIsInfeasible` exercises this path: a network partition
isolates {node1, node2} from {node3, coord}. The coordinator can only see node3 — one node
short of `AtLeast(2)` — and correctly refrains from recruiting.

### LSN visibility in simulation traces

When `DSTSIM_TRACE=1` is set, node state dumps do not include the current WAL LSN. Adding the
LSN (and received LSN for replicas) to the trace output would make it easier to diagnose
replication lag and coordinator-led term change correctness in failing tests.
