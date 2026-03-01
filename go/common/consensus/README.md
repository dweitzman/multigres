# consensus

Pure state machine library for safely driving appointment and re-appointment of write quorums in a
Multigres PostgreSQL cluster.

`OrchNode` executes the three-phase protocol that appoints a primary and sync-replica set. A related
but distinct concern is _deciding when re-appointment is needed_ — for example, determining that the
current primary is unhealthy or that the write quorum is no longer satisfiable. That decision
involves cluster-health evaluation that is not inherently a consensus decision and should be kept as
separate as practical from the appointment protocol itself. The goal is that both concerns can be
reasoned about and simulated independently.

The two node types — `OrchNode` and `PoolerNode` — implement the `dstsim.Node` interface, so they
can be registered in the same simulator and exercised under deterministic, reproducible chaos
without any real I/O or network.

## Algorithm overview

The algorithm runs a **three-phase protocol** within a monotonically increasing _voting term_:

```
Begin → Revoke → Establish
```

1. **Begin** — the orch establishes itself as the coordinator for this term by collecting votes
   from a strict majority of known poolers. A pooler accepts the first coordinator it sees at a
   given term and rejects any later one.

2. **Revoke** — the orch revokes the previous primary by broadcasting `PrimaryTerm=0, Primary=""`.
   Revocation is complete when the `DurabilityPolicy` quorum reports `IsRevoked=true`: either the
   old primary applied the revoke, or so many of its sync replicas applied it that the required
   number of write acks is no longer achievable.

3. **Establish** — the orch calls `DurabilityPolicy.ProposeQuorum` to select a new primary and
   sync-replica set. Establishment is complete when the quorum reports `IsEstablished=true`:
   the new primary and enough sync replicas have all reported `Applied=true`.

If any phase fails to achieve quorum within `appointmentPhaseTimeoutTicks`, the orch escalates the voting term
and restarts the appointment from Begin.

**Term vocabulary.** The protocol uses two distinct term numbers that serve different purposes and
must not be confused:

- **Coordinator term** (currently named `VotingTerm` / `VotedTerm` in code — planned rename) — the
  orch's election counter. It increments on every Begin attempt: retries, orch restarts, and
  competing coordinators all advance it. All three phases of a single appointment cycle share the
  same coordinator term. A high coordinator term means many election rounds have happened; it says
  nothing by itself about the state of postgres replication.
- **Primary term (`PrimaryTerm`)** — the replication quorum epoch. It is set to the coordinator
  term at which the current primary was _successfully established_ via the Establish phase. It only
  advances when a new primary is appointed. Multiple coordinator-term increments (Begin retries,
  orch crashes) can occur without changing the primary term or touching postgres replication at all.

This distinction is critical in `learnEstablishedQuorum` and anywhere the code reasons about
whether a replication quorum is still intact: that is a question about `PrimaryTerm`, not the
coordinator term. When an orch discovers a competing coordinator has a higher term, it backs
off for a jittered interval before retrying, giving the winning coordinator time to finish.

**Rejection signals and stale-view recovery.** When a pooler rejects a proposal it includes a
`RejectionReason` hint and, for rejections that imply the orch has a stale view (`StaleTerm`,
`PrimaryTermMismatch`, `CohortMembership`), a piggybacked `FreshStatus` snapshot of its current
committed state. The orch processes the fresh status atomically with the rejection so its
`knownPoolers` view is updated before the next appointment cycle begins, without requiring an
extra round-trip to receive a separate status broadcast.

**TODO(retry):** Currently the orch always backs off after a rejection, even when the reason was a
correctable stale view rather than a race with a competing coordinator. An improvement would be to
skip the backoff when: (a) the rejection reason is `StaleTerm`, `PrimaryTermMismatch`, or
`CohortMembership`; and (b) the piggybacked fresh status shows no competing coordinator is active
at the higher term. This would allow the orch to retry the appointment immediately with up-to-date
information, reducing recovery latency in the common "orch just restarted" case.

A **pooler** commits each accepted proposal to durable storage _before_ responding (safety: votes
survive crashes). The role change (pg_ctl promote, reconfigure standby) is applied separately by
an external apply loop, which delivers an `ApplySucceededIndicator` when the postgres operation
completes. The state machine persists `Applied=true` before broadcasting the status change to orchs,
so the orch only sees `Applied=true` after it is durably stored. Committed state and applied state
are tracked independently so a crash between the two phases is safe to resume.

## Key types

| Type                      | Description                                                                                                                                   |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `OrchNode`                | Coordinator state machine. All state is ephemeral — a restarted orch re-learns the cluster from pooler status updates.                        |
| `PoolerNode`              | Pooler state machine. Committed state is persisted via `PoolerStorage`; `Applied=true` is persisted when the apply loop signals completion.   |
| `ConsensusState`          | The complete cluster configuration at a given term+seq+phase (primary, quorum spec, cohort members, coordinator). Broadcast by orch.          |
| `PoolerPersistentState`   | Durable pooler state: voted term, role, applied flag, quorum spec, cohort membership. Saved to disk before any acknowledgement.               |
| `DurabilityPolicy`        | Selects a write quorum given full cluster context (health, LSN, zone). Returns a `Quorum` the orch treats as opaque.                          |
| `Quorum`                  | Self-contained quorum snapshot: knows its primary, sync replicas, and how to evaluate `IsEstablished`, `IsRevoked`, `IsWriteQuorum`.          |
| `QuorumCandidate`         | Per-node input to `DurabilityPolicy.ProposeQuorum`: ID, health, LSN, and zone.                                                                |
| `PoolerStorage`           | Interface for durable persistence. Simulation uses `memStorage`; production uses `AtomicStateFile` (write-rename + fsync).                    |
| `ApplySucceededIndicator` | Delivered to `PoolerNode` by the apply loop (goroutine in production, `applyDriverNode` in simulation) when a postgres role change completes. |

## Running the simulation tests

```bash
# All consensus simulation tests
go test ./go/common/consensus/...

# Run a specific test
go test -run TestChaosNetwork_1000Crashes ./go/common/consensus/...

# Run with race detector
go test -race ./go/common/consensus/...

# Check for flakiness (run 10 times with different seeds)
go test -count=10 ./go/common/consensus/...
```

## Tracing

Set `DSTSIM_TRACE=1` to dump a full per-tick trace to stderr when a test fails. This is useful
for diagnosing which indicators were dropped, which nodes stepped, and what requests were emitted.

```bash
DSTSIM_TRACE=1 go test -run TestFlakyApply_1000Failovers ./go/common/consensus/... -v
```

The trace shows, for each non-empty tick:

- Which nodes stepped and what indicators they received / requests they emitted
- Indicators dropped by the delivery policy (network drops or because the target was stopped)
- Indicators delayed by more than 1 tick (chaos network)
- Network partition transitions
- Any assertion violations

## Simulation scenarios

| Test                                    | Delivery policy                                        | Fault injection                                        | Goal                                                        |
| --------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------ | ----------------------------------------------------------- |
| `TestHappyPath_PrimaryElected`          | Fast (1-tick, reliable)                                | None                                                   | A primary is appointed within 200 ticks                     |
| `TestPrimaryPooler_1000Crashes`         | Fast                                                   | Primary crash+restart (random target, 5-tick downtime) | 1000 crash→recovery cycles without safety violations        |
| `TestFlakyApply_1000Failovers`          | Fast                                                   | Crash + 50% apply failure rate                         | 1000 cycles; confirms the apply-retry path is exercised     |
| `TestChaosNetwork_PrimaryElected`       | Unreliable (10% drop, 5-tick delay, 3% partition rate) | None                                                   | A primary is appointed within 5000 ticks despite chaos      |
| `TestChaosNetwork_1000Crashes`          | Unreliable                                             | Crash+restart                                          | 1000 cycles under combined network chaos + crashes          |
| `TestOrchCrash_1000PreEstablishCrashes` | Fast                                                   | Orch crash between Begin and Establish                 | 1000 mid-appointment orch crashes without safety violations |

All tests register the **standard invariants** via `sim.Always()`:

- **`atMostOneQuorum`** — the core safety property. Checks _effective state_ (what postgres is
  actually running with on disk right now): at most one primary may simultaneously hold a write
  quorum. Uses `DurabilityPolicy.ReconstructQuorum` + `Quorum.IsWriteQuorum` to evaluate the
  historical committed sync-replica set, not what the policy would propose today. It is valid for
  two nodes to have `committed.Role=Primary` simultaneously (a stale primary that hasn't received
  Revoke yet) as long as only one has replicas satisfying `IsWriteQuorum`.

- **`appliedMonotonicity`** — once `Applied=true` is persisted for a proposal (`VotedTerm` +
  `VotedSeqNum`), it must never revert to false for that same proposal. A new (higher) proposal
  superseding it is the only valid transition.

## Simulation architecture

```
  DiscoveryNode    ApplyDriverNode(s)    OrchNode(s)           PoolerNode(s)
      │                   │                  │                      │
      │ PoolerDiscovered ─┼─────────────────▶│                      │
      │                   │                  │ BroadcastState ─────▶│
      │                   │                  │◀─── PoolerResponse ──│
      │                   │                  │◀─── PoolerStatus ────│
      │                   │ ApplySucceeded ──┼──────────────────────▶│  (local, bypasses chaos)
      │                   │                  │                      │
      └─────────────────────── all routed via consensusHandler (RequestHandler) ──────────────┘
```

The `consensusHandler` (in `simulation_test.go`) converts requests to indicators:

- `BroadcastStateRequest` → `OrchStateIndicator` to each pooler
- `PoolerResponseRequest` → `PoolerResponseIndicator` to the target orch
- `PoolerStatusUpdateRequest` → `PoolerStatusIndicator` to all orchs
- `PoolerMembershipRequest` → `PoolerDiscoveredIndicator`/`PoolerRemovedIndicator` to all orchs
- `ApplySucceededRequest` → `ApplySucceededIndicator` to the target pooler

All message delivery goes through the active `IndicatorDeliveryPolicy` — the default is
`FastNetwork` (reliable, 1-tick latency). Tests that exercise the chaos path use
`consensusChaosPolicy` which applies different policies per traffic class:

- `TerminateIndicator`: always delivered, 1-tick latency (models SIGTERM)
- `ApplySucceededIndicator`: always delivered, 1-tick latency (local: same process in production)
- Discovery traffic: ordered+reliable, up to 5-tick delay (models etcd watch stream)
- Orch↔pooler traffic: `UnreliableNetwork` with drops, delays, and random partitions

## Production wiring

See `examples/pg_driver/` for the production integration sketch:

- `AtomicStateFile` — crash-safe `PoolerStorage` using write-rename + fsync
- `PostgresApplier` — skeleton with documented apply steps (ALTER SYSTEM, pg_ctl promote / standby
  reconfigure, pg_reload_conf); not yet wired to real postgres
- `OrchDriver` / `PoolerDriver` — 100ms tick loops that buffer gRPC events and call `Node.Step()`.
  After each tick, `PoolerDriver` checks whether the committed role change needs applying and spawns
  an apply goroutine that writes `ApplySucceededIndicator` onto the incoming channel when done.

## What's next

### Stage 1 — Harden existing protocol (current focus)

Small improvements that make the existing code more correct and configurable:

- ✅ **Configurable quorum** — `DurabilityPolicy` interface replaces the hardcoded `syncReplicaQuorum=1`.
  `AnyNPolicy(n)` implements "any N of the sync replicas must ACK". Revocation and establishment
  are delegated to `Quorum.IsRevoked` / `Quorum.IsEstablished`; safety invariants use
  `Quorum.IsWriteQuorum` via `DurabilityPolicy.ReconstructQuorum`.
- ✅ **Late-response filtering** — `PoolerResponseRequest` and `PoolerResponseIndicator` now carry
  `VotingTerm` + `SeqNum`; orch discards responses that don't match the current proposal.
- ✅ **Crash orch nodes in simulation** — the crash driver crashes both poolers and orchs.
  `OrchNode.Restart()` clears all ephemeral state; `discoveryNode` detects the gap by diffing
  `KnownPoolerIDs()` against the live pooler set and re-delivers membership events through the
  normal ordered delivery path. `TestOrchCrash_1000PreEstablishCrashes` asserts 1000 mid-appointment
  orch crashes (after Begin, before Establish) without safety violations.
- ✅ **Strengthen `atMostOneQuorum`** — enumerates all 2^k state combinations across poolers
  with a pending unapplied change (k = number of such nodes, capped at 5 before falling back
  to the effective-state check). Each node independently picks its committed (goal) or
  last-applied (current) state; the invariant must hold for every combination.
- ✅ **Indicator-driven apply loop** — removed `RoleApplier` interface; apply is now driven
  externally. The apply goroutine (production) or `applyDriverNode` (simulation) delivers
  `ApplySucceededIndicator{VotedTerm, VotedSeqNum}` on success; `PoolerNode` persists
  `Applied=true` before broadcasting. Crash recovery reads `committed.Applied` from storage
  directly — no separate `AppliedState()` needed.
- ✅ **Realistic gRPC wiring in `pg_driver.go`** — the production sketch shows the correct
  topology: orch owns all outbound connections (pooler never dials orch); `ProposeState` (unary)
  carries state changes with an accept/reject reply; health updates (poll or stream) carry committed
  state, applied flag, and postgres health back to the orch.
- **Wire up `PostgresApplier`** — fill in `apply()` (ALTER SYSTEM, pg_ctl promote / standby
  reconfigure, pg_reload_conf) in `examples/pg_driver/`.
- **TODO: rename `VotingTerm`/`VotedTerm` → `CoordTerm`** throughout the code to match the
  "coordinator term" vocabulary above and distinguish it clearly from `PrimaryTerm`.
- **TODO: rename and reframe `CohortMember`** — see Stage 2 for the full rationale.
- **TODO: fix `learnEstablishedQuorum` grouping key** — currently groups poolers by their current
  `(VotedTerm, VotedSeqNum)`, which reflects the most-recently voted proposal (possibly a Begin at
  a higher term). After a Begin-only cycle (no Establish), poolers advance `VotedTerm` but
  `QuorumSpec` and `Applied=true` remain from the old Establish. The correct grouping key is
  `PrimaryTerm` (the voting term at which the Establish ran), combined with `Applied=true` on all
  poolers sharing that `PrimaryTerm`. Until fixed, `learnEstablishedQuorum` may fail to recognise
  a still-intact replication quorum after multiple Begin-only cycles, causing an unnecessary
  re-appointment instead of simply tracking the already-appointed primary.
  **This is a liveness gap, not a safety gap** — see the cohort-change safety invariant in Stage
  2.5 for why learning potentially-stale quorum state from replicas is safe: the CohortMembers
  validation in Begin prevents any orch with stale cohort knowledge from successfully changing the
  cluster topology, so an unnecessary re-appointment at worst causes a brief extra failover cycle.
- **TODO: add simulation test for bootstrap from scratch** — run 1000 rounds with different seeds
  under chaos. Assert that bootstrap always eventually completes as long as a quorum of
  bootstrap-eligible nodes is sometimes reachable, even if some are stopped and replaced before
  bootstrap finishes.
- **TODO: add simulation test for second orch joining an established cluster** — assert that
  a newly-started orch correctly learns the existing quorum without disturbing it, and that chaos
  (delayed status updates, partitions) does not cause it to incorrectly compete.
- **TODO: add simulation test demonstrating the `learnEstablishedQuorum` liveness gap** — start
  orch with an unreachable primary and confirm that it correctly falls back to a new appointment
  rather than hanging indefinitely.
- ✅ **Bootstrap safety via `CohortMembers`** — see Stage 2 below.

### Stage 2 — Bootstrapping

✅ Bootstrap safety is implemented. The mechanism relies on `CohortMembers` in `ConsensusState`
and `CohortMember bool` in `PoolerPersistentState`:

- On **Establish** proposals the orch lists all currently known poolers in `CohortMembers`. A
  pooler that receives this proposal and finds its own ID in the list sets `CohortMember=true` in
  its durable state (sticky — never cleared).

- On **Begin** and **Revoke** proposals the orch populates `CohortMembers` from the poolers that
  have already reported `CohortMember=true` in their status. A fresh bootstrap orch (no prior
  confirmed quorum) has not yet received these status updates, so it sends an empty list.

- A pooler with `CohortMember=true` **rejects any proposal** whose `CohortMembers` list does not
  include its own ID. An empty list is the signal that the orch is performing a bootstrap (it
  hasn't discovered the existing cohort yet). The orch must first call `learnEstablishedQuorum`
  and receive pooler status updates before it will include the full cohort in its proposals.

No explicit `Bootstrap bool` flag is needed: the absence of the pooler's own ID from
`CohortMembers` is a sufficient and reliable signal. A high coordinator term alone is not
sufficient protection because bootstrap terms could keep incrementing and a high term must never
override an already-established cohort.

**TODO: reframe and rename `CohortMember`.** The name implies general cohort membership tracking,
but its actual and intended purpose is narrower: **bootstrap protection**. The field records
"this pooler has witnessed a successful Establish and knows a real cluster exists; it must not
silently participate in a fresh bootstrap that would overwrite that cluster." A clearer name
(e.g. `Bootstrapped`) would make this intent obvious and avoid scope creep into cohort admission
protocols that aren't needed for the protection to work. Under the cleaner framing:

- A node sets the flag as soon as it observes any successful Establish (not just one that lists
  its own ID) — this is the "viral" property: once any node in the cluster knows the cluster
  exists, it will refuse naive bootstrap attempts.
- A node leaving the cohort does _not_ need to unset the flag: the flag means "I know this
  cluster exists", not "I am currently a voting member". That distinction collapses the
  complexity in Stage 2.5's removal protocol.

**Cohort admission** (tracked below in the roadmap) is a separate concern: once the cluster is
bootstrapped, admitting new nodes to the _voting_ cohort must itself be a consensus decision.
Until that is implemented, nodes are added directly in simulation and the orch accepts them
without a cohort-admission round.

### Stage 2.5 — Cohort membership changes

Once a cluster is bootstrapped, adding or removing nodes from the _voting_ cohort is itself a
consensus decision. The designed protocol for cohort changes:

**Adding a node:**

1. The orch starts a new voting term. The `Begin` proposal carries the current cohort in
   `CohortMembers` (proof that the orch knows the existing membership). Existing cohort members
   will reject proposals that omit them, so this proof is mandatory.
2. If the write quorum is still satisfiable without revoking (e.g., adding a replica when the
   existing sync-replica set remains intact), the orch may skip the Revoke phase entirely.
3. The `Establish` proposal lists the updated `CohortMembers` including the new node. Poolers that
   see their own ID in this list set `CohortMember=true` (sticky).
4. **Postgres-backed durability:** the new cohort membership is durably committed when the primary
   writes the cohort record to postgres and that write streams to a quorum of sync replicas via
   WAL. The primary sets `Applied=true` only after the cohort WAL record has been received and
   acknowledged by the required sync replicas. This means the cohort change survives a primary
   crash: the next orch restart will learn the new cohort from the replica status reports.

**Removing a node:**

The same voting protocol applies, but the `Establish` proposal omits the departing node from
`CohortMembers`. Normally a pooler with `CohortMember=true` rejects proposals that exclude it
(bootstrap protection). For intentional removal, this stickiness is relaxed: a pooler that already
voted for `Begin` at the same `VotingTerm` and `CoordID` as the arriving `Establish` knows the
orch proved cohort knowledge at `Begin` time, so it accepts the `Establish` even if it is excluded.
This is the signal that the orch is intentionally shrinking the cohort rather than bypassing it.

**What changes — and what doesn't:**

Cohort membership changes use the normal three-phase protocol with the same safety guarantees.
However, if the quorum composition (primary, sync-replica set, ack threshold) is unchanged, the
`QuorumSpec` in `Establish` remains the same and postgres replication settings do not need to be
reconfigured. The primary still marks `Applied=true` only after the cohort WAL record reaches sync
durability — even if no postgres role change is needed.

**Relationship between replication and voting:**

Being a _replicating_ node and being a _voting_ cohort member are distinct. A newly discovered
pooler may begin streaming WAL from the current primary immediately, but it does not participate
in voting (and does not count toward quorum) until it has been formally admitted via an `Establish`
that lists it in `CohortMembers`. Whether a node should join the voting cohort (vs. remaining a
non-voting replica) is a policy decision outside the consensus state machine.

**Key safety invariant — cohort changes require existing-quorum consent:**

A cohort membership change (adding or removing a node) can only happen through an Establish that
was preceded by a Begin listing the _current_ cohort in `CohortMembers`. Because existing cohort
members reject any Begin that omits them, no orch can change the cohort without first proving
knowledge of the existing membership to every current member.

Consequence: **if an orch can reach a quorum of the existing cohort members, their reported
cohort view is authoritative**, even if the primary is unreachable. No other orch could have
silently added new members or moved the primary to a different cohort without those survivors
knowing — such a change would have required them to vote on it. The orch can therefore safely
Begin+Revoke using just the reachable surviving members, relying on the Begin validation for
safety rather than needing to verify the quorum directly from the primary.

This is also why `learnEstablishedQuorum`'s correctness gap is a _liveness_ problem rather than
a _safety_ problem. An orch that can't confirm the current quorum from the primary will fall back
to a new appointment — which may be unnecessary, but cannot result in split-brain: the CohortMembers
check at Begin time ensures the orch cannot proceed unless it has proved knowledge of the full
current cohort.

**Cohort changes must be separate from replication-topology changes:**

A cohort membership change and a replication-topology change (e.g. promoting a new primary) must
not happen in the same coordinator term. The Establish that changes `CohortMembers` must use a
`QuorumSpec` that reflects the _pre-existing_ replication topology. A subsequent term then makes
any needed replication change using the updated cohort. This two-step requirement ensures that
every cohort member always has a consistent view of both membership and replication state —
you never need to reason about partial visibility of a combined change.

**Future idea — revoked-quorum cutoff in Establish proposals:**

An orch completing an Establish at coordinator term T implicitly revokes all primary terms before
T. It could optionally broadcast this fact explicitly (e.g. a `RevokedUpToTerm` field in
`ConsensusState`), letting newly-started orchs quickly determine which quorum state is definitively
stale without needing to reason from pooler status alone. This is not required for correctness
given the cohort-consent invariant above, but could simplify diagnostics and speed up orch restart
in clusters with a long coordinator-term history.

**Current status:** simulation adds nodes directly and the orch accepts them without a
cohort-admission round; that shortcut is fine for testing the appointment protocol but must be
addressed before production use. The CohortMember stickiness relaxation for intentional removal
is designed but not yet implemented in `PoolerNode`.

### Stage 3 — Re-bootstrapping

Safely restart a cluster after all poolers are replaced (e.g. restore from backup). The new orch
starts at `lastKnownTerm + 1` (derived from backup metadata or a quorum of restored poolers) to
avoid split-brain with any ghost processes from the old cluster.

### Stage 4 — Graceful primary replacement

Replace the primary without an availability gap — either because the current primary wants to step
down (planned maintenance) or because an operator wants to promote a specific replica. The
stepping-down pooler emits a `StepDownRequest`; orch immediately starts a new appointment without
waiting for a timeout. A `PromoteRequest` sets a `preferredPrimary` hint that biases
`selectPrimary()` toward the target, then follows normal Revoke → Establish.

### Stage 5 — Additional durability policies

The `DurabilityPolicy` / `Quorum` interface is fully wired (Stage 1) and `QuorumSpec`
serialization is implemented (`Quorum.Serialize` / `DurabilityPolicy.DeserializeQuorum`).
`SyncReplicas` has been removed from both `ConsensusState` and `PoolerPersistentState`; postgres
`synchronous_standby_names` is derived via `policy.DeserializeQuorum(committed.QuorumSpec).PostgresConfig()`.

Stage 5 adds:

- **`ZoneAwarePolicy`** — at least one ACK from the same zone and one from a different zone
  (conjunctive requirement that `AnyNPolicy` cannot express). Requires extending `anyNQuorumSpec`
  or adding a separate type-tagged JSON format.
- Additional policies as operational needs evolve

### Stage 6 — Metrics and observability

The protocol produces a rich stream of events (appointment started, term escalated, phase timed
out, quorum met, primary stopped, etc.) that are valuable both for production monitoring and for
simulation assertions. The open design question is where metrics support lives:

**Option A — Client-side only.** `OrchNode` and `PoolerNode` remain pure state machines with no
metrics hooks; callers instrument what they care about by inspecting the indicators they deliver
and the requests they receive. Clean separation of concerns; `dstsim` stays a generic library.

**Option B — Native event emission in `dstsim`/`consensus`.** The simulator (or the nodes
themselves) emit structured events alongside normal request/indicator flow. This enables:

- The simulator tracking metrics across a full run (appointment latency, timeout counts,
  failover reasons, term escalation rates) without requiring each test to instrument manually.
- Simulation conditions and `sim.Always()` assertions that reference metrics (e.g. "at most N
  term escalations per 100 ticks", "appointments always complete within K ticks").
- Visualising simulation traces by metric rather than raw tick-by-tick output.
- Catching "should be impossible" situations (e.g. simultaneous primaries lasting > 1 tick)
  as metric-threshold assertions rather than boolean invariants.

The argument for Option B is that the simulator is already the natural place to aggregate
cross-node, cross-tick state; adding a lightweight structured event channel doesn't violate
the pure-state-machine property of the nodes themselves (events are emitted as an output, like
requests). A minimal first step would be a `sim.Metrics()` accessor that exposes per-node
counters the simulator collects passively from the request stream, without requiring node code
changes.

### Stage 7 — Timeline / WAL support (deferred)

Handle WAL timelines safely in emergency failovers: no committed writes lost, minimal pg_rewind,
safe promotion rules when multiple orchs compete. Do not start until Stages 1–5 are solid.
