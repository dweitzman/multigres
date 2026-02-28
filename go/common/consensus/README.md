# consensus

Pure state machine library for distributed primary selection in a Multigres PostgreSQL cluster.

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
   Revocation is complete when at least one _revocation set_ has all members reporting `Applied=true`.
   A revocation set is any pooler subset whose acceptance guarantees the old primary can no longer
   commit writes (the primary itself, or the full set of sync replicas).

3. **Establish** — the orch appoints a new primary and sync-replica set. Establishment is complete
   when the new primary and at least `syncReplicaQuorum` sync replicas have all reported
   `Applied=true`.

If any phase fails to achieve quorum within `electionTimeoutTicks`, the orch escalates the term
and restarts from Begin. When an orch discovers a competing coordinator has a higher term, it backs
off for a jittered interval before retrying, giving the winning coordinator time to finish.

A **pooler** commits each accepted proposal to durable storage _before_ responding (safety: votes
survive crashes). The role change (pg_ctl promote, reconfigure standby) is applied separately,
on each tick, until the `RoleApplier` reports success. Committed state and applied state are
tracked independently so a crash between the two phases is safe to resume.

## Key types

| Type                    | Description                                                                                                                                 |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `OrchNode`              | Coordinator state machine. All state is ephemeral — a restarted orch re-learns the cluster from pooler status updates.                      |
| `PoolerNode`            | Pooler state machine. Committed state is persisted via `PoolerStorage`; applied state is recovered via `RoleApplier.AppliedState()`.        |
| `ConsensusState`        | The complete cluster configuration at a given term+seq+phase (primary, sync replicas, coordinator). Broadcast by orch, voted on by poolers. |
| `PoolerPersistentState` | Durable pooler state: voted term, role, applied flag, current primary. Saved to disk before any acknowledgement.                            |
| `PoolerStorage`         | Interface for durable persistence. Simulation uses `memStorage`; production uses `AtomicStateFile` (write-rename + fsync).                  |
| `RoleApplier`           | Interface for operational role execution. Simulation uses `flakyApplier`; production uses `PostgresApplier` (pg_ctl, ALTER SYSTEM, etc.).   |

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

| Test                              | Delivery policy                                        | Fault injection                                        | Goal                                                    |
| --------------------------------- | ------------------------------------------------------ | ------------------------------------------------------ | ------------------------------------------------------- |
| `TestHappyPath_PrimaryElected`    | Fast (1-tick, reliable)                                | None                                                   | A primary is elected within 200 ticks                   |
| `TestPrimaryPooler_1000Crashes`   | Fast                                                   | Primary crash+restart (random target, 5-tick downtime) | 1000 crash→recovery cycles without safety violations    |
| `TestFlakyApply_1000Failovers`    | Fast                                                   | Crash + 50% apply failure rate                         | 1000 cycles; confirms the apply-retry path is exercised |
| `TestChaosNetwork_PrimaryElected` | Unreliable (10% drop, 5-tick delay, 3% partition rate) | None                                                   | A primary is elected within 5000 ticks despite chaos    |
| `TestChaosNetwork_1000Crashes`    | Unreliable                                             | Crash+restart                                          | 1000 cycles under combined network chaos + crashes      |

All tests register the **standard invariants** via `sim.Always()`:

- **`atMostOneQuorum`** — the core safety property. Checks _effective state_ (what postgres is
  actually running with on disk right now): at most one primary may simultaneously hold a write
  quorum (primary + at least `syncReplicaQuorum` sync replicas configured to stream from it).
  It is valid for two nodes to have `committed.Role=Primary` simultaneously (a stale primary
  that hasn't received Revoke yet) as long as only one has replicas streaming to it.

- **`appliedMonotonicity`** — once `Applied=true` is persisted for a proposal (`VotedTerm` +
  `VotedSeqNum`), it must never revert to false for that same proposal. A new (higher) proposal
  superseding it is the only valid transition.

## Simulation architecture

```
  DiscoveryNode          OrchNode(s)           PoolerNode(s)
      │                      │                      │
      │ PoolerDiscovered ───▶│                      │
      │                      │ BroadcastState ─────▶│
      │                      │◀─── PoolerResponse ──│
      │                      │◀─── PoolerStatus ────│
      │                      │                      │
      └──── all routed via consensusHandler (RequestHandler) ────┘
```

The `consensusHandler` (in `simulation_test.go`) converts requests to indicators:

- `BroadcastStateRequest` → `OrchStateIndicator` to each pooler
- `PoolerResponseRequest` → `PoolerResponseIndicator` to the target orch
- `PoolerStatusUpdateRequest` → `PoolerStatusIndicator` to all orchs
- `PoolerMembershipRequest` → `PoolerDiscoveredIndicator`/`PoolerRemovedIndicator` to all orchs

All message delivery goes through the active `IndicatorDeliveryPolicy` — the default is
`FastNetwork` (reliable, 1-tick latency). Tests that exercise the chaos path use
`consensusChaosPolicy` which applies different policies per traffic class:

- TerminateIndicators: always delivered, 1-tick latency
- Discovery traffic: ordered+reliable, up to 5-tick delay (models etcd watch stream)
- Orch↔pooler traffic: `UnreliableNetwork` with drops, delays, and random partitions

## Production wiring

See `examples/pg_driver/` for the production integration sketch:

- `AtomicStateFile` — crash-safe `PoolerStorage` using write-rename + fsync
- `PostgresApplier` — `RoleApplier` skeleton (apply steps are documented but not yet wired)
- `OrchDriver` / `PoolerDriver` — 100ms tick loops that buffer gRPC events and call `Node.Step()`

## What's next

### Stage 1 — Harden existing protocol (current focus)

Small improvements that make the existing code more correct and configurable:

- **Configurable quorum** — `syncReplicaQuorum` is hardcoded to `1` in `orch.go`. Make it a
  field on `OrchNode` and generalize `revocationSets()` for k > 1 sync-replica quorums.
- **Late-response filtering** — `PoolerResponseRequest` lacks a `SeqNum`, so orch can't tell if a
  response is for the current proposal or a stale one. Add `SeqNum`; orch ignores mismatches.
- **Crash orch nodes in simulation** — the crash driver currently only targets poolers. Extend it
  to also crash orch nodes; extend `DiscoveryNode` to re-deliver discovery events to a restarted
  orch (since orch state is ephemeral).
- **Strengthen `atMostOneQuorum`** — also check whether any pooler with a pending-but-unapplied
  state could form a second write quorum if it applied now.
- **Wire up `PostgresApplier`** — fill in `Apply()` (ALTER SYSTEM, pg_ctl promote / standby
  reconfigure, pg_reload_conf) and `AppliedState()` (read postgresql.conf / standby.signal) in
  `examples/pg_driver/`.

### Stage 2 — Bootstrapping

Safely initialize a fresh cluster. Bootstrap eligibility is an external constraint (e.g. a
Kubernetes label declaring "at most N nodes are bootstrap-eligible") — it is not persisted inside
the consensus state machine. Multiple orchs can attempt bootstrap simultaneously; they compete via
normal term election (first to win a Begin quorum proceeds, losers back off). After Establish,
new poolers join the voting cohort by being written into the postgres database, which replicates
the admission record to sync replicas. Before the next Begin term, each cohort member waits until
it has applied WAL through the admission record so it knows the full cohort and can compute quorum
correctly.

### Stage 3 — Re-bootstrapping

Safely restart a cluster after all poolers are replaced (e.g. restore from backup). The new orch
starts at `lastKnownTerm + 1` (derived from backup metadata or a quorum of restored poolers) to
avoid split-brain with any ghost processes from the old cluster.

### Stage 4 — Graceful primary replacement

Replace the primary without an availability gap — either because the current primary wants to step
down (planned maintenance) or because an operator wants to promote a specific replica. The
stepping-down pooler emits a `StepDownRequest`; orch immediately starts a new election without
waiting for a timeout. A `PromoteRequest` sets a `preferredPrimary` hint that biases
`selectPrimary()` toward the target, then follows normal Revoke → Establish.

### Stage 5 — Custom durability policies

A `DurabilityPolicy` interface takes the full set of cluster nodes (with metadata such as
zone/region) and returns the set of valid write quorums. The orch uses the active policy when
computing `revocationSets()` and `establishQuorumMet()`. Built-in policies:

- `AnyNPolicy(n)` — any N replicas must ACK (maps to `synchronous_standby_names = 'ANY N (...)'`)
- `ZoneAwarePolicy` — at least one ACK from the same zone and one from a different zone

The `atMostOneQuorum` invariant is strengthened to use the policy's notion of a valid quorum
rather than the hardcoded ANY-1 check.

### Stage 6 — Timeline / WAL support (deferred)

Handle WAL timelines safely in emergency failovers: no committed writes lost, minimal pg_rewind,
safe promotion rules when multiple orchs compete. Do not start until Stages 1–5 are solid.
