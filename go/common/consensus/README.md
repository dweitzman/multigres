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
   Revocation is complete when the `DurabilityPolicy` quorum reports `IsRevoked=true`: either the
   old primary applied the revoke, or so many of its sync replicas applied it that the required
   number of write acks is no longer achievable.

3. **Establish** — the orch calls `DurabilityPolicy.ProposeQuorum` to select a new primary and
   sync-replica set. Establishment is complete when the quorum reports `IsEstablished=true`:
   the new primary and enough sync replicas have all reported `Applied=true`.

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
| `DurabilityPolicy`      | Selects a write quorum given full cluster context (health, LSN, zone). Returns a `Quorum` the orch treats as opaque.                        |
| `Quorum`                | Self-contained quorum snapshot: knows its primary, sync replicas, and how to evaluate `IsEstablished`, `IsRevoked`, `IsWriteQuorum`.        |
| `QuorumCandidate`       | Per-node input to `DurabilityPolicy.ProposeQuorum`: ID, health, LSN, and zone.                                                              |
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
  quorum. Uses `DurabilityPolicy.ReconstructQuorum` + `Quorum.IsWriteQuorum` to evaluate the
  historical committed sync-replica set, not what the policy would propose today. It is valid for
  two nodes to have `committed.Role=Primary` simultaneously (a stale primary that hasn't received
  Revoke yet) as long as only one has replicas satisfying `IsWriteQuorum`.

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

- ✅ **Configurable quorum** — `DurabilityPolicy` interface replaces the hardcoded `syncReplicaQuorum=1`.
  `AnyNPolicy(n)` implements "any N of the sync replicas must ACK". Revocation and establishment
  are delegated to `Quorum.IsRevoked` / `Quorum.IsEstablished`; safety invariants use
  `Quorum.IsWriteQuorum` via `DurabilityPolicy.ReconstructQuorum`.
- ✅ **Late-response filtering** — `PoolerResponseRequest` and `PoolerResponseIndicator` now carry
  `VotingTerm` + `SeqNum`; orch discards responses that don't match the current proposal.
- **Crash orch nodes in simulation** — the crash driver currently only targets poolers. Extend it
  to also crash orch nodes; extend `DiscoveryNode` to re-deliver discovery events to a restarted
  orch (since orch state is ephemeral).
- **Strengthen `atMostOneQuorum`** — also check whether any pooler with a pending-but-unapplied
  state could form a second write quorum if it applied now.
- **Wire up `PostgresApplier`** — fill in `Apply()` (ALTER SYSTEM, pg_ctl promote / standby
  reconfigure, pg_reload_conf) and `AppliedState()` (read postgresql.conf / standby.signal) in
  `examples/pg_driver/`.
- **Realistic gRPC wiring in `pg_driver.go`** — update the production sketch to reflect the actual
  gRPC topology: the orch holds a streaming health gRPC to each pooler (pooler status flows back as
  streaming responses, not outbound pooler RPCs); the pooler's `PoolerResponseRequest` is a reply on
  the incoming `ProposeState` RPC stream rather than a separate outbound call; and the pooler never
  initiates connections to the orch.

### Stage 2 — Bootstrapping

Safely initialize a fresh cluster. Bootstrap eligibility is an external constraint (e.g. a
Kubernetes label declaring "at most N nodes are bootstrap-eligible") — it is not persisted inside
the consensus state machine. Multiple orchs can attempt bootstrap simultaneously; they compete via
normal term election (first to win a Begin quorum proceeds, losers back off). After Establish,
new poolers join the voting cohort by being written into the postgres database, which replicates
the admission record to sync replicas. Before the next Begin term, each cohort member waits until
it has applied WAL through the admission record so it knows the full cohort and can compute quorum
correctly.

Key design item for bootstrap safety: add a `Bootstrap bool` field to `ConsensusState`. The orch
sets it when using the bootstrap policy (no confirmed quorum). A pooler that has already applied an
Establish proposal (committed `Role = Primary/Replica` with `Applied = true`) must reject any
proposal with `Bootstrap = true`. This prevents a new or partitioned orch that hasn't yet discovered
an existing cohort from accidentally bootstrapping over a healthy cluster. The term number alone is
not sufficient protection: bootstrap terms could keep incrementing (due to racing orchs or pod
churn) and a high bootstrap term number must never override an already-established cohort.
Bootstrap-eligible poolers are those with no applied cohort state; once a pooler has applied an
Establish it is no longer bootstrap-eligible for the lifetime of that cohort.

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

### Stage 5 — Additional durability policies and quorum spec serialization

The `DurabilityPolicy` / `Quorum` interface is already wired into the orch (Stage 1). Stage 5
adds:

- **Quorum spec serialization** — populate `ConsensusState.QuorumSpec` / `PoolerPersistentState.QuorumSpec`
  with serialized quorum bytes so a restarted orch can reconstruct the exact historical quorum
  without re-running `ProposeQuorum`. Requires a type registry (similar to Vitess's
  `RegisterDurability`) so the deserializer knows which concrete type to decode.
- **`ZoneAwarePolicy`** — at least one ACK from the same zone and one from a different zone
  (conjunctive requirement that `AnyNPolicy` cannot express)
- Additional policies as operational needs evolve

### Stage 6 — Timeline / WAL support (deferred)

Handle WAL timelines safely in emergency failovers: no committed writes lost, minimal pg_rewind,
safe promotion rules when multiple orchs compete. Do not start until Stages 1–5 are solid.
