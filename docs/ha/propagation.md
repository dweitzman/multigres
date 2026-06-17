# Propagating Stuck Rule Transitions

When a rule change is interrupted partway through, the cohort can be left holding
a rule-change transaction that is neither confirmed durable nor safe to discard.
This doc describes how such a **stuck transition** is finalized by _propagating_
the in-WAL entry to quorum instead of writing a new one.

> **Status: draft / provisional.** This is not yet implemented

## Purpose and boundary

This doc owns _how a stuck rule transition is safely completed_. It does **not**
own:

- _When_ a stuck transition is detected and recovery is triggered — that's
  [recovery.md](recovery.md) (the coordinator's failover path notices it).
- The _normal_ (uninterrupted) rule change — that's
  [rule-change.md](rule-change.md). Propagation is what happens when that
  protocol was interrupted between writing the rule and marking it durable.

It is the concrete realization of the overview's one-directional preservation:
_"a hung transaction that didn't meet the redundancy requirement may be
propagated and made durable going forward"_
([consensus-overview.md](consensus-overview.md)).

## The problem

A normal rule change writes the rule-change transaction to WAL and then blocks
until a sync-standby quorum acknowledges it — the
[durability checkpoint](rule-change.md#committing-the-change-the-durability-checkpoint).
If the leader (or coordinator) dies _after_ the WAL entry exists on one or more
nodes but _before_ it is acknowledged to quorum and marked durable, the entry is
**in-flight**: possibly durable, possibly not. By one-directional preservation we
cannot simply discard it (it might have been durable), but we also cannot yet
treat it as decided.

A plain failover can't resolve this: the recruit step rejects any node carrying
an in-flight rule beyond the coordinator's known decision ("coordinator view is
stale"). Propagation is the path that _acknowledges_ the in-flight entry and
drives it to completion.

## State model change: decision vs. proposal

Propagation requires distinguishing a _decided_ rule from an _in-flight_ one, so
`PoolerPosition` (and the `current_rule` table) split each rule slot in two:

- **`decision`** — the last marked durable rule (written to WAL and acknowledged
  by quorum). Always present.
- **`proposal`** — an in-flight rule transition. NULL in the common case;
  populated between the proposal write (the sync-replication wait) and the
  decision marking, and cleared atomically when the decision is marked.

`updateRule` writes the `proposal_*` columns first (CAS against the current
`decision_*`), then a second step promotes `proposal_* → decision_*`. Position
comparison becomes: highest **decision** first, then **proposal** (a node with an
in-flight proposal, or a higher one, is further ahead), then LSN
(`ComparePosition`).

> TODO: [state-model.md](state-model.md) currently documents a single-rule model
> (`current_rule` with one rule). Once this lands, update state-model.md's
> "Where the rule lives" and "Positions and PostgreSQL state" sections to the
> decision/proposal split, and link here.

## Detecting a stuck transition

`NewTermRevocation`
([revocation.go](../../go/common/consensus/revocation.go)) now tracks both the
highest **decision** and the highest **proposal** across the cohort:

- `outgoing_decision` = highest decision rule number (always a decision, never a
  proposal).
- If the highest position is an in-flight proposal (`max proposal >
outgoing_decision`), it sets **`propagation_intent`** to that proposal's rule
  number.

The coordinator inspects `propagation_intent`: if set, it takes the propagation
path (`runPropagation`); otherwise the normal safe-proposal path.

`propagation_intent` also relaxes one check. Normally `ValidateRevocation`
rejects a recruit whose in-flight proposal is beyond `outgoing_decision`
("coordinator view is stale"). When `propagation_intent` exactly matches that
proposal, the coordinator has explicitly acknowledged the entry and promised to
propagate it — so the recruit is accepted.

`propagation_intent` is the in-WAL **proposal** rule number, which is strictly
_greater_ than `outgoing_decision` (the proposal sits one transition ahead of the
last decision).

## The propagation flow (coordinator)

`Coordinator.runPropagation`
([coordinator.go](../../go/services/multiorch/consensus/coordinator.go)):

1. **Back off** if anyone recently accepted a revocation (`checkRecentAcceptance`).
2. **Pre-vote** feasibility: `CheckPropagationPossible`
   ([propagation.go](../../go/common/consensus/propagation.go)) — enough of the
   outgoing cohort could accept (so the in-WAL rule number is unique and can't be
   overwritten by a parallel quorum), and a most-advanced node carries the
   matching proposal.
3. **Recruit** all nodes with the propagation revocation. Followers accept via
   the `propagation_intent` exception above.
4. **Pick the propagation leader**: `FindPropagationLeaders` returns the
   most-advanced recruited nodes whose proposal matches `propagation_intent`;
   the coordinator picks the first cohort-eligible one.
5. **Dispatch**:
   - `Propagate` to the propagation leader (finalize the in-WAL entry).
   - `SetTermPrimary` to every other recruited node, pointing it at the
     propagation leader as its replication source.
6. The leader's `Propagate` success means the transition is finalized.

## Propagate (pooler side)

`Propagate`
([rpc_consensus.go](../../go/services/multipooler/manager/rpc_consensus.go)) on
the propagation leader, under the action lock:

1. Validate inputs; `propagation_intent` must match `expected_proposal`'s rule
   number; `ValidateRevocation`; require a prior `Recruit` for this term
   (standby, no `primary_conninfo`).
2. **Finalize the in-WAL proposal** (`propagateProposal`): apply the `Both` GUC →
   promotion hook (postgres becomes primary) → emit `pg_logical_emit_message` as
   the quorum gate → mark `proposal_* → decision_*` → apply the `Incoming` GUC.
   No new rule content is written — the existing WAL entry is driven to quorum
   and marked decided.
3. **Self-promote**: write a fresh rule at `(revoked_below_term, 0)` naming this
   pooler as leader, carrying the propagated proposal's cohort and durability
   policy forward.
4. Update topology to PRIMARY/SERVING and record `(rule, primary)`.

So propagation produces _two_ rule writes on the leader: finalizing the stuck
entry as a decision, then a normal self-promotion at the new term.

## SetTermPrimary changes (followers)

`SetTermPrimary` ([consensusdata.proto](../../proto/consensusdata.proto)) gains a
required **`primary_revocation`** field, and `ReplicationPrimary` records it.
Two behavior changes support propagation:

- **Staleness gate**: a `SetTermPrimary` whose `primary_revocation` is below the
  one already recorded for the known primary is rejected — guarding against
  stale deliveries from older recovery rounds.
- **`leader.id` need not match `rule.leader_id`**: in propagation the rule's
  authored leader is the now-dead primary, while the contact info points at the
  propagation leader that finalized it. `leader` is treated as
  eventually-consistent discovery info, not authority.

## Why it's safe

> TODO: write out the preservation argument explicitly. Sketch: the outgoing
> cohort quorum (`checkPropagationUniqueTermRevocation`) guarantees the in-WAL
> rule number is unique, so no parallel quorum can overwrite it; finalizing
> rather than discarding the in-flight entry honors one-directional preservation
> (never drop a possibly-durable transaction); and the self-promotion at the new
> term re-establishes a single leader. Tie back to the overview's chain model.

## Implementation map (branch `dw-transition-clarity-v4`)

| Concern                          | Code                                                                                                                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| decision/proposal split          | `PoolerPosition` in [clustermetadata.proto](../../proto/clustermetadata.proto); `current_rule` columns + `updateRule` in `go/services/multipooler/manager/rule_store.go` |
| detection + `propagation_intent` | [revocation.go](../../go/common/consensus/revocation.go) (`NewTermRevocation`, `ValidateRevocation`)                                                                     |
| feasibility + leader selection   | [propagation.go](../../go/common/consensus/propagation.go) (`CheckPropagationPossible`, `FindPropagationLeaders`)                                                        |
| coordinator driver               | [coordinator.go](../../go/services/multiorch/consensus/coordinator.go) (`runPropagation`)                                                                                |
| pooler finalize                  | `Propagate` + `propagateProposal` / `newPropagationUpdate` in `rpc_consensus.go` / `rule_store.go`                                                                       |
| follower re-point                | `SetTermPrimary` + `primary_revocation` ([consensusdata.proto](../../proto/consensusdata.proto))                                                                         |
| e2e                              | `go/test/endtoend/multiorch/propagation_test.go` (`TestPropagationRecovery`)                                                                                             |

## See also

- [Consensus overview](consensus-overview.md) — one-directional preservation;
  the "hung transaction… propagated and made durable" sentence is this.
- [Changing the rules safely](rule-change.md) — the normal path propagation
  completes when interrupted.
- [The HA state model](state-model.md) — decision/proposal split lands here once merged.
