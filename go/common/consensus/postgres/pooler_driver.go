// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/consensus"
)

// ── PoolerDriver ─────────────────────────────────────────────────────────────

// PoolerDriver runs a PoolerNode state machine and implements the gRPC server
// that coordinators connect to. The pooler never dials the coordinator; all
// coordinator-initiated gRPC connections are inbound.
//
// Three inbound RPCs are served (see handler stubs at the bottom of this file):
//
//   - WritePolicy (unary): coordinator sends a Term write to the primary.
//     The handler converts it to a WritePolicyIndicator and waits for the
//     LeaderWritePolicyResponseRequest that the tick loop emits in reply.
//
//   - Recruit (unary): coordinator recruits this node into a rule-change range.
//     The handler converts it to a RecruitIndicator and waits for the
//     RecruitResponseRequest.
//
//   - PushRules (unary): coordinator pushes already-committed rules to this node
//     (primary path: rules committed via WAL; replica path: coordinator notifies
//     the replica directly rather than relying on WAL polling). The handler
//     delivers an SidecarApplyResponseIndicator to the tick loop.
//     TODO: on replicas, consider also polling consensus_durability_rules to pick
//     up rule changes even when the coordinator is temporarily unreachable.
//
//   - WatchStatus (server-streaming): coordinator subscribes to pooler status.
//     After each tick the loop fans PoolerStatusUpdateRequests onto statusUpdates.
//
// After each tick the driver also intercepts local requests from the state machine:
//
//   - SidecarApplyLeaderPolicyRequest: drives the GUC/WAL pipeline (see applyPolicyRecord).
//   - SidecarRevokeParticipationRequest: stops quorum participation (see revokeParticipation).
//
// In production a SIGTERM handler pushes TerminateIndicator onto incoming so
// the pooler records the postgres shutdown before the process exits.
type PoolerDriver struct {
	node *consensus.PoolerNode
	pg   *PostgresApplier

	tick     int64
	incoming chan consensus.Indicator

	// Channels through which the tick loop delivers responses to gRPC handlers.
	writePolicyReplies chan consensus.LeaderWritePolicyResponseRequest
	recruitResponses   chan consensus.RecruitResponseRequest
	statusUpdates      chan consensus.PoolerStatusUpdateRequest
}

// NewPoolerDriver creates a PoolerDriver that loads its last committed state from
// stateFile on startup, so crash recovery requires no extra startup logic.
func NewPoolerDriver(
	id consensus.NodeID,
	properties consensus.NodeProperties,
	stateFile *AtomicStateFile,
	pg *PostgresApplier,
) *PoolerDriver {
	node := consensus.NewPoolerNode(id, stateFile, properties)
	return &PoolerDriver{
		node:               node,
		pg:                 pg,
		incoming:           make(chan consensus.Indicator, 64),
		writePolicyReplies: make(chan consensus.LeaderWritePolicyResponseRequest, 1),
		recruitResponses:   make(chan consensus.RecruitResponseRequest, 1),
		statusUpdates:      make(chan consensus.PoolerStatusUpdateRequest, 4),
	}
}

// Run starts the pooler tick loop. Returns when ctx is cancelled. The gRPC
// server must be started separately; the handler stubs below communicate with
// this loop via the channel fields.
func (d *PoolerDriver) Run(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var pending []consensus.Indicator

	for {
		select {
		case ind := <-d.incoming:
			pending = append(pending, ind)

		case <-ticker.C:
			d.tick++
			requests := d.node.Step(d.tick, pending)
			pending = pending[:0]

			for _, req := range requests {
				switch r := req.(type) {
				case consensus.SidecarApplyLeaderPolicyRequest:
					// Intercept before RequestHandler: run the GUC/WAL pipeline.
					// Capture committed term now (in the tick loop) before handing
					// off to the goroutine — PoolerNode must not be accessed
					// concurrently.
					currentTerm := d.node.CommittedState().CachedTerm
					go d.applyPolicyRecord(ctx, r, currentTerm)

				case consensus.SidecarRevokeParticipationRequest:
					// Intercept before RequestHandler: stop quorum participation.
					role := d.node.CommittedState().Role
					go d.revokeParticipation(ctx, r, role)

				case consensus.LeaderWritePolicyResponseRequest:
					// Route to the WritePolicy gRPC handler waiting for the reply.
					select {
					case d.writePolicyReplies <- r:
					default:
					}

				case consensus.RecruitResponseRequest:
					// Route to the Recruit gRPC handler waiting for the reply.
					select {
					case d.recruitResponses <- r:
					default:
					}

				case consensus.PoolerStatusUpdateRequest:
					// Fan the status snapshot to the active WatchStatus stream.
					select {
					case d.statusUpdates <- r:
					default:
					}
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ── GUC/WAL apply pipeline ────────────────────────────────────────────────────

// applyPolicyRecord executes the GUC/WAL pipeline for a SidecarApplyLeaderPolicyRequest.
// Called in a goroutine; delivers SidecarApplyResponseIndicator via d.incoming.
//
// The coordinator always separates adds and removes into distinct writes (see
// CoordNode.advance). We enforce this invariant and use it to determine the
// correct ordering of the GUC update relative to the WAL record commit:
//
//   - Removing members: update synchronous_standby_names FIRST (stop waiting
//     for removed nodes' ACKs), then INSERT the term record. If we crash
//     between the two steps, the GUC reverts to the old term on restart
//     (storage file not yet updated) and the coordinator retries the write —
//     safe because no write was ever stalled waiting for the removed node.
//
//   - Adding members: INSERT the term record FIRST (WAL propagates to new
//     members), then update synchronous_standby_names. If we crash between the
//     two steps, the GUC is re-applied on restart from the newly committed term
//     (storage.Save happens when SidecarApplyResponseIndicator is processed by
//     PoolerNode) — safe because new members do not need to ACK the write that
//     adds them.
func (d *PoolerDriver) applyPolicyRecord(
	ctx context.Context,
	req consensus.SidecarApplyLeaderPolicyRequest,
	currentTerm *consensus.Term,
) {
	added, removed := diffCohort(currentTerm, &req.Term)

	if len(added) > 0 && len(removed) > 0 {
		// The coordinator must never issue a simultaneous add+remove. Reject
		// loudly rather than silently applying in an arbitrary order.
		d.deliverApplyResult(ctx, req.Term, false)
		return
	}

	if len(removed) > 0 {
		// Remove path: update GUC first so the primary stops waiting for ACKs
		// from nodes that are being removed.
		if err := d.pg.updateSyncStandbyNames(ctx, req.Term); err != nil {
			d.deliverApplyResult(ctx, req.Term, false)
			return
		}
	}

	// Commit the term record as a WAL entry. Replicas will learn about the
	// change when the coordinator notifies them via PushRules.
	if err := d.pg.insertRulesRecord(ctx, req.Term); err != nil {
		d.deliverApplyResult(ctx, req.Term, false)
		return
	}

	if len(added) > 0 || len(removed) == 0 {
		// Add path (or no-op membership change): update GUC after the WAL record
		// so new members are already receiving WAL before we require their ACKs.
		// A crash here is safe: the GUC is re-applied on restart because
		// storage.Load returns the freshly committed term.
		if err := d.pg.updateSyncStandbyNames(ctx, req.Term); err != nil {
			// Non-fatal: WAL record is already committed and durable. Log the error
			// and return success; the GUC will be re-applied on next restart.
			//   log.Printf("warning: GUC update after add failed: %v; will re-apply on restart", err)
			_ = err
		}
	}

	d.deliverApplyResult(ctx, req.Term, true)
}

func (d *PoolerDriver) deliverApplyResult(ctx context.Context, term consensus.Term, accepted bool) {
	select {
	case d.incoming <- consensus.SidecarApplyResponseIndicator{Term: term, Accepted: accepted}:
	case <-ctx.Done():
	}
}

// ── Revocation sidecar ────────────────────────────────────────────────────────

// revokeParticipation implements the SidecarRevokeParticipationRequest: stops this node
// from participating in write quorum under the current rules.
// Called in a goroutine; delivers SidecarRevokeResponseIndicator via d.incoming.
//
// On success the indicator carries the node's final LSN and rules seq so the
// coordinator can rank candidates by WAL progress when choosing a new primary.
//
// Replica revocation:
//  1. Disconnect the WAL receiver from the primary. Once the streaming connection
//     drops the primary stalls waiting for this node's ACK (assuming it remains
//     in synchronous_standby_names). The coordinator unblocks the primary later
//     by issuing new rules that remove this node.
//  2. Wait for WAL replay to stabilise: poll pg_last_wal_replay_lsn() until it
//     stops advancing for several consecutive polls. This ensures the replica has
//     applied as much WAL as possible before reporting its position — maximising
//     its candidacy for promotion. See waitForReplayStabilize in rpc_consensus.go.
//  3. Report the stabilised replay LSN and the rules seq from the last applied
//     rules record (which the replica now knows from the replayed WAL).
//
// Primary revocation:
//  1. Enter read-only mode so new client writes are rejected.
//  2. Abort pending synchronous commits by temporarily clearing
//     synchronous_standby_names so stalled writes complete (they fail with
//     read-only or cancellation errors) rather than hanging forever.
//  3. Report pg_current_wal_lsn() and the current rules seq.
func (d *PoolerDriver) revokeParticipation(
	ctx context.Context,
	req consensus.SidecarRevokeParticipationRequest,
	role consensus.PoolerRole,
) {
	var lsn consensus.LSN
	var rulesSeq int64
	accepted := false

	switch role {
	case consensus.RoleReplica:
		// 1. Disconnect the WAL receiver.
		//   _, err := d.pg.db.ExecContext(ctx,
		//       `SELECT pg_terminate_backend(pid)
		//        FROM pg_stat_activity
		//        WHERE backend_type = 'walreceiver'`)
		//   if err != nil { break }
		//
		// 2. Wait for replay to stabilise (stop advancing).
		//   replayLSN, err := waitForReplayStabilize(ctx, d.pg.db)
		//   if err != nil { break }
		//   lsn = consensus.LSN(replayLSN)
		//
		// 3. Read the most recently applied rules seq from the consensus table.
		//   err = d.pg.db.QueryRowContext(ctx,
		//       `SELECT seq FROM consensus_durability_rules ORDER BY seq DESC LIMIT 1`,
		//   ).Scan(&rulesSeq)
		//   if err != nil { break }
		//
		//   accepted = true

	case consensus.RolePrimary:
		// 1. Enter read-only mode.
		//   _, err := d.pg.db.ExecContext(ctx, `ALTER SYSTEM SET default_transaction_read_only = on`)
		//   if err != nil { break }
		//   _, err = d.pg.db.ExecContext(ctx, `SELECT pg_reload_conf()`)
		//   if err != nil { break }
		//
		// 2. Abort stalled synchronous commits by clearing synchronous_standby_names.
		//   _, err = d.pg.db.ExecContext(ctx, `ALTER SYSTEM SET synchronous_standby_names = ''`)
		//   if err != nil { break }
		//   _, err = d.pg.db.ExecContext(ctx, `SELECT pg_reload_conf()`)
		//   if err != nil { break }
		//
		// 3. Read the current WAL position and rules seq.
		//   var rawLSN uint64
		//   err = d.pg.db.QueryRowContext(ctx,
		//       `SELECT pg_lsn_to_uint64(pg_current_wal_lsn())`,
		//   ).Scan(&rawLSN)
		//   if err != nil { break }
		//   lsn = consensus.LSN(rawLSN)
		//   rulesSeq = int64(d.node.CommittedState().PolicySeq())
		//   accepted = true
	}

	select {
	case d.incoming <- consensus.SidecarRevokeResponseIndicator{
		CorrelationID: req.CorrelationID,
		Accepted:      accepted,
		LSN:           lsn,
		TermSeq:       rulesSeq,
	}:
	case <-ctx.Done():
	}
}

// ── gRPC handlers (inbound from coordinator) ─────────────────────────────────

// WritePolicy is the unary gRPC handler called by the coordinator to write a
// new Term to this primary. It queues a WritePolicyIndicator for the next
// tick and blocks until the tick loop emits a LeaderWritePolicyResponseRequest in reply.
//
// Production implementation sketch:
//
//	func (d *PoolerDriver) WritePolicy(ctx context.Context, req *pb.WritePolicyRequest) (*pb.WritePolicyResponse, error) {
//	    term, err := termFromProto(req.Term)
//	    if err != nil { return nil, err }
//	    select {
//	    case d.incoming <- consensus.WritePolicyIndicator{
//	        CorrelationID: req.CorrelationId,
//	        Term:          term,
//	    }:
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	    select {
//	    case r := <-d.writePolicyReplies:
//	        return &pb.WritePolicyResponse{Accepted: r.Accepted, CurrentSeq: r.CurrentSeq}, nil
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	}

// PushRules is the unary gRPC handler called by the coordinator to notify this
// node (typically a replica) that a new term has been committed to the WAL.
// Delivers an SidecarApplyResponseIndicator directly into the tick loop. The
// coordinator calls this on all known replicas after a WritePolicy succeeds.
//
// Production implementation sketch:
//
//	func (d *PoolerDriver) PushRules(ctx context.Context, req *pb.PushRulesRequest) (*pb.PushRulesResponse, error) {
//	    term, err := termFromProto(req.Term)
//	    if err != nil { return nil, err }
//	    select {
//	    case d.incoming <- consensus.SidecarApplyResponseIndicator{Term: term, Accepted: true}:
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	    return &pb.PushRulesResponse{}, nil
//	}

// Recruit is the unary gRPC handler called by the coordinator to recruit this
// node into a term-change range. Queues a RecruitIndicator for the next tick
// and blocks until the tick loop emits a RecruitResponseRequest.
//
// Production implementation sketch:
//
//	func (d *PoolerDriver) Recruit(ctx context.Context, req *pb.RecruitRequest) (*pb.RecruitResponse, error) {
//	    select {
//	    case d.incoming <- consensus.RecruitIndicator{
//	        CorrelationID: req.CorrelationId,
//	        CoordID:       consensus.NodeID(req.CoordId),
//	        AtTermSeq:     req.AtTermSeq,
//	        ProposedSeq:   req.ProposedSeq,
//	    }:
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	    select {
//	    case r := <-d.recruitResponses:
//	        return &pb.RecruitResponse{
//	            Accepted: r.Accepted,
//	            Term:     termToProto(r.Term), // may be nil
//	        }, nil
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	}

// WatchStatus is the server-streaming gRPC handler through which coordinators
// subscribe to pooler status updates. Sends a snapshot for every committed-state
// change observed by the tick loop.
//
// Production implementation sketch:
//
//	func (d *PoolerDriver) WatchStatus(_ *pb.WatchStatusRequest, stream pb.PoolerService_WatchStatusServer) error {
//	    for {
//	        select {
//	        case snap := <-d.statusUpdates:
//	            if err := stream.Send(&pb.PoolerStatusSnapshot{
//	                State:          stateToProto(snap.State),
//	                PostgresStatus: postgresStatusToProto(snap.PostgresStatus),
//	                Properties:     propertiesToProto(snap.Properties),
//	            }); err != nil {
//	                return err
//	            }
//	        case <-stream.Context().Done():
//	            return stream.Context().Err()
//	        }
//	    }
//	}

// ── PostgresApplier ───────────────────────────────────────────────────────────

// PostgresApplier executes the postgres operations that apply a rules change.
// In production it holds a *sql.DB connected to the local postgres instance.
type PostgresApplier struct {
	// db *sql.DB
}

// updateSyncStandbyNames updates synchronous_standby_names and reloads postgres
// to reflect the new cohort and policy. The value uses the postgres 'ANY n (...)'
// syntax that AtLeastPolicy maps to naturally (ANY n = AtLeastThreshold()-1).
//
// AtLeast(1) or an empty standby list produces “” (no synchronous replication).
// AtLeast(n+1) with standbys produces 'ANY n (node1,node2,...)'.
func (pg *PostgresApplier) updateSyncStandbyNames(ctx context.Context, rules consensus.Term) error {
	val, err := syncStandbyNamesValue(rules)
	if err != nil {
		return err
	}

	// ALTER SYSTEM SET synchronous_standby_names = val
	//   _, err = pg.db.ExecContext(ctx, `ALTER SYSTEM SET synchronous_standby_names = `+val)
	//   if err != nil { return err }

	// SELECT pg_reload_conf()
	//   _, err = pg.db.ExecContext(ctx, `SELECT pg_reload_conf()`)
	//   return err

	_ = val
	_ = ctx
	return nil
}

// insertRulesRecord commits a new Term as a row in the
// consensus_durability_rules table, creating a WAL entry that propagates to
// replicas. The INSERT uses ON CONFLICT DO NOTHING so it is idempotent —
// retrying after a crash before the response was delivered is safe.
func (pg *PostgresApplier) insertRulesRecord(ctx context.Context, rules consensus.Term) error {
	// data, err := json.Marshal(termJSON from rules)
	// if err != nil { return err }
	//
	// _, err = pg.db.ExecContext(ctx,
	//     `INSERT INTO consensus_durability_rules (seq, data)
	//      VALUES ($1, $2)
	//      ON CONFLICT (seq) DO NOTHING`,
	//     rules.Seq, data,
	// )
	// return err

	_ = rules
	_ = ctx
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// syncStandbyNamesValue returns the postgres synchronous_standby_names value for
// the given term. Assumes AtLeastPolicy; returns an error for unsupported types.
// The postgres ANY N value equals AtLeastThreshold()-1 because postgres counts
// replica ACKs while AtLeastPolicy counts total node ACKs (primary + replicas).
func syncStandbyNamesValue(rules consensus.Term) (string, error) {
	at, ok := rules.Policy.(atLeastThresholder)
	if !ok {
		return "", fmt.Errorf("unsupported DurabilityPolicy type %T", rules.Policy)
	}
	// AtLeastThreshold includes the primary; postgres ANY N counts only replicas.
	n := at.AtLeastThreshold() - 1

	var standbys []string
	for _, m := range rules.Members {
		if m.ID != rules.Primary {
			standbys = append(standbys, string(m.ID))
		}
	}

	if n == 0 || len(standbys) == 0 {
		return "''", nil
	}
	return fmt.Sprintf("'ANY %d (%s)'", n, strings.Join(standbys, ",")), nil
}

// diffCohort returns the node IDs added and removed when moving from before to
// after. Both slices are nil if membership is unchanged.
// Iterates over the input slices (not maps) to preserve deterministic order.
func diffCohort(before, after *consensus.Term) (added, removed []consensus.NodeID) {
	beforeSet := make(map[consensus.NodeID]bool)
	if before != nil {
		for _, m := range before.Members {
			beforeSet[m.ID] = true
		}
	}
	afterSet := make(map[consensus.NodeID]bool)
	if after != nil {
		for _, m := range after.Members {
			afterSet[m.ID] = true
		}
	}
	if after != nil {
		for _, m := range after.Members {
			if !beforeSet[m.ID] {
				added = append(added, m.ID)
			}
		}
	}
	if before != nil {
		for _, m := range before.Members {
			if !afterSet[m.ID] {
				removed = append(removed, m.ID)
			}
		}
	}
	return added, removed
}
