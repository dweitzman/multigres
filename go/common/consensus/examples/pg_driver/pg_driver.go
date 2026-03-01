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

// Package pg_driver sketches the production wiring that sits around the pure
// consensus state machines. It shows:
//
//   - AtomicStateFile: crash-safe PoolerStorage (write-rename + fsync).
//   - PostgresApplier: executes role changes (pg_ctl, postgresql.conf, pg_reload_conf).
//   - OrchDriver: main loop for OrchNode. The orch owns all outbound gRPC connections
//     to poolers. Two call types per pooler:
//     (1) ProposeState (unary) — broadcasts a consensus state change; the pooler's
//     accept/reject reply feeds back as a PoolerResponseIndicator.
//     (2) HealthCheck (server-streaming) — a long-lived stream; the pooler pushes
//     status snapshots (committed state, applied flag, postgres health) as they
//     change, feeding PoolerStatusIndicators. Applied-state transitions arrive here.
//   - PoolerDriver: gRPC server for PoolerNode. The pooler never dials the orch.
//     It serves ProposeState (unary) and HealthCheck (server-streaming) RPCs, mapping
//     them to/from the Indicator/Request types the state machine understands.
//     After each tick the driver checks whether the committed role change needs to be
//     applied. If so, it starts (or checks) an apply goroutine that runs the postgres
//     operations asynchronously and writes ApplySucceededIndicator onto the incoming
//     channel when done. PoolerNode is never accessed from outside the tick loop —
//     all communication goes through the indicators channel.
//
// gRPC call sites are annotated with comments but not wired to real proto stubs.
// Both drivers compile against the consensus interfaces so any API change surfaces
// here immediately.
package pg_driver

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/multigres/multigres/go/common/consensus"
)

// Compile-time check: production implementation must satisfy the storage interface.
// Any API change breaks the build here, prompting a review of the production path.
var _ consensus.PoolerStorage = (*AtomicStateFile)(nil)

// ── AtomicStateFile (consensus.PoolerStorage) ────────────────────────────────

// AtomicStateFile persists PoolerPersistentState to disk using write-rename+fsync
// so that a crash mid-write never leaves a partial or corrupt file behind.
// This upholds the consensus invariant that committed votes must survive restarts.
type AtomicStateFile struct {
	path string // e.g. /var/lib/pooler/consensus.json
}

// NewAtomicStateFile creates an AtomicStateFile that writes to path.
func NewAtomicStateFile(path string) *AtomicStateFile {
	return &AtomicStateFile{path: path}
}

// Save atomically persists state to disk:
//  1. Marshal to JSON.
//  2. Write to a temp file in the same directory (same filesystem as path).
//  3. fsync the temp file — data is durable before the rename.
//  4. Rename temp → path (atomic on POSIX).
//  5. fsync the parent directory — directory entry is durable.
func (f *AtomicStateFile) Save(state consensus.PoolerPersistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dirOf(f.path), ".consensus-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return err
	}
	dir, err := os.Open(dirOf(f.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Load reads the state file written by Save and deserialises it.
// Called by NewPoolerNode on startup and by Restart() after a crash.
// Returns an error (and zero state) if the file does not exist yet (first run).
func (f *AtomicStateFile) Load() (consensus.PoolerPersistentState, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return consensus.PoolerPersistentState{}, err
	}
	var state consensus.PoolerPersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return consensus.PoolerPersistentState{}, err
	}
	return state, nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == os.PathSeparator {
			return path[:i]
		}
	}
	return "."
}

// ── PostgresApplier ───────────────────────────────────────────────────────────

// PostgresApplier executes replication role changes on the local PostgreSQL
// instance. Used by PoolerDriver after each tick to drive the apply loop.
type PostgresApplier struct {
	// pg PgController — interface to pg_ctl, ALTER SYSTEM, pg_reload_conf, etc.
}

// apply executes the role change described by state. Returns true on success,
// false for a transient failure (caller retries on the next tick).
func (a *PostgresApplier) apply(state consensus.PoolerPersistentState) bool {
	switch state.Role {
	case consensus.RolePrimary:
		// 1. Deserialise state.QuorumSpec via the DurabilityPolicy to get the Quorum,
		//    then call quorum.PostgresConfig() to get the synchronous_standby_names value.
		//    ALTER SYSTEM SET synchronous_standby_names = quorum.PostgresConfig().
		// 2. pg_ctl promote (or SELECT pg_promote()) if currently in standby mode.
		// 3. SELECT pg_reload_conf() so the new synchronous_standby_names takes effect.
		// 4. Poll pg_is_in_recovery() until false; return false if it times out (retry next tick).
		return true

	case consensus.RoleReplica:
		// 1. ALTER SYSTEM SET primary_conninfo = 'host=<state.Primary> ...'.
		// 2. Write standby.signal (PG≥12) so postgres enters standby on next start.
		// 3. SELECT pg_reload_conf() to reconnect streaming replication without a full restart.
		// 4. Poll pg_stat_wal_receiver until state='streaming'; return false if it times out.
		return true

	default:
		// RoleUnknown: no proposal received yet; nothing to apply.
		return true
	}
}

// ── OrchDriver ───────────────────────────────────────────────────────────────

// OrchDriver runs an OrchNode state machine at a fixed tick rate.
// The orch owns all outbound gRPC connections to poolers; a pooler never dials
// the orch. Two call types are made per discovered pooler:
//
//   - ProposeState (unary): called for each BroadcastStateRequest. A goroutine
//     per pooler calls ProposeState and pushes the reply back onto indicators as
//     a PoolerResponseIndicator.
//
//   - Health updates: the orch periodically fetches (or subscribes to) pooler
//     status — committed state, applied flag, postgres health. Each update is
//     pushed onto indicators as a PoolerStatusIndicator. This is the only source
//     of applied-state updates; the pooler never initiates an outbound call.
//
// Both flows push onto the same indicators channel, which is drained at each tick.
// Callers notify the driver of pooler membership changes via OnPoolerDiscovered /
// OnPoolerRemoved (e.g. driven by an etcd watch or service-discovery callback).
type OrchDriver struct {
	node       *consensus.OrchNode
	tick       int64
	indicators chan consensus.Indicator
	// poolerClients map[consensus.NodeID]pb.PoolerServiceClient
}

// NewOrchDriver creates an OrchDriver for the given node.
func NewOrchDriver(node *consensus.OrchNode) *OrchDriver {
	return &OrchDriver{
		node:       node,
		indicators: make(chan consensus.Indicator, 64),
	}
}

// Indicators returns the channel onto which all inbound events should be pushed.
// The etcd watch goroutine writes PoolerDiscoveredIndicator/PoolerRemovedIndicator
// here. Health stream goroutines (started by OnPoolerDiscovered) write
// PoolerStatusIndicator here.
func (d *OrchDriver) Indicators() chan<- consensus.Indicator {
	return d.indicators
}

// Run starts the orch tick loop. It returns when ctx is cancelled.
func (d *OrchDriver) Run(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var pending []consensus.Indicator

	for {
		select {
		case ind := <-d.indicators:
			pending = append(pending, ind)

		case <-ticker.C:
			d.tick++
			requests := d.node.Step(d.tick, pending)
			pending = pending[:0]

			for _, req := range requests {
				switch r := req.(type) {
				case consensus.BroadcastStateRequest:
					// For each target pooler, call ProposeState (unary) in a goroutine.
					// The goroutine converts the response to a PoolerResponseIndicator
					// and pushes it onto d.indicators for the next tick.
					//
					//   for _, poolerID := range resolveTargets(r.Targets) {
					//       go func(id consensus.NodeID) {
					//           resp, err := d.poolerClients[id].ProposeState(ctx, &pb.ProposeStateRequest{
					//               State:               protoFromConsensusState(r.State),
					//               ExpectedPrimaryTerm: r.ExpectedPrimaryTerm,
					//           })
					//           if err != nil {
					//               return // pooler unreachable; orch will retry on appointment phase timeout
					//           }
					//           d.indicators <- consensus.PoolerResponseIndicator{
					//               FromPooler:   id,
					//               CoordTerm:    resp.CoordTerm,
					//               SeqNum:       resp.SeqNum,
					//               Accepted:     resp.Accepted,
					//               KnownTerm:    resp.KnownTerm,
					//               KnownCoordID: consensus.NodeID(resp.KnownCoordId),
					//           }
					//       }(poolerID)
					//   }
					_ = r
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// OnPoolerDiscovered should be called (e.g. from a service-discovery callback)
// when a new pooler becomes known. It pushes a PoolerDiscoveredIndicator so
// OrchNode learns about the pooler, then begins fetching health updates.
// Run this in a goroutine; it exits when ctx is cancelled or the pooler is removed.
func (d *OrchDriver) OnPoolerDiscovered(ctx context.Context, poolerID consensus.NodeID) {
	// Notify OrchNode that this pooler exists.
	//   d.indicators <- consensus.PoolerDiscoveredIndicator{PoolerID: poolerID}

	// Begin health updates. Each snapshot becomes a PoolerStatusIndicator.
	// Whether this is implemented as polling or a streaming RPC is an internal
	// detail; the indicator shape is the same either way.
	//
	//   for ctx.Err() == nil {
	//       snap, err := d.poolerClients[poolerID].GetStatus(ctx, &pb.StatusRequest{})
	//       if err != nil { <backoff>; continue }
	//       d.indicators <- consensus.PoolerStatusIndicator{
	//           PoolerID:       poolerID,
	//           StatusSeq:      snap.StatusSeq,
	//           State:          consensusStateFromProto(snap.CommittedState),
	//           Applied:        snap.Applied,
	//           PostgresStatus: postgresStatusFromProto(snap.PostgresStatus),
	//       }
	//       <wait for next tick or stream event>
	//   }
}

// OnPoolerRemoved should be called when a pooler is no longer reachable or has
// been deregistered. It cancels the health-update goroutine (via ctx) and notifies
// OrchNode.
func (d *OrchDriver) OnPoolerRemoved(poolerID consensus.NodeID) {
	//   d.indicators <- consensus.PoolerRemovedIndicator{PoolerID: poolerID}
}

// ── PoolerDriver ─────────────────────────────────────────────────────────────

// PoolerDriver runs a PoolerNode state machine and implements the gRPC server
// that orchs connect to. The pooler never dials the orch; all gRPC connections
// are inbound.
//
// Two RPCs are served (see handler stubs at the bottom of this file):
//
//	ProposeState (unary) — the orch sends a consensus state change proposal:
//	  1. The handler converts it to an OrchStateIndicator and queues it on incoming.
//	  2. The tick loop processes it and emits a PoolerResponseRequest.
//	  3. The handler reads from proposeReplies and returns the accept/reject response.
//
//	HealthCheck (server-streaming) — the orch subscribes to pooler status:
//	  1. After each tick the loop fans PoolerStatusUpdateRequests onto statusUpdates.
//	  2. The handler streams each snapshot to the orch as a proto message.
//	  Applied-state changes reach the orch here, not via a pooler-initiated RPC.
//
// After each tick the driver also checks whether the committed role change needs
// to be applied. If so, it starts a goroutine that runs the postgres operations
// (pg_ctl promote, ALTER SYSTEM SET, pg_reload_conf) asynchronously and writes
// ApplySucceededIndicator onto incoming when done. PoolerNode is never accessed
// from outside the tick loop — all communication goes through the indicators channel.
//
// In production a SIGTERM handler pushes a TerminateIndicator onto incoming so
// the pooler can record the postgres shutdown before the process exits.
type PoolerDriver struct {
	node           *consensus.PoolerNode
	pg             *PostgresApplier
	tick           int64
	incoming       chan consensus.Indicator                 // fed by gRPC handlers and apply loop; drained each tick
	proposeReplies chan consensus.PoolerResponseRequest     // PoolerResponseRequests for ProposeState
	statusUpdates  chan consensus.PoolerStatusUpdateRequest // snapshots for HealthCheck streams
	// applyingFor is the StateID of the proposal the current apply goroutine is
	// working on. Prevents spawning a second goroutine for the same proposal
	// while the first is still in flight. Only read/written in the tick loop.
	applyingFor consensus.StateID
}

// NewPoolerDriver creates a PoolerDriver. The PoolerNode loads its last committed
// state from stateFile automatically, so crash recovery requires no extra startup.
func NewPoolerDriver(
	id consensus.NodeID,
	stateFile *AtomicStateFile,
	pg *PostgresApplier,
) *PoolerDriver {
	node := consensus.NewPoolerNode(id, stateFile)
	return &PoolerDriver{
		node:           node,
		pg:             pg,
		incoming:       make(chan consensus.Indicator, 64),
		proposeReplies: make(chan consensus.PoolerResponseRequest, 1),
		statusUpdates:  make(chan consensus.PoolerStatusUpdateRequest, 4),
	}
}

// Run starts the pooler tick loop. It returns when ctx is cancelled.
// The gRPC server must be started separately; the two gRPC handlers below
// communicate with this loop via the incoming / proposeReplies / statusUpdates channels.
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
				case consensus.PoolerResponseRequest:
					// Route the accept/reject to the ProposeState handler waiting for it.
					select {
					case d.proposeReplies <- r:
					default:
					}
				case consensus.PoolerStatusUpdateRequest:
					// Fan the status snapshot to the active HealthCheck stream handler.
					select {
					case d.statusUpdates <- r:
					default:
					}
				}
			}

			// After the tick, check whether the committed role change needs applying.
			// PoolerNode is never accessed from the goroutine — only from this tick
			// loop — so there is no concurrent read.
			//
			// Synchronization guarantees:
			//
			// (1) No two simultaneous applies for the same proposal.
			//     applyingFor tracks the StateID of the in-flight goroutine.
			//     We only spawn a new goroutine when committed.VotedStateID()
			//     differs, so a second goroutine for the same proposal is
			//     never started while the first is still running.
			//
			// (2) An out-of-date apply can never overwrite a newer committed state.
			//     When the committed goal advances (new proposal), we start a fresh
			//     goroutine immediately without waiting for the previous one to
			//     finish. The previous goroutine may still be running postgres
			//     operations, but its ApplySucceededIndicator carries the old
			//     VotedTerm/VotedSeqNum. PoolerNode validates those values against
			//     the current committed state before accepting the indicator, so a
			//     stale result is silently discarded and applied state never reverts.
			//
			//     (In production you may also cancel the old goroutine's context
			//     to avoid redundant postgres work, but correctness does not
			//     require it — the state-machine guard is sufficient.)
			committed := d.node.CommittedState()
			if !d.node.IsApplied() && d.node.PostgresStatus() == consensus.PostgresRunning &&
				committed.CommittedStateID() != d.applyingFor {
				d.applyingFor = committed.CommittedStateID()
				go func(state consensus.PoolerPersistentState) {
					if d.pg.apply(state) {
						select {
						case d.incoming <- consensus.ApplySucceededIndicator{
							CoordTerm: state.Committed.CoordTerm,
							SeqNum:    state.Committed.SeqNum,
						}:
						case <-ctx.Done():
						}
					}
					// On failure, the driver will try again on the next tick that
					// finds IsApplied()=false with the same proposal still committed.
				}(committed)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ProposeState is the unary gRPC handler called by the orch to propose a consensus
// state change. It queues the request as an OrchStateIndicator for the next tick
// and waits for the PoolerResponseRequest the tick loop emits in reply.
//
// Sketch of the production implementation (not wired to real proto stubs):
//
//	func (d *PoolerDriver) ProposeState(ctx context.Context, req *pb.ProposeStateRequest) (*pb.ProposeStateResponse, error) {
//	    d.incoming <- consensus.OrchStateIndicator{
//	        CoordID:             consensus.NodeID(req.State.CoordId),
//	        PoolerID:            d.node.ID(),
//	        State:               consensusStateFromProto(req.State),
//	        ExpectedPrimaryTerm: consensus.Term(req.ExpectedPrimaryTerm),
//	    }
//	    select {
//	    case r := <-d.proposeReplies:
//	        return &pb.ProposeStateResponse{
//	            Accepted:     r.Accepted,
//	            VotingTerm:   int64(r.VotingTerm),
//	            SeqNum:       r.SeqNum,
//	            KnownTerm:    int64(r.KnownTerm),
//	            KnownCoordId: string(r.KnownCoordID),
//	        }, nil
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	}

// GetStatus is the gRPC handler called by the orch to poll (or subscribe to)
// pooler status. It reads a PoolerStatusUpdateRequest snapshot produced by the
// most recent tick and returns it as a proto response. For a streaming variant
// this handler would loop, sending each new snapshot as it arrives.
// This is the only channel through which the orch learns about applied-state changes.
//
// Sketch of the production implementation (not wired to real proto stubs):
//
//	func (d *PoolerDriver) GetStatus(ctx context.Context, req *pb.StatusRequest) (*pb.PoolerStatusSnapshot, error) {
//	    select {
//	    case snap := <-d.statusUpdates:
//	        return &pb.PoolerStatusSnapshot{
//	            CommittedState:  protoFromPersistentState(snap.State),
//	            Applied:         snap.Applied,
//	            PostgresStatus:  protoFromPostgresStatus(snap.PostgresStatus),
//	        }, nil
//	    case <-ctx.Done():
//	        return nil, ctx.Err()
//	    }
//	}
