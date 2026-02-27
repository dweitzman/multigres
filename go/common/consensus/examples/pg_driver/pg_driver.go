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
//   - PostgresApplier: operational RoleApplier (pg_ctl, postgresql.conf, pg_reload_conf).
//   - OrchDriver: main loop for OrchNode — ticks every 100 ms, translates
//     BroadcastStateRequests into gRPC calls, feeds gRPC responses back as Indicators.
//   - PoolerDriver: main loop for PoolerNode — ticks every 100 ms, translates
//     PoolerResponseRequests and PoolerStatusUpdateRequests into gRPC calls to orchs.
//     Startup automatically restores committed state from AtomicStateFile.
//
// The orch emits only BroadcastStateRequest; the pooler emits PoolerResponseRequest
// and PoolerStatusUpdateRequest. TerminateRequest is simulation-only: in production
// the equivalent is a SIGTERM delivered outside the consensus request system.
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

// Compile-time checks: production implementations must satisfy the interfaces
// PoolerNode depends on. Any API change breaks the build here, prompting a review
// of whether the production path still makes sense.
var (
	_ consensus.PoolerStorage = (*AtomicStateFile)(nil)
	_ consensus.RoleApplier   = (*PostgresApplier)(nil)
)

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

// ── PostgresApplier (consensus.RoleApplier) ──────────────────────────────────

// PostgresApplier executes replication role changes on the local PostgreSQL
// instance. Injected into NewPoolerNode by the multipooler service alongside a
// live PgController (pgctld client or pg_ctl wrapper).
type PostgresApplier struct {
	// pg PgController — interface to pg_ctl, ALTER SYSTEM, pg_reload_conf, etc.
}

// Apply executes the role change described by state. Returns true on success,
// false for a transient failure (PoolerNode retries on the next tick).
func (a *PostgresApplier) Apply(state consensus.PoolerPersistentState) bool {
	switch state.Role {
	case consensus.RolePrimary:
		// 1. ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (...)' using state.SyncReplicas.
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

// AppliedState returns the replication configuration currently in effect on disk
// by reading the postgres GUC settings. This is recoverable after a crash because
// postgresql.conf / standby.signal survive process restarts.
//
// In production, derive the PoolerPersistentState by reading:
//   - Whether standby.signal exists → Role = RoleReplica; absent → Role = RolePrimary.
//   - primary_conninfo (from postgresql.auto.conf) → Primary field.
//   - synchronous_standby_names (from postgresql.auto.conf) → SyncReplicas field.
//
// Returns false if no role change has been applied yet (clean first-start, config
// files contain only defaults).
func (a *PostgresApplier) AppliedState() (consensus.PoolerPersistentState, bool) {
	// TODO: Read postgresql.conf / standby.signal to recover applied state.
	// For now returns false (no applied state known), which causes PoolerNode to
	// fall back to the committed state on restart.
	return consensus.PoolerPersistentState{}, false
}

// ── OrchDriver ───────────────────────────────────────────────────────────────

// OrchDriver runs an OrchNode state machine at a fixed tick rate. On each tick it
// calls node.Step(), then translates the returned BroadcastStateRequests into gRPC
// calls to poolers. Incoming gRPC responses (PoolerResponseIndicator,
// PoolerStatusIndicator) and etcd-watch events (PoolerDiscoveredIndicator,
// PoolerRemovedIndicator) are pushed onto the indicators channel by their respective
// handlers and buffered here for the next tick.
type OrchDriver struct {
	node *consensus.OrchNode
	tick int64
	// poolerClients map[consensus.NodeID]pb.PoolerServiceClient
}

// Run starts the orch tick loop. It returns when ctx is cancelled.
func (d *OrchDriver) Run(ctx context.Context, indicators <-chan consensus.Indicator) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var pending []consensus.Indicator

	for {
		select {
		case ind := <-indicators:
			// Buffer incoming indicators for the next tick. Sources:
			//   - gRPC PoolerResponse handler → PoolerResponseIndicator
			//   - gRPC PoolerStatus handler   → PoolerStatusIndicator
			//   - etcd watch on pooler keys   → PoolerDiscoveredIndicator / PoolerRemovedIndicator
			pending = append(pending, ind)

		case <-ticker.C:
			d.tick++
			requests := d.node.Step(d.tick, pending)
			pending = pending[:0]

			for _, req := range requests {
				switch r := req.(type) {
				case consensus.BroadcastStateRequest:
					// Send an OrchStateIndicator to each target pooler via gRPC:
					//   targets := r.Targets  // nil means all known poolers
					//   for _, id := range targets {
					//       poolerClients[id].ProposeState(ctx, &pb.ProposeStateRequest{
					//           State:               protoFromConsensusState(r.State),
					//           ExpectedPrimaryTerm: r.ExpectedPrimaryTerm,
					//       })
					//   }
					// The pooler gRPC handler converts the response to a
					// PoolerResponseIndicator and pushes it back to this indicators channel.
					_ = r
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ── PoolerDriver ─────────────────────────────────────────────────────────────

// PoolerDriver runs a PoolerNode state machine at a fixed tick rate. It is
// constructed with an AtomicStateFile and a PostgresApplier; NewPoolerNode
// automatically loads any previously committed state from the file on startup,
// so crash recovery requires no additional calls.
//
// In production, a SIGTERM handler pushes a TerminateIndicator onto the indicators
// channel so the pooler can record the postgres shutdown in its next Step() call.
type PoolerDriver struct {
	node *consensus.PoolerNode
	tick int64
	// orchClients map[consensus.NodeID]pb.OrchServiceClient
}

// NewPoolerDriver creates a PoolerDriver. The PoolerNode loads its last committed
// state from stateFile automatically, so crash recovery requires no extra startup.
func NewPoolerDriver(
	id consensus.NodeID,
	stateFile *AtomicStateFile,
	applier *PostgresApplier,
) *PoolerDriver {
	node := consensus.NewPoolerNode(id, stateFile, applier)
	return &PoolerDriver{node: node}
}

// Run starts the pooler tick loop. It returns when ctx is cancelled.
func (d *PoolerDriver) Run(ctx context.Context, indicators <-chan consensus.Indicator) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var pending []consensus.Indicator

	for {
		select {
		case ind := <-indicators:
			// Buffer incoming indicators for the next tick. Sources:
			//   - gRPC ProposeState handler → OrchStateIndicator
			//   - SIGTERM signal handler    → TerminateIndicator
			pending = append(pending, ind)

		case <-ticker.C:
			d.tick++
			requests := d.node.Step(d.tick, pending)
			pending = pending[:0]

			for _, req := range requests {
				switch r := req.(type) {
				case consensus.PoolerResponseRequest:
					// Reply to the orch that proposed the state:
					//   orchClients[r.ToOrch].PoolerResponse(ctx, &pb.PoolerResponseRequest{
					//       Accepted:     r.Accepted,
					//       KnownTerm:    r.KnownTerm,
					//       KnownCoordId: string(r.KnownCoordID),
					//   })
					_ = r

				case consensus.PoolerStatusUpdateRequest:
					// Broadcast committed state + postgres status to all known orchs:
					//   for _, client := range orchClients {
					//       client.PoolerStatus(ctx, &pb.PoolerStatusRequest{
					//           Applied:        r.Applied,
					//           PostgresStatus: protoFromPostgresStatus(r.PostgresStatus),
					//           State:          protoFromPersistentState(r.State),
					//       })
					//   }
					_ = r
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
