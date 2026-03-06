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

package simulation

import (
	"fmt"

	"github.com/multigres/multigres/go/common/consensus"
)

// lsn is a Log Sequence Number, a monotonically increasing position in the WAL.
type lsn int64

// walEntry is one record in the simulated WAL buffer. Each entry either
// represents a user transaction (record == nil, just advances the LSN) or a
// DurabilityPolicyRecord change.
//
// TODO(failover): research why replication is expected to fail in postgres if a
// replica and primary have diverging transactions (e.g. after a split-brain or
// a failed failover), and try to simulate something similar to what postgres
// does. In a graceful switchover the WAL timeline is compatible and replication
// continues from the new primary without gap. In emergency failover there may
// be timeline divergence that requires something like pg_rewind before the
// stale node can resume replication. For Stage 1 (cohort expansion, no
// failover) the current representation is sufficient.
//
// TODO(reclamation): implement WAL reclamation via periodic checkpoint entries.
// A checkpoint marks a position at which all effects can be safely discarded if
// all cohort members have applied past it. For now the buffer grows unboundedly.
type walEntry struct {
	pos    lsn
	record *consensus.DurabilityPolicyRecord // nil = user transaction
}

// SimPooler wraps a PoolerNode and acts as the local postgres driver in
// simulation. It models a postgres instance that can operate in two modes:
//
//   - Primary mode: maintains the authoritative WAL, tracks which replicas
//     have received each LSN, and enforces write quorum via simulated
//     synchronous_standby_names before considering a write durable.
//
//   - Replica mode: pulls WAL entries directly from its configured primary
//     each tick (analogous to postgres's primary_conninfo), tracks the
//     highest LSN received, and delivers PolicyRecordAppliedIndicators to the
//     wrapped PoolerNode when policy records arrive.
//
// WAL replication is handled within Step() — each replica looks up its
// primary's SimPooler and reads new entries directly, mirroring how each
// postgres instance manages its own replication connection.
//
// SimPooler intercepts PolicyRecordApplyRequest before it reaches the
// RequestHandler, simulates the postgres SQL transaction and replication
// settings update, then queues PolicyRecordAppliedIndicator for the next
// PoolerNode.Step call.
type SimPooler struct {
	node *consensus.PoolerNode
	sim  *simType

	// wal is the WAL buffer. Primary appends entries here; replicas receive
	// entries via pullWAL. Each entry's pos is strictly increasing.
	wal []walEntry

	// nextPos is the next LSN to assign when appending a new entry (primary only).
	nextPos lsn

	// syncStandbys is the set of replicas listed in synchronous_standby_names
	// (primary only). Write quorum is determined by checking how many of these
	// nodes have ACKed the write LSN using syncPolicy.IsWriteQuorum.
	syncStandbys []consensus.NodeID
	syncPolicy   consensus.DurabilityPolicy // nil = AnyN(0), no ACKs required

	// replicaACK tracks the highest WAL position each replica has confirmed
	// receiving (primary only). Updated by pullWAL calls from replicas.
	replicaACK map[consensus.NodeID]lsn

	// pendingApply is a policy record appended to the WAL but not yet durable
	// (primary only): waiting for writeQuorumMet. At most one policy write can
	// be pending at a time (PoolerNode serialises writes).
	pendingApply    *consensus.DurabilityPolicyRecord
	pendingApplyPos lsn

	// receivedLSN is the highest WAL position this node has received from the
	// primary (replica only). On graceful switchover the WAL timeline is
	// compatible so this position remains valid against the new primary.
	receivedLSN lsn

	// primaryConnInfo is the primary this node is currently configured to
	// replicate from, analogous to postgres's primary_conninfo setting. Tracked
	// to detect when the replication target changes.
	primaryConnInfo consensus.NodeID

	// pendingWAL holds entries received from the primary that have not yet
	// been processed in a Step call.
	pendingWAL []walEntry

	// queuedIndicators holds indicators to deliver to the wrapped PoolerNode on
	// the next Step call (e.g. PolicyRecordAppliedIndicator once a write is durable).
	queuedIndicators []consensus.Indicator
}

// NewSimPooler creates a SimPooler wrapping the given PoolerNode. sim is the
// simulator used to look up peer SimPoolers for WAL replication each tick.
func NewSimPooler(node *consensus.PoolerNode, sim *simType) *SimPooler {
	return &SimPooler{
		node:       node,
		sim:        sim,
		replicaACK: make(map[consensus.NodeID]lsn),
	}
}

// Node returns the wrapped PoolerNode.
func (s *SimPooler) Node() *consensus.PoolerNode {
	return s.node
}

// ID returns the node's identifier.
func (s *SimPooler) ID() consensus.NodeID {
	return s.node.ID()
}

// AppendUserTx simulates a user (non-policy) transaction on the primary,
// advancing the WAL LSN. Useful in tests for verifying replica lag tracking
// or creating WAL gaps between policy entries.
func (s *SimPooler) AppendUserTx() {
	s.nextPos++
	s.wal = append(s.wal, walEntry{pos: s.nextPos})
}

// SyncStandbys returns the current simulated synchronous_standby_names set.
// Useful in tests for asserting that sync settings are updated correctly.
func (s *SimPooler) SyncStandbys() []consensus.NodeID {
	result := make([]consensus.NodeID, len(s.syncStandbys))
	copy(result, s.syncStandbys)
	return result
}

// Step processes indicators and advances the SimPooler state machine one tick.
//
// The method:
//  1. Pulls new WAL from the configured primary if this node is a replica.
//     Doing this first lets replicas registered before the primary in the
//     simulator step order feed ACKs to the primary within the same tick.
//  2. Checks if a pending primary write has reached write quorum (primary path).
//  3. Processes incoming WAL entries (replica path).
//  4. Calls PoolerNode.Step with all accumulated indicators.
//  5. Intercepts PolicyRecordApplyRequest from PoolerNode output.
//
// Returns the subset of requests to pass to the RequestHandler.
func (s *SimPooler) Step(tick int64, externalInds []consensus.Indicator) []consensus.Request {
	// Replica path: pull WAL from the configured primary.
	s.pullWAL()

	inds := make([]consensus.Indicator, 0, len(s.queuedIndicators)+len(externalInds)+len(s.pendingWAL))
	inds = append(inds, s.queuedIndicators...)
	inds = append(inds, externalInds...)
	s.queuedIndicators = nil

	// Primary path: check if a pending write has reached write quorum.
	if s.pendingApply != nil && s.writeQuorumMet(s.pendingApplyPos) {
		inds = append(inds, consensus.PolicyRecordAppliedIndicator{
			PolicyID: s.pendingApply.ID,
			Record:   *s.pendingApply,
		})
		s.applySyncSettings(s.pendingApply)
		s.pendingApply = nil
	}

	// Replica path: process WAL entries received from the primary.
	for _, e := range s.pendingWAL {
		if e.record != nil {
			inds = append(inds, consensus.PolicyRecordAppliedIndicator{
				PolicyID: e.record.ID,
				Record:   *e.record,
			})
		}
	}
	s.pendingWAL = nil

	// Step the wrapped PoolerNode with all accumulated indicators.
	reqs := s.node.Step(tick, inds)

	// Intercept PolicyRecordApplyRequest before forwarding to the RequestHandler.
	var forwarded []consensus.Request
	for _, req := range reqs {
		if apply, ok := req.(consensus.PolicyRecordApplyRequest); ok {
			s.handleApply(apply)
		} else {
			forwarded = append(forwarded, req)
		}
	}
	return forwarded
}

// pullWAL simulates WAL streaming replication. If this SimPooler is a replica
// with a configured primary, it pulls new WAL entries from the primary's buffer
// and updates the primary's replica ACK tracking.
//
// This mirrors how each postgres replica maintains its own replication
// connection to the primary (via primary_conninfo), rather than relying on a
// separate replication mediator.
//
// On graceful switchover the WAL timeline remains compatible and receivedLSN is
// preserved — the replica resumes from where it left off against the new
// primary. Emergency failover with timeline divergence may require additional
// handling (see walEntry TODO).
func (s *SimPooler) pullWAL() {
	state := s.node.CommittedState()
	if state.Role != consensus.RoleReplica || state.Primary == "" {
		return
	}

	// Track primary_conninfo changes so we can detect reconnections.
	s.primaryConnInfo = state.Primary

	primary := s.findSimPooler(s.primaryConnInfo)
	if primary == nil || primary.Node().CommittedState().Role != consensus.RolePrimary {
		return
	}

	entries := primary.walEntriesSince(s.receivedLSN)
	s.receiveWAL(entries)
	primary.ackLSN(s.ID(), s.receivedLSN)
}

// findSimPooler looks up a SimPooler by ID from the simulator's registered nodes.
func (s *SimPooler) findSimPooler(id consensus.NodeID) *SimPooler {
	for _, node := range s.sim.Nodes() {
		if sp, ok := node.(*SimPooler); ok && sp.ID() == id {
			return sp
		}
	}
	return nil
}

// handleApply simulates the local postgres driver applying a DurabilityPolicyRecord.
//
//  1. Validates the CAS (PreviousID must match the latest policy in this node's WAL).
//  2. Appends a new WAL entry at the next LSN.
//  3. If the write is immediately durable under current sync settings, queues
//     PolicyRecordAppliedIndicator for the next Step call and updates sync settings.
//  4. Otherwise, sets pendingApply to wait for replica ACKs.
//
// For cohort additions: sync settings are updated AFTER the write is durable
// under the old settings, so new replicas are not required to ACK the write
// that adds them.
func (s *SimPooler) handleApply(req consensus.PolicyRecordApplyRequest) {
	// CAS validation against the SimPooler's WAL state. The PoolerNode has
	// already validated against its committed state; this catches divergence
	// between the two views.
	currentID := s.latestWALPolicyID()
	if req.Record.PreviousID != currentID {
		panic(fmt.Sprintf("SimPooler CAS mismatch on %s: Record.PreviousID=%q, latest WAL policy=%q",
			s.node.ID(), req.Record.PreviousID, currentID))
	}

	record := req.Record
	s.nextPos++
	s.wal = append(s.wal, walEntry{pos: s.nextPos, record: &record})

	s.pendingApply = &record
	s.pendingApplyPos = s.nextPos

	// If write quorum is already met (e.g. no sync standbys required), the
	// write is immediately durable. Queue the indicator and update sync settings;
	// pendingApply is cleared so the quorum check at the top of the next Step
	// call is a no-op.
	if s.writeQuorumMet(s.nextPos) {
		s.queuedIndicators = append(s.queuedIndicators, consensus.PolicyRecordAppliedIndicator{
			PolicyID: record.ID,
			Record:   record,
		})
		s.applySyncSettings(&record)
		s.pendingApply = nil
	}
}

// writeQuorumMet returns true if the current sync settings are satisfied for
// a write at the given WAL position. A nil syncPolicy (no policy applied yet)
// is treated as AnyN(0): no ACKs required.
func (s *SimPooler) writeQuorumMet(pos lsn) bool {
	if s.syncPolicy == nil {
		return true
	}
	var acking []consensus.NodeID
	for _, standby := range s.syncStandbys {
		if s.replicaACK[standby] >= pos {
			acking = append(acking, standby)
		}
	}
	return s.syncPolicy.IsWriteQuorum(acking)
}

// applySyncSettings updates the simulated synchronous_standby_names from a
// newly applied DurabilityPolicyRecord. Sync standbys are all cohort members
// except this node itself.
func (s *SimPooler) applySyncSettings(record *consensus.DurabilityPolicyRecord) {
	var standbys []consensus.NodeID
	for _, id := range record.CohortMembers {
		if id != s.node.ID() {
			standbys = append(standbys, id)
		}
	}
	s.syncStandbys = standbys
	s.syncPolicy = record.Policy
}

// latestWALPolicyID returns the ID of the most recently appended policy record
// in this node's WAL. If the WAL contains no policy entries yet, it falls back
// to the PoolerNode's committed state (handles pre-initialized nodes whose WAL
// buffer is empty but committed state was loaded from storage).
func (s *SimPooler) latestWALPolicyID() consensus.PolicyID {
	for i := len(s.wal) - 1; i >= 0; i-- {
		if s.wal[i].record != nil {
			return s.wal[i].record.ID
		}
	}
	return s.node.CommittedState().PolicyVersion()
}

// walEntriesSince returns all WAL entries with position > after.
func (s *SimPooler) walEntriesSince(after lsn) []walEntry {
	var result []walEntry
	for _, e := range s.wal {
		if e.pos > after {
			result = append(result, e)
		}
	}
	return result
}

// ackLSN records that a replica has received WAL up to pos (primary only).
func (s *SimPooler) ackLSN(replicaID consensus.NodeID, pos lsn) {
	if pos > s.replicaACK[replicaID] {
		s.replicaACK[replicaID] = pos
	}
}

// receiveWAL accepts WAL entries from the primary. Only entries with
// pos > receivedLSN are accepted to prevent duplicate processing.
func (s *SimPooler) receiveWAL(entries []walEntry) {
	for _, e := range entries {
		if e.pos > s.receivedLSN {
			s.receivedLSN = e.pos
			s.pendingWAL = append(s.pendingWAL, e)
		}
	}
}
