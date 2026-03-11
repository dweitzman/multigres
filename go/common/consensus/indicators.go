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

package consensus

// Indicator is implemented by all incoming events in the consensus protocol.
// CoordNode and PoolerNode both receive Indicator values in their Step() calls.
type Indicator interface {
	consensusIndicator()
}

// --- Indicators for PoolerNode ---

// WritePolicyIndicator is delivered to the primary PoolerNode when a CoordNode
// requests a Term change. The primary validates the CAS (Term.Seq must equal
// its current PolicySeq + 1); if valid, it emits a PolicyRecordApplyRequest
// to the local postgres driver.
//
// CorrelationID is set by the RequestHandler (or by tests injecting indicators
// directly). The primary echoes it back in WritePolicyResponseRequest so the
// handler can route the response to the originating coordinator.
type WritePolicyIndicator struct {
	CorrelationID string
	Term          Term // Term.Seq is the CAS key (must be currentSeq+1)
}

func (WritePolicyIndicator) consensusIndicator() {}

// ApplyRulesResponseIndicator is delivered to a PoolerNode by the local
// postgres driver after attempting to apply a Term change.
// Accepted=true means the record was committed and is propagating via WAL;
// Accepted=false means the transaction failed (e.g. compare-and-swap mismatch)
// and the pending apply should be aborted.
//
// On the replica path the WAL watcher always delivers Accepted=true since the
// record it is reporting has already been durably committed by the primary.
type ApplyRulesResponseIndicator struct {
	Term     Term
	Accepted bool
}

func (ApplyRulesResponseIndicator) consensusIndicator() {}

// RecruitIndicator is delivered to a PoolerNode by a coordinator to recruit
// it into a coordinator-led term change covering the range AtTermSeq→ProposedSeq.
//
// The pooler validates the request against any existing commitment and its
// current term, then durably persists the new commitment and stops participating
// in write quorum before responding.
type RecruitIndicator struct {
	CorrelationID string
	CoordID       NodeID
	AtTermSeq     int64
	ProposedSeq   int64
}

func (RecruitIndicator) consensusIndicator() {}

// RevokeParticipationResponseIndicator is delivered to a PoolerNode by its
// local sidecar once it has attempted to stop this node from participating in
// write quorum under the current rules.
//
// Accepted is true if the sidecar successfully stopped quorum participation
// (replica: WAL receiver disconnected; primary: read-only mode active).
// Accepted is false if the operation failed (e.g. postgres unreachable);
// the PoolerNode will forward the failure to the coordinator as a rejected
// RecruitResponseRequest.
//
// When Accepted is true, LSN and TermSeq carry the node's WAL position at the
// moment revocation completed. The coordinator uses these to rank candidates and
// pick the most up-to-date node as the new primary during emergency failover:
//   - For a replica: LSN is the last_replay_lsn once WAL replay has stabilised
//     (stopped advancing), and TermSeq is the seq of the last term record
//     the replica has replayed.
//   - For a primary: LSN is the last committed WAL position after any pending
//     write has been aborted, and TermSeq is the primary's current term seq.
type RevokeParticipationResponseIndicator struct {
	CorrelationID string
	Accepted      bool
	LSN           LSN   // last committed/replayed WAL position at revocation time
	TermSeq       int64 // seq of most recently applied Term
}

func (RevokeParticipationResponseIndicator) consensusIndicator() {}

// WriteShadowWALIndicator is delivered to a recruited PoolerNode by a
// coordinator after reaching recruitment quorum. It instructs the node to
// persist the new term to shadow WAL, and optionally apply GUC settings
// immediately (primary_conninfo / synchronous_standby_names) so the node can
// resume replication or be promoted to primary without a separate message.
// The node must hold a RecruitmentCommitment that authorises Term.Seq.
//
// CorrelationID is echoed back in PropagateTermAckedRequest so the coordinator
// can track which nodes have durably persisted the shadow WAL entry.
//
// BaseLSN anchors the shadow entry to the real WAL timeline: the node must
// have receivedLSN >= BaseLSN before appending, ensuring all recruited nodes
// place the shadow entry at the same epoch in the WAL history. The coordinator
// computes BaseLSN as the maximum LSN across all recruited nodes' responses.
//
// ApplyNow, when true, asks the sidecar to apply the term's GUC settings
// immediately in addition to writing the shadow WAL. This allows a coordinator
// to complete a term change in a single round-trip rather than two.
type WriteShadowWALIndicator struct {
	CorrelationID string
	Term          Term
	BaseLSN       LSN
	ApplyNow      bool
}

func (WriteShadowWALIndicator) consensusIndicator() {}

// WriteShadowWALAckedIndicator is delivered to a CoordNode when a pooler has
// durably persisted a WriteShadowWALIndicator to its shadow WAL. Accepted=true
// means the shadow WAL write succeeded and the coordinator may count this
// node toward the propagation quorum.
type WriteShadowWALAckedIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
}

func (WriteShadowWALAckedIndicator) consensusIndicator() {}

// TerminateIndicator is delivered to a PoolerNode to signal graceful shutdown.
// The pooler records PostgresStopped and emits a final status update.
// In production this corresponds to a SIGTERM sent to the multipooler process.
type TerminateIndicator struct{}

func (TerminateIndicator) consensusIndicator() {}

// --- Indicators for CoordNode ---

// WritePolicyResponseIndicator is delivered to a CoordNode when the target
// primary has completed handling a WritePolicyIndicator. Accepted=true means
// the record was committed and is propagating via WAL. When false, CurrentSeq
// carries the primary's actual current policy seq so the coord can compute the
// correct next seq on retry without a separate round-trip.
//
// CorrelationID is echoed from the corresponding WritePolicyIndicator.
type WritePolicyResponseIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
	CurrentSeq    int64 // set when Accepted=false
}

func (WritePolicyResponseIndicator) consensusIndicator() {}

// RecruitResponseIndicator is delivered to a CoordNode when a pooler has
// responded to a RecruitIndicator. Term carries the pooler's committed term so
// the coordinator can identify the best candidate for promotion. LSN reports
// the node's WAL position at revocation time; the coordinator uses the maximum
// LSN across all recruited nodes as the BaseLSN for shadow WAL writes.
type RecruitResponseIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
	Term          *Term // pooler's committed term at the time of response
	LSN           LSN   // WAL position at revocation time; zero when Accepted=false
}

func (RecruitResponseIndicator) consensusIndicator() {}

// PoolerStatusIndicator is delivered to CoordNode when a pooler broadcasts its
// status. It carries the pooler's full committed state (including the current
// Term), postgres operational status, and static node properties.
type PoolerStatusIndicator struct {
	PoolerID       NodeID
	State          PoolerPersistentState
	PostgresStatus PostgresStatus
	Properties     NodeProperties
}

func (PoolerStatusIndicator) consensusIndicator() {}

// PoolerDiscoveredIndicator is delivered to CoordNode when a new pooler
// registers in etcd (or is otherwise discovered by the provisioner).
type PoolerDiscoveredIndicator struct {
	PoolerID NodeID
}

func (PoolerDiscoveredIndicator) consensusIndicator() {}

// PoolerRemovedIndicator is delivered to CoordNode when a pooler deregisters
// from etcd.
type PoolerRemovedIndicator struct {
	PoolerID NodeID
}

func (PoolerRemovedIndicator) consensusIndicator() {}
