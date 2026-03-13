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

// Request is implemented by all outgoing messages emitted by consensus nodes.
// The RequestHandler converts Requests into Indicators delivered to target nodes.
type Request interface {
	consensusRequest()
}

// Indicator is implemented by all incoming events in the consensus protocol.
// CoordNode and PoolerNode both receive Indicator values in their Step() calls.
type Indicator interface {
	consensusIndicator()
}

// RequestCorrelation can be embedded in any Request to associate an opaque
// correlation token with the exchange. The RequestHandler uses it to route the
// eventual response Request back to the node that originated the exchange
// without requiring production code to carry an explicit return address.
type RequestCorrelation struct {
	CorrelationID string // empty = no response routing needed
}

// --- Normal (leader-driven) path ---
//
// The current primary writes a new Term directly (e.g. cohort expansion). The
// coordinator requests the write; the primary validates, applies it via its
// sidecar, and responds.

// LeaderWritePolicyRequest asks the RequestHandler to deliver a Term write to the
// target pooler (which must be the current primary). The primary validates the
// compare-and-swap: committed.PolicySeq() must equal FromSeq. If it does not,
// the write is rejected and the primary returns its current seq so the coord
// can retry with the correct next seq.
//
// The coordinator sets FromSeq to the base seq it is writing from. For a
// normal sequential write this is currentTerm.Seq; for a seq-bump write it
// is the current quorum term's seq, with Term.Seq jumping further ahead.
//
// The RequestHandler auto-generates a correlation ID for each request and uses
// it to route the eventual LeaderWritePolicyResponseRequest back to FromCoord without
// requiring any extra routing state in production code.
type LeaderWritePolicyRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	FromSeq      int64 // CAS base: primary's committed.PolicySeq() must equal this
	Term         Term
}

func (LeaderWritePolicyRequest) consensusRequest() {}

// LeaderWritePolicyIndicator is delivered to the primary PoolerNode when a CoordNode
// requests a Term change. The primary validates the CAS (committed.PolicySeq()
// must equal FromSeq); if valid, it emits a SidecarApplyLeaderPolicyRequest to the
// local postgres driver.
//
// Using an explicit FromSeq rather than requiring Term.Seq == PolicySeq+1
// allows coordinators to write seq-bump terms (jumping more than one seq at a
// time) to unblock nodes stuck on a higher term without a commitment.
//
// CorrelationID is set by the RequestHandler (or by tests injecting indicators
// directly). The primary echoes it back in LeaderWritePolicyResponseRequest so the
// handler can route the response to the originating coordinator.
type LeaderWritePolicyIndicator struct {
	CorrelationID string
	FromSeq       int64 // CAS base: committed.PolicySeq() must equal this
	Term          Term
}

func (LeaderWritePolicyIndicator) consensusIndicator() {}

// SidecarApplyLeaderPolicyRequest is emitted by the primary PoolerNode when it needs
// the local postgres driver to apply a Term change. The driver is responsible
// for the full apply sequence: updating synchronous_standby_names and then
// committing the SQL transaction. If the transaction fails, the driver must
// roll back the replication settings change and deliver a failure indicator.
//
// FromSeq is the CAS base: the driver must reject the apply if the WAL already
// contains a term record at or beyond Term.Seq (i.e. latestWALPolicySeq !=
// FromSeq). This guards against a stale apply request landing after a
// concurrent write has already advanced the WAL.
//
// In production, the local driver goroutine handles this and delivers a
// SidecarApplyResponseIndicator back to the PoolerNode on completion.
// In simulation, the SimPooler wrapper intercepts this request (before it
// reaches the RequestHandler), simulates the apply, and queues the result
// indicator for the next tick.
type SidecarApplyLeaderPolicyRequest struct {
	FromSeq int64 // CAS base: latestWALPolicySeq must equal this
	Term    Term
}

func (SidecarApplyLeaderPolicyRequest) consensusRequest() {}

// SidecarApplyResponseIndicator is delivered to a PoolerNode by the local
// postgres driver after attempting to apply a Term change.
// Accepted=true means the record was committed and is propagating via WAL;
// Accepted=false means the transaction failed (e.g. compare-and-swap mismatch)
// and the pending apply should be aborted.
//
// On the replica path the WAL watcher always delivers Accepted=true since the
// record it is reporting has already been durably committed by the primary.
//
// This indicator is the shared response type for all sidecar apply operations:
// SidecarApplyLeaderPolicyRequest (leader path) and SidecarApplyTermSettingsRequest
// (coordinator propose and resume paths).
type SidecarApplyResponseIndicator struct {
	Term     Term
	Accepted bool
}

func (SidecarApplyResponseIndicator) consensusIndicator() {}

// LeaderWritePolicyResponseRequest is emitted by a PoolerNode in response to a
// LeaderWritePolicyIndicator, after the local postgres write has either succeeded or
// failed. Accepted is true if the record was durably committed. When false,
// CurrentSeq carries the primary's actual current policy seq so the coord can
// correct its next seq on retry without a separate status round-trip.
//
// CorrelationID is echoed from the LeaderWritePolicyIndicator that triggered this
// response. The RequestHandler uses it to route the response back to the
// originating node.
type LeaderWritePolicyResponseRequest struct {
	RequestCorrelation
	Accepted   bool
	CurrentSeq int64 // primary's current policy seq when Accepted=false
}

func (LeaderWritePolicyResponseRequest) consensusRequest() {}

// LeaderWritePolicyResponseIndicator is delivered to a CoordNode when the target
// primary has completed handling a LeaderWritePolicyIndicator. Accepted=true means
// the record was committed and is propagating via WAL. When false, CurrentSeq
// carries the primary's actual current policy seq so the coord can compute the
// correct next seq on retry without a separate round-trip.
//
// CorrelationID is echoed from the corresponding LeaderWritePolicyIndicator.
type LeaderWritePolicyResponseIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
	CurrentSeq    int64 // set when Accepted=false
}

func (LeaderWritePolicyResponseIndicator) consensusIndicator() {}

// --- Coordinator-led term change: Recruit phase ---
//
// The coordinator sends RecruitRequests to all cohort members. Each recruited
// node durably commits to this coordinator's authority range and withdraws
// from write quorum (stopping WAL ACKs or entering read-only mode) via its
// sidecar before responding.

// RecruitRequest asks the RequestHandler to deliver a RecruitIndicator to the
// target pooler. The handler auto-generates a correlation ID so that the
// eventual RecruitResponseRequest can be routed back to the originating coord.
type RecruitRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	AtTermSeq    int64
	ProposedSeq  int64
}

func (RecruitRequest) consensusRequest() {}

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

// SidecarRevokeParticipationRequest is emitted by PoolerNode to its local sidecar
// asking it to stop this node from participating in write quorum under the
// current rules. The sidecar responds with SidecarRevokeResponseIndicator
// once the revocation is in effect.
//
// For a replica sidecar: stop forwarding WAL ACKs to the primary.
// For a primary sidecar: become read-only (reject new write transactions).
//
// In simulation, SimPooler intercepts this before RequestHandler sees it.
type SidecarRevokeParticipationRequest struct {
	RequestCorrelation
}

func (SidecarRevokeParticipationRequest) consensusRequest() {}

// SidecarRevokeResponseIndicator is delivered to a PoolerNode by its
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
// pick the most up-to-date node as the new primary during a coordinator-led term change:
//   - For a replica: LSN is the last_replay_lsn once WAL replay has stabilised
//     (stopped advancing), and TermSeq is the seq of the last term record
//     the replica has replayed.
//   - For a primary: LSN is the last committed WAL position after any pending
//     write has been aborted, and TermSeq is the primary's current term seq.
type SidecarRevokeResponseIndicator struct {
	CorrelationID string
	Accepted      bool
	LSN           LSN   // last committed/replayed WAL position at revocation time
	TermSeq       int64 // seq of most recently applied Term
}

func (SidecarRevokeResponseIndicator) consensusIndicator() {}

// RecruitResponseRequest is the pooler's response to a RecruitIndicator.
// When Accepted is true, Position carries the pooler's NodePosition at the
// time revocation completed: Position.Term is the highest-Seq accepted term
// (from shadow WAL if one exists, otherwise the committed real-WAL term) and
// Position.LSN is the real WAL position. The coordinator uses these to detect
// work started by a previous coordinator (Position.Term.Seq == proposedSeq)
// and to pick the best promotion candidate.
//
// Commitment is the node's current durable commitment, if any and still
// relevant (AtTermSeq >= the node's committed policy seq). It is included on
// both accepted and rejected responses. On a rejection, the coordinator uses
// it to detect competing coordinators and bump its proposedSeq if needed.
type RecruitResponseRequest struct {
	RequestCorrelation
	Accepted   bool
	Position   NodePosition           // zero when Accepted=false
	Commitment *RecruitmentCommitment // non-nil if node holds a current commitment
}

func (RecruitResponseRequest) consensusRequest() {}

// RecruitResponseIndicator is delivered to a CoordNode when a pooler has
// responded to a RecruitIndicator. When Accepted is true, Position carries the
// pooler's NodePosition at revocation time: Position.Term is the highest-Seq
// accepted term (from shadow WAL if one exists, otherwise the committed
// real-WAL term) and Position.LSN is the real WAL position. The coordinator
// uses these to detect work started by a previous coordinator
// (Position.Term.Seq == proposedSeq) and to pick the best promotion candidate.
//
// Commitment mirrors RecruitResponseRequest.Commitment: the node's current
// durable commitment if still relevant, or nil.
type RecruitResponseIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
	Position      NodePosition           // zero when Accepted=false
	Commitment    *RecruitmentCommitment // non-nil if node holds a current commitment
}

func (RecruitResponseIndicator) consensusIndicator() {}

// --- Coordinator-led term change: Propagate phase ---
//
// If recruited nodes have unequal WAL positions, the coordinator sends
// PropagatePositionRequests to bring them all to the best candidate's position
// before writing the new term. Skipped when all nodes are already aligned.

// PropagatePositionRequest asks the handler to deliver a history-copy
// instruction to a recruited pooler. The pooler (via its sidecar) must
// replicate SourceNode's committed history (CachedTerm + ShadowWAL) up
// through TargetPosition, replacing its own history. In production this
// corresponds to pg_rewind followed by WAL streaming from SourceNode; in
// simulation the sidecar copies state directly.
//
// The handler auto-generates a correlation ID so the eventual
// PropagatePositionAckedRequest is routed back to the originating coord.
type PropagatePositionRequest struct {
	TargetPooler   NodeID
	FromCoord      NodeID
	SourceNode     NodeID       // node whose history to replicate
	TargetPosition NodePosition // copy history up through this position
}

func (PropagatePositionRequest) consensusRequest() {}

// PropagatePositionIndicator is delivered to a recruited PoolerNode after the
// local sidecar has completed copying history from SourceNode. The sidecar
// first writes the new committed state to durable storage (CachedTerm +
// ShadowWAL from SourceNode, up through TargetPosition), then this indicator
// signals PoolerNode to reload its committed state from storage and ack.
//
// In simulation SimPooler intercepts this indicator, performs the WAL copy
// directly (by reading SourceNode's WAL buffer), calls SaveCommittedState on
// the PoolerNode so the reloaded state reflects the copy, then lets the
// indicator reach PoolerNode normally. In production the sidecar does
// pg_rewind + WAL streaming and writes the result to storage before delivery.
type PropagatePositionIndicator struct {
	CorrelationID  string
	SourceNode     NodeID
	TargetPosition NodePosition
}

func (PropagatePositionIndicator) consensusIndicator() {}

// PropagatePositionAckedRequest is emitted by a PoolerNode after it has
// durably applied the propagated history. The handler routes it back to the
// coordinator that sent the PropagatePositionRequest.
type PropagatePositionAckedRequest struct {
	RequestCorrelation
	Accepted bool
}

func (PropagatePositionAckedRequest) consensusRequest() {}

// PropagatePositionAckedIndicator is delivered to a CoordNode when a pooler
// has durably applied a PropagatePositionIndicator.
type PropagatePositionAckedIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
}

func (PropagatePositionAckedIndicator) consensusIndicator() {}

// --- Coordinator-led term change: Propose phase ---
//
// The coordinator writes the new Term to shadow WAL on all recruited nodes.
// With ApplyNow=true, the sidecar also applies GUC settings immediately,
// allowing promotion or replica reconnection in a single round-trip.

// ProposeRequest asks the handler to deliver a new term to a recruited
// pooler's shadow WAL. The handler auto-generates a correlation ID so that the
// eventual ProposeAckedRequest can be routed back to the originating coord.
//
// BaseLSN anchors the shadow entry to the real WAL timeline: it is the last
// real WAL position the coordinator observed across all recruited nodes before
// writing this entry. The pooler validates receivedLSN >= BaseLSN before
// appending, ensuring all recruited nodes place the shadow entry at the same
// epoch in their WAL history. Together (BaseLSN, Term.Seq) uniquely and totally
// orders shadow WAL entries — first by epoch (BaseLSN), then by term sequence
// (Term.Seq) — giving the same strict ordering guarantee as real WAL.
//
// ApplyNow, when true, instructs the pooler to apply the term's GUC settings
// immediately in addition to writing to shadow WAL, so it can resume replication
// or be promoted without a separate follow-up message.
type ProposeRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	Term         Term
	BaseLSN      LSN
	ApplyNow     bool
}

func (ProposeRequest) consensusRequest() {}

// ProposeIndicator is delivered to a recruited PoolerNode by a
// coordinator after reaching recruitment quorum. It instructs the node to
// persist the new term to shadow WAL, and optionally apply GUC settings
// immediately (primary_conninfo / synchronous_standby_names) so the node can
// resume replication or be promoted to primary without a separate message.
// The node must hold a RecruitmentCommitment that authorises Term.Seq.
//
// CorrelationID is echoed back in ProposeAckedRequest so the coordinator
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
type ProposeIndicator struct {
	CorrelationID string
	Term          Term
	BaseLSN       LSN
	ApplyNow      bool
}

func (ProposeIndicator) consensusIndicator() {}

// SidecarApplyTermSettingsRequest is emitted by a PoolerNode to its sidecar when it
// receives a ProposeIndicator with ApplyNow=true or a ResumeIndicator. The sidecar
// applies GUC settings exactly as it would after seeing the term record arrive in
// the WAL stream (calling applyGUCForTerm), then delivers a SidecarApplyResponseIndicator
// back to the PoolerNode so it can persist the new committed state and clear its
// commitment.
type SidecarApplyTermSettingsRequest struct {
	Term Term
}

func (SidecarApplyTermSettingsRequest) consensusRequest() {}

// ProposeAckedRequest is emitted by a PoolerNode after it has durably
// persisted a ProposeIndicator to its shadow WAL. The RequestHandler
// routes it back to the coordinator that sent the ProposeRequest.
type ProposeAckedRequest struct {
	RequestCorrelation
	Accepted bool
}

func (ProposeAckedRequest) consensusRequest() {}

// ProposeAckedIndicator is delivered to a CoordNode when a pooler has
// durably persisted a ProposeIndicator to its shadow WAL. Accepted=true
// means the shadow WAL write succeeded and the coordinator may count this
// node toward the propagation quorum.
type ProposeAckedIndicator struct {
	CorrelationID string
	FromPooler    NodeID
	Accepted      bool
}

func (ProposeAckedIndicator) consensusIndicator() {}

// --- Coordinator-led term change: Resume phase ---
//
// The coordinator sends ResumeRequests to stale nodes — nodes whose committed
// term is behind the quorum-confirmed term — so they can apply the current
// term and resume replication without a full term-change round-trip. Also used
// to release stuck-revoked nodes whose recruitment was abandoned.
// Resume emits SidecarApplyTermSettingsRequest (defined above in propose phase)
// and receives SidecarApplyResponseIndicator (defined above in leader path).

// ResumeRequest asks the handler to deliver a ResumeIndicator to the target
// pooler. The coordinator sends this to bring stale nodes up to the
// quorum-confirmed term without running a new full term-change protocol.
// Fire-and-forget: no correlation ID, no ack expected.
type ResumeRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	Term         Term // the quorum-confirmed term to apply
}

func (ResumeRequest) consensusRequest() {}

// ResumeIndicator is delivered to a PoolerNode by a coordinator to bring it
// up to the current quorum-confirmed term. Used when a node is stuck: it was
// recruited during a coordinator-led term change but missed the propose phase,
// or was unreachable during recruitment and has since fallen behind.
//
// On receipt the node updates its committed term, updates GUC settings
// (primary_conninfo / synchronous_standby_names), and resumes replication.
// If the node's WAL has diverged from the term's primary it should first
// attempt to reconnect to the leader; if streaming is blocked it may run
// pg_rewind to catch up. This is safe because the term was already established
// without this node's participation, so any divergent transactions on this
// node are not required to satisfy the durability policy.
// Fire-and-forget: no ack is returned to the coordinator.
type ResumeIndicator struct {
	FromCoord NodeID
	Term      Term // the quorum-confirmed term to apply
}

func (ResumeIndicator) consensusIndicator() {}

// --- Status, membership, and control ---

// PoolerStatusUpdateRequest is emitted by a PoolerNode whenever its committed
// state changes (rules write committed, WAL record applied, postgres stopped,
// or after a crash-restart). The RequestHandler delivers it to all known
// CoordNodes as a PoolerStatusIndicator so the coord can track cluster state.
type PoolerStatusUpdateRequest struct {
	State          PoolerPersistentState
	PostgresStatus PostgresStatus
	Properties     NodeProperties
}

func (PoolerStatusUpdateRequest) consensusRequest() {}

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

// PoolerMembershipRequest is emitted by the discovery node when the set of
// registered poolers changes. The RequestHandler converts it into
// PoolerDiscoveredIndicator and PoolerRemovedIndicator messages delivered to
// CoordNodes. In production this is driven by an etcd watch stream.
//
// TargetCoord, if non-empty, restricts delivery to a single coord. This allows
// each coord to get an independent stream (with independent delivery delays)
// rather than a single broadcast.
type PoolerMembershipRequest struct {
	TargetCoord NodeID   // empty = all coords
	Discovered  []NodeID // poolers that newly appeared
	Removed     []NodeID // poolers that departed
}

func (PoolerMembershipRequest) consensusRequest() {}

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

// TerminateRequest is emitted by a driver node to signal graceful shutdown of
// a specific pooler. The RequestHandler converts it to a TerminateIndicator
// delivered to the target. In production this corresponds to a SIGTERM signal.
type TerminateRequest struct {
	Target NodeID
}

func (TerminateRequest) consensusRequest() {}

// TerminateIndicator is delivered to a PoolerNode to signal graceful shutdown.
// The pooler records PostgresStopped and emits a final status update.
// In production this corresponds to a SIGTERM sent to the multipooler process.
type TerminateIndicator struct{}

func (TerminateIndicator) consensusIndicator() {}
