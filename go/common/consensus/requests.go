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

// RequestCorrelation can be embedded in any Request to associate an opaque
// correlation token with the exchange. The RequestHandler uses it to route the
// eventual response Request back to the node that originated the exchange
// without requiring production code to carry an explicit return address.
type RequestCorrelation struct {
	CorrelationID string // empty = no response routing needed
}

// --- Requests from CoordNode ---

// WriteShadowWALRequest asks the handler to deliver a new term to a recruited
// pooler's shadow WAL. The handler auto-generates a correlation ID so that the
// eventual WriteShadowWALAckedRequest can be routed back to the originating coord.
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
type WriteShadowWALRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	Term         Term
	BaseLSN      LSN
	ApplyNow     bool
}

func (WriteShadowWALRequest) consensusRequest() {}

// WriteShadowWALAckedRequest is emitted by a PoolerNode after it has durably
// persisted a PropagateTermIndicator to its shadow WAL. The RequestHandler
// routes it back to the coordinator that sent the WriteShadowWALRequest.
type WriteShadowWALAckedRequest struct {
	RequestCorrelation
	Accepted bool
}

func (WriteShadowWALAckedRequest) consensusRequest() {}

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

// WritePolicyRequest asks the RequestHandler to deliver a Term write to the
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
// it to route the eventual WritePolicyResponseRequest back to FromCoord without
// requiring any extra routing state in production code.
type WritePolicyRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	FromSeq      int64 // CAS base: primary's committed.PolicySeq() must equal this
	Term         Term
}

func (WritePolicyRequest) consensusRequest() {}

// --- Requests from PoolerNode ---

// PolicyRecordApplyRequest is emitted by the primary PoolerNode when it needs
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
// In production, the local driver goroutine handles this and delivers an
// ApplyRulesResponseIndicator back to the PoolerNode on completion.
// In simulation, the SimPooler wrapper intercepts this request (before it
// reaches the RequestHandler), simulates the apply, and queues the result
// indicator for the next tick.
type PolicyRecordApplyRequest struct {
	FromSeq int64 // CAS base: latestWALPolicySeq must equal this
	Term    Term
}

func (PolicyRecordApplyRequest) consensusRequest() {}

// WritePolicyResponseRequest is emitted by a PoolerNode in response to a
// WritePolicyIndicator, after the local postgres write has either succeeded or
// failed. Accepted is true if the record was durably committed. When false,
// CurrentSeq carries the primary's actual current policy seq so the coord can
// correct its next seq on retry without a separate status round-trip.
//
// CorrelationID is echoed from the WritePolicyIndicator that triggered this
// response. The RequestHandler uses it to route the response back to the
// originating node.
type WritePolicyResponseRequest struct {
	RequestCorrelation
	Accepted   bool
	CurrentSeq int64 // primary's current policy seq when Accepted=false
}

func (WritePolicyResponseRequest) consensusRequest() {}

// ApplyWALTermRequest is emitted by a PoolerNode to its sidecar when it
// receives a WriteShadowWALIndicator with ApplyNow=true. The sidecar applies GUC settings exactly
// as it would after seeing the term record arrive in the WAL stream (calling
// applyGUCForTerm), then delivers an ApplyRulesResponseIndicator back to the
// PoolerNode so it can persist the new committed state and clear its commitment.
type ApplyWALTermRequest struct {
	Term Term
}

func (ApplyWALTermRequest) consensusRequest() {}

// RevokeParticipationRequest is emitted by PoolerNode to its local sidecar
// asking it to stop this node from participating in write quorum under the
// current rules. The sidecar responds with RevokeParticipationResponseIndicator
// once the revocation is in effect.
//
// For a replica sidecar: stop forwarding WAL ACKs to the primary.
// For a primary sidecar: become read-only (reject new write transactions).
//
// In simulation, SimPooler intercepts this before RequestHandler sees it.
type RevokeParticipationRequest struct {
	RequestCorrelation
}

func (RevokeParticipationRequest) consensusRequest() {}

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

// --- Requests from driver/simulation nodes ---

// TerminateRequest is emitted by a driver node to signal graceful shutdown of
// a specific pooler. The RequestHandler converts it to a TerminateIndicator
// delivered to the target. In production this corresponds to a SIGTERM signal.
type TerminateRequest struct {
	Target NodeID
}

func (TerminateRequest) consensusRequest() {}

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

// PropagatePositionAckedRequest is emitted by a PoolerNode after it has
// durably applied the propagated history. The handler routes it back to the
// coordinator that sent the PropagatePositionRequest.
type PropagatePositionAckedRequest struct {
	RequestCorrelation
	Accepted bool
}

func (PropagatePositionAckedRequest) consensusRequest() {}

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
