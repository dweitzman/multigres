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

import (
	"math/rand/v2"
	"slices"

	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// CoordNode is the coordinator state machine.
//
// # Normal path — cohort expansion
//
// The coordinator monitors pooler statuses, identifies observers (poolers that
// are replicating but not yet in the cohort), and expands the cohort by writing
// Term updates to the primary via compare-and-swap.
//
// Adds and removes are always separate writes. This is required because the
// correct ordering of synchronous_standby_names changes relative to the rules
// write differs between the two cases:
//   - Adding cohort members: update sync settings AFTER the write (new replicas
//     don't need to ack the write that adds them).
//   - Removing cohort members: update sync settings BEFORE the write (must not
//     relax the quorum before the removal is durable).
//
// # Manual mode
//
// When targetPolicy is nil the coordinator operates in manual mode: it never
// autonomously adds observed replicas to the cohort.
//
// # Coordinator-led term change (Stage 3)
//
// When the primary becomes unreachable (or for a planned switchover) the coordinator
// runs a multi-phase protocol:
//
//  1. Recruit: send RecruitRequest to all cohort members. Each recruited node
//     durably commits to this coordinator's authority range and withdraws from
//     write quorum for the old primary (stopping WAL ACKs or entering read-only).
//     Recruitment continues until RevokesAndSamplesAllRevocationSets is satisfied.
//
//  2. Propagate (optional): if recruited nodes have unequal WAL positions, send
//     PropagatePositionRequests to bring them all to the best candidate's position.
//     Skipped when all nodes are already at the target position.
//
//  3. Propose: write the new Term to shadow WAL on all recruited nodes with
//     ApplyNow=true so each node simultaneously persists the term and transitions
//     to its new role (primary or replica). The term is established once all
//     recruited nodes ack.
//
// Note: the coordinator may also discover mid-recruitment that the primary has
// recovered or that another coordinator has already completed a valid term change.
// TODO: add a "release" message for the coordinator to signal recruited nodes
// that no term change was needed and they may resume normal quorum participation.
//
// The coordinator also sends ResumeRequests to stale nodes — nodes whose
// committed term is behind the quorum-confirmed term — so they can apply the
// current term and resume replication without a full term-change round-trip.
//
// # State
//
// All state is ephemeral — a restarted coordinator re-learns the cluster by
// processing PoolerStatusIndicator and PoolerDiscoveredIndicator updates.
type CoordNode struct {
	id           NodeID
	targetPolicy DurabilityPolicy // nil = manual mode
	ha           HighAvailabilityStrategy
	rng          *rand.Rand // used for random tiebreaking in candidate selection

	// known tracks each pooler the coord has been told about. Values are
	// updated each time a PoolerStatusIndicator arrives.
	known map[NodeID]*knownPooler

	// pendingWrite is set when a WritePolicyRequest has been emitted and we
	// are waiting for a WritePolicyResponseIndicator. Only one write is in
	// flight at a time.
	pendingWrite *pendingPolicyWrite

	// pendingRecruitment is set while a coordinator-led term change is in
	// progress. Cleared once all recruited nodes have acked the new term, or
	// on Restart.
	pendingRecruitment *pendingRecruitment

	// resumeSentTicks records the last tick at which a ResumeRequest was sent
	// to each pooler. Used to rate-limit sends: a Resume is not re-sent to the
	// same node until at least PhaseRetryTicks ticks have elapsed, preventing
	// message floods when nodes are slow to catch up.
	resumeSentTicks map[NodeID]int64

	// highestKnownCommitments maps atTermSeq to the highest-ProposedSeq
	// RecruitmentCommitment seen for that base term, learned from rejected
	// recruit responses and PoolerStatusIndicator broadcasts. Two uses:
	//   1. When starting a new recruitment for atTermSeq, initialize proposedSeq
	//      to highestKnownCommitments[atTermSeq].ProposedSeq+1 so we don't need
	//      a rejection round-trip to learn about competing coordinators.
	//   2. Mid-recruitment: bump proposedSeq when the known highest exceeds it.
	// Cleared on Restart since all coordinator state is ephemeral.
	//
	// TODO: prune entries whose atTermSeq is strictly below the highest quorum-
	// confirmed term seq. Once a term has reached write quorum, any commitment
	// anchored to an earlier seq can never be acted upon, so keeping those entries
	// only wastes memory.
	highestKnownCommitments map[int64]*RecruitmentCommitment

	// stuckRevokedSince is the tick at which stuck-revoked nodes were first
	// detected while the primary was healthy. Used to implement a grace period
	// before writing a seq-bump term: we wait HealthTimeoutTicks to confirm the
	// lower-seq quorum is still valid before concluding the higher-seq node is
	// definitively stuck.
	stuckRevokedSince int64
}

type knownPooler struct {
	state          PoolerPersistentState
	pgStatus       PostgresStatus
	properties     NodeProperties
	lastStatusTick int64 // tick at which the last PoolerStatusIndicator was received
}

type pendingPolicyWrite struct {
	target NodeID
	term   Term
	since  int64 // tick at which the write was dispatched
}

// recruitmentPhase tracks which step of the coordinator-led term change protocol is active.
type recruitmentPhase int

const (
	// recruitPhaseRecruiting: sending RecruitRequests and collecting responses
	// until RevokesAndSamplesAllRevocationSets is satisfied.
	recruitPhaseRecruiting recruitmentPhase = iota

	// recruitPhasePropagate: syncing WAL positions across recruited nodes by
	// sending PropagatePositionRequests. All recruited nodes are brought to the
	// best candidate's WAL position before the new term is written. After all
	// PropagatePositionAcked responses arrive, transitions to recruitPhasePropose.
	recruitPhasePropagate

	// recruitPhasePropose: writing the new term to shadow WAL on all recruited
	// nodes with ApplyNow=true so each node simultaneously persists the term and
	// transitions to its new role. The term is established once all nodes ack.
	recruitPhasePropose
)

// pendingRecruitment tracks an in-flight coordinator-led term change.
type pendingRecruitment struct {
	atTermSeq     int64          // base term seq the coordinator is working from
	proposedSeq   int64          // seq that will be written on success
	baseTerm      *Term          // term at the time recruitment started
	cohort        []CohortMember // cohort members to recruit
	failedPrimary CohortMember   // the unreachable primary (excluded from candidacy)

	// Per-phase message tracking: separate sent/acks maps per phase so each
	// phase's progress is unambiguous and late responses from earlier phases
	// don't corrupt later ones.
	recruitSince     int64                    // tick when recruitment started
	recruitSent      map[NodeID]bool          // RecruitRequests sent
	recruitResponses map[NodeID]recruitedResp // accepted RecruitResponse keyed by pooler ID

	phase         recruitmentPhase
	bestCandidate NodeID // selected new primary (set when entering propagate or propose)
	newTerm       *Term  // the new term to write to shadow WAL
	phaseSince    int64  // tick when the current post-recruit phase started or was last retried

	propagateSent map[NodeID]bool // PropagatePositionRequest sent
	propagateAcks map[NodeID]bool // PropagatePositionAcked received

	proposeSent map[NodeID]bool // WriteShadowWALRequest(ApplyNow=true) sent
	proposeAcks map[NodeID]bool // WriteShadowWALAcked for propose received
}

type recruitedResp struct {
	position NodePosition // WAL position and highest accepted term at revocation time
}

// NewCoordNode creates a coordinator node.
// targetPolicy is the desired DurabilityPolicy to work toward as the cohort
// grows (e.g. AtLeastPolicy(3) for a 3-node HA cluster).
// ha provides the high-availability policy that decides when to initiate a
// coordinator-led term change. Pass nil to use DefaultHighAvailability().
// rng is used to break ties randomly when multiple candidates are equally
// eligible for promotion during a coordinator-led term change. Pass nil to use a default
// global source (non-deterministic; avoid in tests).
func NewCoordNode(id NodeID, targetPolicy DurabilityPolicy, ha HighAvailabilityStrategy, rng *rand.Rand) *CoordNode {
	if ha == nil {
		ha = DefaultHighAvailability()
	}
	return &CoordNode{
		id:                      id,
		targetPolicy:            targetPolicy,
		ha:                      ha,
		rng:                     rng,
		known:                   make(map[NodeID]*knownPooler),
		highestKnownCommitments: make(map[int64]*RecruitmentCommitment),
		resumeSentTicks:         make(map[NodeID]int64),
	}
}

// Restart clears all ephemeral coordinator state. The coordinator re-learns
// the cluster by processing PoolerStatusIndicator updates on subsequent ticks.
func (c *CoordNode) Restart() {
	c.known = make(map[NodeID]*knownPooler)
	c.pendingWrite = nil
	c.pendingRecruitment = nil
	c.highestKnownCommitments = make(map[int64]*RecruitmentCommitment)
	c.resumeSentTicks = make(map[NodeID]int64)
	c.stuckRevokedSince = 0
}

// computeTermVersions scans all known poolers and computes the two key Term
// views: the highest-Seq version seen from any pooler, and the highest-Seq
// version for which write quorum is confirmed.
func (c *CoordNode) computeTermVersions() (highestSeen, highestQuorum *Term) {
	nodeTerms := make(map[NodeID]*Term, len(c.known))
	for id, p := range sortedmaps.All(c.known) {
		nodeTerms[id] = p.state.CachedTerm
	}
	return HighestTermVersions(nodeTerms)
}

// HighestTermVersions computes two Term views from a map of node ID → committed
// term: the highest-Seq term seen across any node, and the highest-Seq term for
// which write quorum is confirmed according to each term's own DurabilityPolicy.
//
// This is the same computation CoordNode uses internally and is exported so
// tests and tools can determine cluster quorum state without a live CoordNode.
func HighestTermVersions(nodeTerms map[NodeID]*Term) (highestSeen, highestQuorum *Term) {
	termsBySeq := make(map[int64]*Term)
	for _, r := range sortedmaps.Values(nodeTerms) {
		if r == nil {
			continue
		}
		if _, exists := termsBySeq[r.Seq]; !exists {
			termsBySeq[r.Seq] = r
		}
		if highestSeen == nil || r.Seq > highestSeen.Seq {
			highestSeen = r
		}
	}
	if highestSeen == nil {
		return nil, nil
	}

	// Sort descending so we only visit term versions that at least one node
	// has committed (avoids scanning every seq from highestSeen down to 1).
	seqs := sortedmaps.Keys(termsBySeq) // ascending
	slices.Reverse(seqs)                // now descending
	for _, seq := range seqs {
		r := termsBySeq[seq]
		if isTermDurable(r, nodeTerms) {
			highestQuorum = r
			break
		}
	}
	return highestSeen, highestQuorum
}

// isTermDurable returns true if the given term has achieved write quorum
// according to the provided map of node ID → committed term. The primary is
// included in the acking set since it commits locally before propagating via
// WAL — this correctly handles AtLeast(1) where the primary alone is sufficient.
func isTermDurable(t *Term, nodeTerms map[NodeID]*Term) bool {
	if t.Policy == nil {
		return true // nil policy: no acks needed
	}
	var acking []CohortMember
	for _, m := range t.Members {
		r, ok := nodeTerms[m.ID]
		if !ok || r == nil || r.Seq < t.Seq {
			continue
		}
		acking = append(acking, m)
	}
	return t.Policy.IsDurable(t.Members, acking)
}

// ShardStatus constructs the coordinator's current view of the cluster from
// accumulated PoolerStatusIndicators. It is the bridge between the coordinator's
// internal state and the pure HA policy functions in high_availability.go.
func (c *CoordNode) ShardStatus(tick int64) ShardStatus {
	highestSeen, highestQuorum := c.computeTermVersions()
	health := make(map[NodeID]NodeHealth, len(c.known))
	for id, p := range sortedmaps.All(c.known) {
		health[id] = NodeHealth{
			PostgresStatus: p.pgStatus,
			LastHeardTick:  p.lastStatusTick,
		}
	}
	return ShardStatus{
		Tick:              tick,
		HighestSeenTerm:   highestSeen,
		HighestQuorumTerm: highestQuorum,
		NodeHealth:        health,
	}
}

// ID returns the coordinator node's unique identifier.
func (c *CoordNode) ID() NodeID {
	return c.id
}

// KnownPoolerIDs returns the IDs of all poolers the coordinator currently
// tracks. Used by simulation infrastructure to reconcile discovery state.
func (c *CoordNode) KnownPoolerIDs() []NodeID {
	return sortedmaps.Keys(c.known)
}

// Step processes all indicators that arrived this tick and returns requests.
func (c *CoordNode) Step(tick int64, indicators []Indicator) []Request {
	var reqs []Request
	for _, ind := range indicators {
		switch v := ind.(type) {
		case PoolerDiscoveredIndicator:
			if _, ok := c.known[v.PoolerID]; !ok {
				c.known[v.PoolerID] = &knownPooler{}
			}
		case PoolerRemovedIndicator:
			delete(c.known, v.PoolerID)
		case PoolerStatusIndicator:
			if p, ok := c.known[v.PoolerID]; ok {
				p.state = v.State
				p.pgStatus = v.PostgresStatus
				p.properties = v.Properties
				p.lastStatusTick = tick
			}
			// Proactively learn about existing commitments from status broadcasts
			// so we start recruitment with an informed proposedSeq.
			c.observeCommitment(v.State.Commitment)
		case WritePolicyResponseIndicator:
			reqs = append(reqs, c.handleWriteResponse(v)...)
		case RecruitResponseIndicator:
			c.handleRecruitResponse(v)
		case WriteShadowWALAckedIndicator:
			c.handleWriteShadowWALAcked(v)
		case PropagatePositionAckedIndicator:
			c.handlePropagatePositionAcked(v)
		}
	}

	reqs = append(reqs, c.advance(tick)...)
	return reqs
}

// handleWriteResponse processes the primary's response to a WritePolicyRequest
// on the leader-led path. Updates the coordinator's cached view of the primary's
// term so the next write uses the correct Seq without waiting for a status broadcast.
func (c *CoordNode) handleWriteResponse(ind WritePolicyResponseIndicator) []Request {
	if c.pendingWrite == nil || ind.FromPooler != c.pendingWrite.target {
		return nil // stale or unexpected response
	}

	if ind.Accepted {
		// The write succeeded. Update our cached view of the primary's term
		// so the next write uses the correct Seq without waiting for a status
		// broadcast.
		if p, ok := c.known[ind.FromPooler]; ok {
			term := c.pendingWrite.term
			p.state.CachedTerm = &term
		}
	} else {
		// CAS mismatch: the primary's current seq is ind.CurrentSeq. Clear our
		// cached term if stale and wait for a PoolerStatusIndicator before retrying.
		if p, ok := c.known[ind.FromPooler]; ok {
			if p.state.CachedTerm == nil || p.state.CachedTerm.Seq != ind.CurrentSeq {
				p.state.CachedTerm = nil
			}
		}
	}
	c.pendingWrite = nil
	return nil
}

// observeCommitment records a commitment in highestKnownCommitments if it
// carries higher authority (greater ProposedSeq) than what we have already
// seen for the same AtTermSeq. Called from both status broadcasts and
// rejected recruit responses so we always have the most informed view.
func (c *CoordNode) observeCommitment(commitment *RecruitmentCommitment) {
	if commitment == nil {
		return
	}
	existing := c.highestKnownCommitments[commitment.AtTermSeq]
	if existing == nil || commitment.ProposedSeq > existing.ProposedSeq {
		c.highestKnownCommitments[commitment.AtTermSeq] = commitment
	}
}

// handleRecruitResponse records an accepted RecruitResponseIndicator during
// the recruiting phase. On rejection, records any commitment the pooler
// reported so we can bump proposedSeq if needed.
func (c *CoordNode) handleRecruitResponse(ind RecruitResponseIndicator) {
	// Always learn from the commitment field, regardless of acceptance.
	c.observeCommitment(ind.Commitment)

	if c.pendingRecruitment == nil || c.pendingRecruitment.phase != recruitPhaseRecruiting {
		return
	}
	if !ind.Accepted {
		return
	}
	c.pendingRecruitment.recruitResponses[ind.FromPooler] = recruitedResp{
		position: ind.Position,
	}
}

// handleWriteShadowWALAcked records an ack from a pooler in the propose phase.
func (c *CoordNode) handleWriteShadowWALAcked(ind WriteShadowWALAckedIndicator) {
	if c.pendingRecruitment == nil || c.pendingRecruitment.phase != recruitPhasePropose {
		return
	}
	if ind.Accepted {
		c.pendingRecruitment.proposeAcks[ind.FromPooler] = true
	}
}

// handlePropagatePositionAcked records an ack from a pooler in the propagate phase.
func (c *CoordNode) handlePropagatePositionAcked(ind PropagatePositionAckedIndicator) {
	if c.pendingRecruitment == nil || c.pendingRecruitment.phase != recruitPhasePropagate {
		return
	}
	if ind.Accepted {
		c.pendingRecruitment.propagateAcks[ind.FromPooler] = true
	}
}

// advance decides the next action for this tick. If the primary is healthy it
// drives the normal leader-led cohort expansion path; otherwise it delegates to
// advanceCoordLedChange. Called at the end of every Step() after all indicators
// have been processed.
func (c *CoordNode) advance(tick int64) []Request {
	// Expire timed-out in-flight writes.
	if c.pendingWrite != nil {
		if tick-c.pendingWrite.since >= WriteTimeoutTicks {
			c.pendingWrite = nil
			// TODO: Should we mark the primary as unhealthy and try again as a coordinator-initiated
			// term change?...
		}
	}

	status := c.ShardStatus(tick)
	if c.ha.NeedsLeaderFailover(status) {
		// Primary is unreachable: abort any in-flight leader-led write and run
		// the coordinator-led term change protocol.
		c.pendingWrite = nil
		return c.advanceCoordLedChange(tick, status)
	}

	// Primary is healthy: abort any in-flight coordinator-led term change.
	c.pendingRecruitment = nil

	// Send Resume to any stale nodes so they can catch up to the quorum term.
	reqs := c.advanceResume(tick, status.HighestQuorumTerm)

	// Wait for any in-flight leader-led write to complete.
	if c.pendingWrite != nil {
		return reqs
	}

	primaryID, primaryKnown := c.findPrimary()
	if primaryKnown == nil {
		return reqs
	}

	currentTerm := primaryKnown.state.CachedTerm
	if currentTerm == nil {
		return reqs // primary has no term yet; wait for status
	}

	// Manual mode: never auto-add observers.
	if c.targetPolicy == nil {
		return reqs
	}

	// Check for stuck-revoked nodes whose PolicySeq exceeds the quorum term,
	// making standard resumes ineffective. A seq-bump term write unblocks them.
	stuckSeq := c.maxStuckRevokedSeq(status.HighestQuorumTerm)

	// Autonomous expansion: add all observed replicas not yet in the cohort.
	observers := c.observers(currentTerm)
	switch {
	case len(observers) == 0 && stuckSeq == 0:
		// Nothing to do: cohort is current and no stuck-revoked nodes.
		c.stuckRevokedSince = 0
		return reqs

	case len(observers) == 0:
		// No expansion needed, but stuck-revoked nodes need a seq-bump write.
		// Apply a grace period before writing to avoid reactive writes during
		// transient states and to confirm the lower-seq quorum is still valid.
		if c.stuckRevokedSince == 0 {
			c.stuckRevokedSince = tick
		}
		if tick-c.stuckRevokedSince < HealthTimeoutTicks {
			return reqs
		}
		c.stuckRevokedSince = 0 // reset so we don't write again immediately

	default: // len(observers) > 0
		c.stuckRevokedSince = 0 // expansion in progress; reset grace period
	}

	// Determine new members: expand if observers exist, otherwise keep current cohort.
	var newMembers []CohortMember
	if len(observers) > 0 {
		newMembers = append(slices.Clone(currentTerm.Members), observers...)
	} else {
		newMembers = slices.Clone(currentTerm.Members)
	}

	// Determine new seq: at minimum the next increment; jump further if needed
	// to unblock stuck-revoked nodes so their resumes are no longer stale.
	newSeq := currentTerm.Seq + 1
	if stuckSeq >= newSeq {
		newSeq = stuckSeq + 1
	}

	newTerm := Term{
		Seq:     newSeq,
		Primary: primaryID,
		Members: newMembers,
		Policy:  c.policyForMembers(newMembers),
	}

	c.pendingWrite = &pendingPolicyWrite{
		target: primaryID,
		term:   newTerm,
		since:  tick,
	}

	return append(reqs, WritePolicyRequest{
		TargetPooler: primaryID,
		FromCoord:    c.id,
		FromSeq:      currentTerm.Seq, // CAS base: primary must still be at this seq
		Term:         newTerm,
	})
}

// advanceResume sends ResumeRequests to known poolers whose committed term is
// behind the quorum-confirmed term, so they can apply the current term and
// resume replication. Rate-limited to at most one message per node per
// PhaseRetryTicks ticks to avoid flooding nodes that are slow to respond.
func (c *CoordNode) advanceResume(tick int64, quorumTerm *Term) []Request {
	if quorumTerm == nil {
		return nil
	}
	var reqs []Request
	for id, p := range sortedmaps.All(c.known) {
		if p.state.CachedTerm != nil && p.state.CachedTerm.Seq >= quorumTerm.Seq && p.state.Commitment == nil {
			continue // already up to date and not revoked
		}
		lastSent := c.resumeSentTicks[id]
		if lastSent > 0 && tick-lastSent < PhaseRetryTicks {
			continue // rate-limit: too soon to resend
		}
		c.resumeSentTicks[id] = tick
		reqs = append(reqs, ResumeRequest{
			TargetPooler: id,
			FromCoord:    c.id,
			Term:         *quorumTerm,
		})
	}
	return reqs
}

// maxStuckRevokedSeq returns the highest CachedTerm.Seq among known poolers
// whose seq exceeds the quorum seq. Such nodes reject Resume messages (which
// use the quorum term) as stale, so a seq-bump write is required to advance
// the quorum seq past them before Resumes can take effect.
//
// This covers two cases:
//   - Stuck-revoked nodes: Commitment != nil AND CachedTerm.Seq > quorumTerm.Seq
//   - Zombie primaries: Commitment == nil AND CachedTerm.Seq > quorumTerm.Seq
//     (e.g. a node that completed a non-quorum write when the primary role was
//     about to switch; it will reject Resume for the lower quorum term).
//
// Returns 0 if no such nodes are found.
func (c *CoordNode) maxStuckRevokedSeq(quorumTerm *Term) int64 {
	if quorumTerm == nil {
		return 0
	}
	var max int64
	for _, p := range sortedmaps.Values(c.known) {
		if p.state.CachedTerm == nil {
			continue
		}
		if p.state.CachedTerm.Seq <= quorumTerm.Seq {
			continue // standard Resume would work; not stuck
		}
		if p.state.CachedTerm.Seq > max {
			max = p.state.CachedTerm.Seq
		}
	}
	return max
}

// advanceCoordLedChange drives the coordinator-led term change protocol
// through its Recruit → [Propagate →] Propose phases.
func (c *CoordNode) advanceCoordLedChange(tick int64, status ShardStatus) []Request {
	base := status.HighestSeenTerm
	if base == nil {
		base = status.HighestQuorumTerm
	}
	if base == nil {
		return nil // no known cluster state; cannot safely failover
	}

	if c.pendingRecruitment == nil {
		var failedPrimary CohortMember
		for _, m := range base.Members {
			if m.ID == base.Primary {
				failedPrimary = m
				break
			}
		}
		c.pendingRecruitment = &pendingRecruitment{
			atTermSeq:        base.Seq,
			proposedSeq:      base.Seq + 1,
			baseTerm:         base,
			cohort:           base.Members,
			failedPrimary:    failedPrimary,
			recruitSent:      make(map[NodeID]bool),
			recruitResponses: make(map[NodeID]recruitedResp),
			recruitSince:     tick,
			phase:            recruitPhaseRecruiting,
		}
	}

	pr := c.pendingRecruitment
	switch pr.phase {
	case recruitPhaseRecruiting:
		return c.advanceRecruitingPhase(pr, tick)
	case recruitPhasePropagate:
		return c.advancePropagatePhase(pr, tick)
	case recruitPhasePropose:
		return c.advanceProposePhase(pr, tick)
	}
	return nil
}

// advanceRecruitingPhase sends RecruitRequests to unsent cohort members and
// transitions to the propose phase once RevokesAndSamplesAllRevocationSets is
// satisfied.
func (c *CoordNode) advanceRecruitingPhase(pr *pendingRecruitment, tick int64) []Request {
	var reqs []Request

	// Retry: re-send to nodes that haven't responded after a timeout.
	// This recovers from nodes that were down when recruitment started and
	// whose initial RecruitRequest was dropped.
	if tick-pr.recruitSince >= PhaseRetryTicks {
		for _, nodeID := range sortedmaps.Keys(pr.recruitSent) {
			if _, hasResponse := pr.recruitResponses[nodeID]; !hasResponse {
				delete(pr.recruitSent, nodeID)
			}
		}
		pr.recruitSince = tick
	}

	for _, m := range pr.cohort {
		if m.ID == pr.failedPrimary.ID {
			continue // don't contact the unreachable primary
		}
		if !pr.recruitSent[m.ID] {
			pr.recruitSent[m.ID] = true
			reqs = append(reqs, RecruitRequest{
				TargetPooler: m.ID,
				FromCoord:    c.id,
				AtTermSeq:    pr.atTermSeq,
				ProposedSeq:  pr.proposedSeq,
			})
		}
	}

	// Build the set of recruited members from accepted responses.
	var recruitedMembers []CohortMember
	for _, m := range pr.cohort {
		if _, ok := pr.recruitResponses[m.ID]; ok {
			recruitedMembers = append(recruitedMembers, m)
		}
	}

	// Check whether we have enough recruited nodes to safely proceed.
	baseTerm := pr.baseTerm
	enoughRecruited := false
	if baseTerm.Policy == nil {
		enoughRecruited = len(recruitedMembers) > 0
	} else {
		enoughRecruited = baseTerm.Policy.RevokesAndSamplesAllRevocationSets(
			baseTerm.Members, recruitedMembers, pr.failedPrimary,
		)
	}
	if !enoughRecruited {
		return reqs
	}

	candidate := c.pickBestCandidate(pr)
	if candidate == "" {
		return reqs
	}

	newTerm := Term{
		Seq:     pr.proposedSeq,
		Primary: candidate,
		Members: baseTerm.Members,
		Policy:  baseTerm.Policy,
	}

	pr.phase = recruitPhasePropose
	pr.bestCandidate = candidate
	pr.newTerm = &newTerm
	pr.proposeSent = make(map[NodeID]bool)
	pr.proposeAcks = make(map[NodeID]bool)
	pr.phaseSince = tick

	reqs = append(reqs, c.sendProposeRequests(pr)...)
	return reqs
}

// advancePropagatePhase sends PropagatePositionRequests to bring all recruited
// nodes to the best candidate's WAL position, then transitions to propose.
func (c *CoordNode) advancePropagatePhase(pr *pendingRecruitment, tick int64) []Request {
	// Retry if timed out: clear sent flags for unacked nodes so they are re-sent.
	if tick-pr.phaseSince >= PhaseRetryTicks {
		for _, nodeID := range sortedmaps.Keys(pr.propagateSent) {
			if !pr.propagateAcks[nodeID] {
				delete(pr.propagateSent, nodeID)
			}
		}
		pr.phaseSince = tick
	}

	bestPos := pr.recruitResponses[pr.bestCandidate].position
	var reqs []Request
	for _, nodeID := range sortedmaps.Keys(pr.recruitResponses) {
		if nodeID == pr.bestCandidate {
			// Best candidate already has the target position.
			pr.propagateAcks[nodeID] = true
			continue
		}
		if !pr.propagateSent[nodeID] {
			pr.propagateSent[nodeID] = true
			reqs = append(reqs, PropagatePositionRequest{
				TargetPooler:   nodeID,
				FromCoord:      c.id,
				SourceNode:     pr.bestCandidate,
				TargetPosition: bestPos,
			})
		}
	}

	// Check whether all recruited nodes have acked.
	for _, nodeID := range sortedmaps.Keys(pr.recruitResponses) {
		if !pr.propagateAcks[nodeID] {
			return reqs
		}
	}

	// All propagated — transition to propose phase.
	pr.phase = recruitPhasePropose
	pr.proposeSent = make(map[NodeID]bool)
	pr.proposeAcks = make(map[NodeID]bool)
	pr.phaseSince = tick
	reqs = append(reqs, c.sendProposeRequests(pr)...)
	return reqs
}

// advanceProposePhase sends WriteShadowWALRequests (ApplyNow=true) to all
// recruited nodes and clears pendingRecruitment once all have acked. Using
// ApplyNow=true writes the shadow WAL entry and activates GUC settings in a
// single round-trip, completing the term change.
func (c *CoordNode) advanceProposePhase(pr *pendingRecruitment, tick int64) []Request {
	// Retry if timed out: clear sent flags for unacked nodes so they are re-sent.
	if tick-pr.phaseSince >= PhaseRetryTicks {
		for _, nodeID := range sortedmaps.Keys(pr.proposeSent) {
			if !pr.proposeAcks[nodeID] {
				delete(pr.proposeSent, nodeID)
			}
		}
		pr.phaseSince = tick
	}

	reqs := c.sendProposeRequests(pr)

	// Check whether all nodes we sent to have acked.
	if len(pr.proposeSent) == 0 {
		return reqs
	}
	for _, nodeID := range sortedmaps.Keys(pr.proposeSent) {
		if !pr.proposeAcks[nodeID] {
			return reqs // still waiting
		}
	}

	// All acks received: update our cached view and consider the term established.
	// Update ALL recruited nodes so that isDurable() immediately confirms quorum
	// at the new term seq, preventing the coordinator from treating the just-completed
	// failover as an incomplete one and kicking off a spurious follow-on recruitment.
	// Clear Commitment because the propose phase fulfilled it: the nodes will clear
	// their own commitments in handleApplyRulesResponse, and the coordinator's view
	// must reflect that to avoid spurious resume traffic.
	newTerm := *pr.newTerm
	for _, nodeID := range sortedmaps.Keys(pr.recruitResponses) {
		if p, ok := c.known[nodeID]; ok {
			p.state.CachedTerm = &newTerm
			p.state.Commitment = nil
			if nodeID == pr.newTerm.Primary {
				p.state.Role = RolePrimary
			} else {
				p.state.Role = RoleReplica
			}
		}
	}
	c.pendingRecruitment = nil
	return reqs
}

// sendProposeRequests emits WriteShadowWALRequests (ApplyNow=true) to any
// recruited node that has not yet been sent one.
func (c *CoordNode) sendProposeRequests(pr *pendingRecruitment) []Request {
	if pr.newTerm == nil {
		return nil
	}
	// Compute BaseLSN as the maximum real WAL position reported by recruited nodes.
	var baseLSN LSN
	for _, resp := range sortedmaps.Values(pr.recruitResponses) {
		if resp.position.LSN > baseLSN {
			baseLSN = resp.position.LSN
		}
	}
	var reqs []Request
	for _, nodeID := range sortedmaps.Keys(pr.recruitResponses) {
		if !pr.proposeSent[nodeID] {
			pr.proposeSent[nodeID] = true
			reqs = append(reqs, WriteShadowWALRequest{
				TargetPooler: nodeID,
				FromCoord:    c.id,
				Term:         *pr.newTerm,
				BaseLSN:      baseLSN,
				ApplyNow:     true,
			})
		}
	}
	return reqs
}

// pickBestCandidate returns the NodeID of the recruited node with the most
// advanced NodePosition (highest real WAL position, tiebroken by term Seq),
// excluding the failed primary. When multiple nodes share the best position,
// one is chosen at random using c.rng so that primary load is distributed
// evenly across failovers.
func (c *CoordNode) pickBestCandidate(pr *pendingRecruitment) NodeID {
	// Collect all candidates that are strictly tied for best position.
	var bestPos NodePosition
	var bestCandidates []NodeID
	for nodeID, resp := range sortedmaps.All(pr.recruitResponses) {
		if nodeID == pr.failedPrimary.ID {
			continue
		}
		switch {
		case len(bestCandidates) == 0 || resp.position.After(bestPos):
			bestPos = resp.position
			bestCandidates = []NodeID{nodeID}
		case !bestPos.After(resp.position): // equal
			bestCandidates = append(bestCandidates, nodeID)
		}
	}
	if len(bestCandidates) == 0 {
		return ""
	}
	if len(bestCandidates) == 1 || c.rng == nil {
		return bestCandidates[0]
	}
	return bestCandidates[c.rng.IntN(len(bestCandidates))]
}

// findPrimary returns the NodeID and knownPooler for the active primary, or the
// zero ID and nil if none is found.
//
// TODO: strengthen this. Ideally we find a pooler whose current rules
// satisfy its own durability requirements (i.e. the write quorum of the policy is
// met by the replicas currently known to be streaming), which is strong evidence of
// a working quorum. For now, a simple role+health check suffices.
func (c *CoordNode) findPrimary() (NodeID, *knownPooler) {
	for id, p := range sortedmaps.All(c.known) {
		if p.state.Role == RolePrimary && p.pgStatus == PostgresRunning {
			return id, p
		}
	}
	return "", nil
}

// observers returns known poolers not listed in the current cohort as
// CohortMembers (with their cached properties), sorted for deterministic output.
func (c *CoordNode) observers(currentTerm *Term) []CohortMember {
	inCohort := make(map[NodeID]bool)
	for _, m := range currentTerm.Members {
		inCohort[m.ID] = true
	}

	var result []CohortMember
	for _, id := range sortedmaps.Keys(c.known) {
		if !inCohort[id] {
			p := c.known[id]
			result = append(result, CohortMember{
				ID:         id,
				Properties: p.properties,
			})
		}
	}
	return result
}

// policyForMembers returns the best achievable policy for the given cohort,
// capped at the configured target policy. Uses AtLeastPolicy(len(members)) as
// the intermediate policy when the target is not yet achievable: it requires
// all current members to ack, providing the strongest guarantee possible until
// the target policy becomes achievable.
func (c *CoordNode) policyForMembers(members []CohortMember) DurabilityPolicy {
	if c.targetPolicy.IsAchievable(members) {
		return c.targetPolicy
	}
	return AtLeastPolicy(len(members))
}
