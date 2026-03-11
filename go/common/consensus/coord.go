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
// # Emergency path — coordinator-led term change (Stage 3)
//
// When the primary becomes unreachable the coordinator runs a two-phase protocol:
//
//  1. Recruit: send RecruitRequest to all cohort members. Each recruited node
//     durably commits to this coordinator's authority range and withdraws from
//     write quorum for the old primary (stopping WAL ACKs or entering read-only).
//     Recruitment continues until RevokesAndSamplesAllRevocationSets is satisfied.
//
//  2. Establish: write a new Term to the best available candidate (chosen by
//     highest committed PolicySeq). On success, propagate the new Term to all
//     other recruited members so they can update their committed state and
//     reconnect to the new primary.
//
// Note: the coordinator may also discover mid-recruitment that the primary has
// recovered or that another coordinator has already completed a valid term change.
// TODO: add a "release" message for the coordinator to signal recruited nodes
// that no term change was needed and they may resume normal quorum participation.
//
// # State
//
// All state is ephemeral — a restarted coordinator re-learns the cluster by
// processing PoolerStatusIndicator and PoolerDiscoveredIndicator updates.
type CoordNode struct {
	id           NodeID
	targetPolicy DurabilityPolicy // nil = manual mode

	// known tracks each pooler the coord has been told about. Values are
	// updated each time a PoolerStatusIndicator arrives.
	known map[NodeID]*knownPooler

	// pendingWrite is set when a WritePolicyRequest has been emitted and we
	// are waiting for a WritePolicyResponseIndicator. Only one write is in
	// flight at a time.
	pendingWrite *pendingPolicyWrite

	// healthTimeoutTicks is the number of ticks without a PoolerStatusIndicator
	// before a pooler is considered unreachable. Zero disables timeout-based
	// staleness checking.
	healthTimeoutTicks int64

	// writeTimeoutTicks is the number of ticks to wait for a WritePolicyResponse
	// before abandoning the in-flight write and retrying. Zero disables the
	// timeout (suitable when the network is reliable and drops are impossible).
	writeTimeoutTicks int64

	// pendingRecruitment is set while the coordinator is in the recruitment
	// phase of emergency failover. Cleared once enough nodes are recruited and
	// the establishing write is dispatched, or on Restart.
	pendingRecruitment *pendingRecruitment
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

// recruitmentPhase tracks which step of the emergency failover protocol is active.
type recruitmentPhase int

const (
	// recruitPhaseRecruiting: sending RecruitRequests and collecting responses
	// until RevokesAndSamplesAllRevocationSets is satisfied.
	recruitPhaseRecruiting recruitmentPhase = iota

	// recruitPhaseShadowWAL: sending WriteShadowWALRequests to all recruited
	// nodes and waiting for WriteShadowWALAckedIndicators.
	recruitPhaseShadowWAL
)

// shadowWALRetryTicks is the number of ticks to wait for a WriteShadowWALAcked
// response before re-sending to unacked nodes. This handles the case where a
// recruited node crashes mid-phase and needs to be retried after it restarts.
const shadowWALRetryTicks int64 = 50

// pendingRecruitment tracks an in-flight emergency recruitment phase.
type pendingRecruitment struct {
	atTermSeq     int64                    // base term seq the coordinator is working from
	proposedSeq   int64                    // seq that will be written on success
	baseTerm      *Term                    // term at the time recruitment started
	cohort        []CohortMember           // cohort members to recruit
	failedPrimary CohortMember             // the unreachable primary (excluded from candidacy)
	sentTo        map[NodeID]bool          // members we have sent RecruitRequests to
	responses     map[NodeID]recruitedResp // accepted responses keyed by pooler ID
	since         int64                    // tick when recruitment started (for timeout)

	// Shadow WAL phase fields (set when entering recruitPhaseShadowWAL).
	phase          recruitmentPhase
	newTerm        *Term           // the new term to write to shadow WAL on all recruited nodes
	propagateSent  map[NodeID]bool // nodes to which WriteShadowWALRequest has been sent
	propagateAcks  map[NodeID]bool // nodes that have acked the shadow WAL write
	shadowWALSince int64           // tick when shadow WAL phase started or last retried
}

type recruitedResp struct {
	term *Term // committed term from the pooler's response
	lsn  LSN   // WAL position at revocation time
}

// NewCoordNode creates a coordinator node.
// targetPolicy is the desired DurabilityPolicy to work toward as the cohort
// grows (e.g. AtLeastPolicy(3) for a 3-node HA cluster).
func NewCoordNode(id NodeID, targetPolicy DurabilityPolicy) *CoordNode {
	return &CoordNode{
		id:                id,
		targetPolicy:      targetPolicy,
		known:             make(map[NodeID]*knownPooler),
		writeTimeoutTicks: 150,
	}
}

// SetHealthTimeout configures a staleness threshold: if a pooler has not sent
// a status update within ticks ticks, it is considered unreachable.
// Zero disables timeout-based staleness (the default).
func (c *CoordNode) SetHealthTimeout(ticks int64) {
	c.healthTimeoutTicks = ticks
}

// SetWriteTimeout configures how long the coordinator waits for a
// WritePolicyResponse before abandoning the in-flight write and retrying.
// Zero disables the timeout.
func (c *CoordNode) SetWriteTimeout(ticks int64) {
	c.writeTimeoutTicks = ticks
}

// Restart clears all ephemeral coordinator state. The coordinator re-learns
// the cluster by processing PoolerStatusIndicator updates on subsequent ticks.
func (c *CoordNode) Restart() {
	c.known = make(map[NodeID]*knownPooler)
	c.pendingWrite = nil
	c.pendingRecruitment = nil
}

// ClusterView is the coordinator's current best-known state of the cluster,
// computed from accumulated PoolerStatusIndicators.
type ClusterView struct {
	// HighestQuorumTerm is the highest-Seq Term for which the coordinator has
	// confirmed a write quorum: enough cohort members have reported applying
	// this version (or a later one) to satisfy the term's DurabilityPolicy.
	// This is the last known-good state of the cluster.
	// Nil if no version has confirmed quorum.
	HighestQuorumTerm *Term

	// HighestSeenTerm is the highest-Seq Term reported by any known pooler,
	// regardless of quorum. Nil if no term has been seen.
	// If HighestSeenTerm.Seq > HighestQuorumTerm.Seq, a partial leader-driven
	// term change exists and must be propagated before establishing a new primary.
	HighestSeenTerm *Term

	// PrimaryHealthy is true when the best-known primary is currently reachable.
	// The "best-known" primary is HighestQuorumTerm.Primary when a quorum-confirmed
	// version exists, falling back to HighestSeenTerm.Primary otherwise. The
	// primary is considered reachable when postgres is running and its last status
	// is within the configured health timeout.
	//
	// This is the key flag for deciding between the normal path (PrimaryHealthy=true)
	// and the emergency failover path (PrimaryHealthy=false).
	PrimaryHealthy bool
}

// ClusterView returns the coordinator's current cluster state assessment.
// tick is used for health-staleness checks when a health timeout is configured.
func (c *CoordNode) ClusterView(tick int64) ClusterView {
	highestSeen, highestQuorum := c.computeTermVersions()

	// Identify the best-known primary: prefer the quorum-confirmed version,
	// fall back to the highest seen when quorum is not yet confirmed.
	var primaryID NodeID
	if highestQuorum != nil {
		primaryID = highestQuorum.Primary
	} else if highestSeen != nil {
		primaryID = highestSeen.Primary
	}

	var primaryHealthy bool
	if primaryID != "" {
		if p, ok := c.known[primaryID]; ok {
			primaryHealthy = c.isHealthy(p, tick)
		}
	}
	return ClusterView{
		HighestSeenTerm:   highestSeen,
		HighestQuorumTerm: highestQuorum,
		PrimaryHealthy:    primaryHealthy,
	}
}

// computeTermVersions scans all known poolers and computes the two key Term
// views: the highest-Seq version seen from any pooler, and the highest-Seq
// version for which write quorum is confirmed.
func (c *CoordNode) computeTermVersions() (highestSeen, highestQuorum *Term) {
	// Build a map from Seq → term record using the term reported by each pooler.
	// All poolers with the same Seq hold the same immutable record.
	termsBySeq := make(map[int64]*Term)
	for _, p := range sortedmaps.Values(c.known) {
		r := p.state.CachedTerm
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

	// Walk versions from highest to lowest, returning the first with confirmed quorum.
	for seq := highestSeen.Seq; seq >= 1; seq-- {
		r, ok := termsBySeq[seq]
		if !ok {
			continue // coordinator has not seen this version
		}
		if c.isDurable(r) {
			highestQuorum = r
			break
		}
	}
	return highestSeen, highestQuorum
}

// isDurable returns true if enough cohort members have reported applying
// t.Seq (or a later version) to satisfy t.Policy.IsDurable. The primary is
// included in the acking set since it commits locally before propagating via
// WAL — this correctly handles AtLeast(1) where the primary alone is sufficient.
func (c *CoordNode) isDurable(t *Term) bool {
	if t.Policy == nil {
		return true // nil policy: no acks needed
	}
	var acking []CohortMember
	for _, m := range t.Members {
		p, ok := c.known[m.ID]
		if !ok || p.state.CachedTerm == nil || p.state.CachedTerm.Seq < t.Seq {
			continue
		}
		acking = append(acking, m)
	}
	return t.Policy.IsDurable(t.Members, acking)
}

// isHealthy returns true if a pooler is currently considered reachable.
// A pooler is unhealthy when its postgres is stopped or (if a health timeout
// is configured) when it has not sent a status update within the timeout window.
func (c *CoordNode) isHealthy(p *knownPooler, tick int64) bool {
	if p.pgStatus == PostgresStopped {
		return false
	}
	if c.healthTimeoutTicks > 0 && p.lastStatusTick > 0 && tick-p.lastStatusTick > c.healthTimeoutTicks {
		return false
	}
	return true
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
		case WritePolicyResponseIndicator:
			reqs = append(reqs, c.handleWriteResponse(v)...)
		case RecruitResponseIndicator:
			c.handleRecruitResponse(v)
		case WriteShadowWALAckedIndicator:
			c.handleWriteShadowWALAcked(v)
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

// handleRecruitResponse records an accepted RecruitResponseIndicator during
// the recruiting phase of emergency failover.
func (c *CoordNode) handleRecruitResponse(ind RecruitResponseIndicator) {
	if c.pendingRecruitment == nil || c.pendingRecruitment.phase != recruitPhaseRecruiting {
		return
	}
	if !ind.Accepted {
		return
	}
	c.pendingRecruitment.responses[ind.FromPooler] = recruitedResp{
		term: ind.Term,
		lsn:  ind.LSN,
	}
}

// handleWriteShadowWALAcked records an ack from a pooler in the shadow WAL
// phase of emergency failover.
func (c *CoordNode) handleWriteShadowWALAcked(ind WriteShadowWALAckedIndicator) {
	if c.pendingRecruitment == nil || c.pendingRecruitment.phase != recruitPhaseShadowWAL {
		return
	}
	if ind.Accepted {
		c.pendingRecruitment.propagateAcks[ind.FromPooler] = true
	}
}

// advance decides the next action for this tick. If the primary is healthy it
// drives the normal leader-led cohort expansion path; otherwise it delegates to
// advanceEmergency for coordinator-led failover. Called at the end of every
// Step() after all indicators have been processed.
func (c *CoordNode) advance(tick int64) []Request {
	// Expire timed-out in-flight writes.
	if c.pendingWrite != nil {
		if c.writeTimeoutTicks > 0 && tick-c.pendingWrite.since >= c.writeTimeoutTicks {
			c.pendingWrite = nil
			// TODO: Should we mark the primary as unhealthy and try again as a coordinator-initiated
			// term change?...
		}
	}

	view := c.ClusterView(tick)
	if !view.PrimaryHealthy {
		// Primary is unreachable: abort any in-flight leader-led write and run
		// the coordinator-led emergency failover protocol.
		c.pendingWrite = nil
		return c.advanceEmergency(tick, view)
	}

	// Primary is healthy: abort any in-flight emergency recruitment.
	c.pendingRecruitment = nil

	// Wait for any in-flight leader-led write to complete.
	if c.pendingWrite != nil {
		return nil
	}

	primaryID, primaryKnown := c.findPrimary()
	if primaryKnown == nil {
		return nil
	}

	currentTerm := primaryKnown.state.CachedTerm
	if currentTerm == nil {
		return nil // primary has no term yet; wait for status
	}

	// Manual mode: never auto-add observers.
	if c.targetPolicy == nil {
		return nil
	}

	// Autonomous expansion: add all observed replicas not yet in the cohort.
	observers := c.observers(currentTerm)
	if len(observers) == 0 {
		return nil // cohort is already up to date
	}

	// Add all observers in one write.
	newMembers := append(slices.Clone(currentTerm.Members), observers...)
	newTerm := Term{
		Seq:     currentTerm.Seq + 1,
		Primary: primaryID,
		Members: newMembers,
		Policy:  c.policyForMembers(newMembers),
	}

	c.pendingWrite = &pendingPolicyWrite{
		target: primaryID,
		term:   newTerm,
		since:  tick,
	}

	return []Request{WritePolicyRequest{
		TargetPooler: primaryID,
		FromCoord:    c.id,
		Term:         newTerm,
	}}
}

// advanceEmergency drives the two-phase coordinator-led emergency failover:
//  1. Recruiting: send RecruitRequests to all cohort members and collect acks
//     until RevokesAndSamplesAllRevocationSets is satisfied.
//  2. Shadow WAL: send WriteShadowWALRequests to all recruited nodes and wait
//     for WriteShadowWALAckedIndicators; term is established when all ack.
func (c *CoordNode) advanceEmergency(tick int64, view ClusterView) []Request {
	base := view.HighestSeenTerm
	if base == nil {
		base = view.HighestQuorumTerm
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
			atTermSeq:     base.Seq,
			proposedSeq:   base.Seq + 1,
			baseTerm:      base,
			cohort:        base.Members,
			failedPrimary: failedPrimary,
			sentTo:        make(map[NodeID]bool),
			responses:     make(map[NodeID]recruitedResp),
			since:         tick,
			phase:         recruitPhaseRecruiting,
		}
	}

	pr := c.pendingRecruitment
	switch pr.phase {
	case recruitPhaseRecruiting:
		return c.advanceRecruitingPhase(pr, tick)
	case recruitPhaseShadowWAL:
		return c.advanceShadowWALPhase(pr, tick)
	}
	return nil
}

// advanceRecruitingPhase sends RecruitRequests to unsent cohort members and
// transitions to the shadow WAL phase once RevokesAndSamplesAllRevocationSets
// is satisfied.
func (c *CoordNode) advanceRecruitingPhase(pr *pendingRecruitment, tick int64) []Request {
	var reqs []Request

	for _, m := range pr.cohort {
		if m.ID == pr.failedPrimary.ID {
			continue // don't contact the unreachable primary
		}
		if !pr.sentTo[m.ID] {
			pr.sentTo[m.ID] = true
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
		if _, ok := pr.responses[m.ID]; ok {
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

	pr.phase = recruitPhaseShadowWAL
	pr.newTerm = &newTerm
	pr.propagateSent = make(map[NodeID]bool)
	pr.propagateAcks = make(map[NodeID]bool)
	pr.shadowWALSince = tick

	reqs = append(reqs, c.sendWriteShadowWALRequests(pr)...)
	return reqs
}

// advanceShadowWALPhase sends any outstanding WriteShadowWALRequests and clears
// pendingRecruitment once all recruited nodes have acked.
func (c *CoordNode) advanceShadowWALPhase(pr *pendingRecruitment, tick int64) []Request {
	// Retry: if enough ticks have elapsed since the last send, re-queue any
	// unacked nodes so sendWriteShadowWALRequests will re-send to them. This
	// handles nodes that crashed between receiving the request and sending the ack.
	if tick-pr.shadowWALSince >= shadowWALRetryTicks {
		for _, nodeID := range sortedmaps.Keys(pr.propagateSent) {
			if !pr.propagateAcks[nodeID] {
				delete(pr.propagateSent, nodeID)
			}
		}
		pr.shadowWALSince = tick
	}

	reqs := c.sendWriteShadowWALRequests(pr)

	// Check whether all nodes we sent to have acked.
	if len(pr.propagateSent) == 0 {
		return reqs
	}
	for _, nodeID := range sortedmaps.Keys(pr.propagateSent) {
		if !pr.propagateAcks[nodeID] {
			return reqs // still waiting
		}
	}

	// All acks received: update our cached view and consider the term established.
	if p, ok := c.known[pr.newTerm.Primary]; ok {
		newTerm := *pr.newTerm
		p.state.CachedTerm = &newTerm
		p.state.Role = RolePrimary
	}
	c.pendingRecruitment = nil
	return reqs
}

// sendWriteShadowWALRequests emits WriteShadowWALRequests to any recruited node
// that has not yet been sent one.
func (c *CoordNode) sendWriteShadowWALRequests(pr *pendingRecruitment) []Request {
	if pr.newTerm == nil {
		return nil
	}
	// Compute BaseLSN as the maximum WAL position reported by recruited nodes.
	var baseLSN LSN
	for _, resp := range sortedmaps.Values(pr.responses) {
		if resp.lsn > baseLSN {
			baseLSN = resp.lsn
		}
	}
	var reqs []Request
	for _, nodeID := range sortedmaps.Keys(pr.responses) {
		if !pr.propagateSent[nodeID] {
			pr.propagateSent[nodeID] = true
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

// pickBestCandidate returns the NodeID of the recruited node with the highest
// committed term seq (most up-to-date WAL), excluding the failed primary.
// Ties are broken deterministically by NodeID (lexicographic ascending).
func (c *CoordNode) pickBestCandidate(pr *pendingRecruitment) NodeID {
	var bestID NodeID
	var bestSeq int64
	for nodeID, resp := range sortedmaps.All(pr.responses) {
		if nodeID == pr.failedPrimary.ID {
			continue
		}
		var seq int64
		if resp.term != nil {
			seq = resp.term.Seq
		}
		if bestID == "" || seq > bestSeq || (seq == bestSeq && nodeID < bestID) {
			bestSeq = seq
			bestID = nodeID
		}
	}
	return bestID
}

// findPrimary returns the NodeID and knownPooler for the active primary, or the
// zero ID and nil if none is found.
//
// TODO(stage3): strengthen this. Ideally we find a pooler whose current rules
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
