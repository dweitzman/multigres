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

	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// orchPhase is the current stage of the three-phase election state machine.
type orchPhase int

const (
	phaseIdle      orchPhase = iota
	phaseBegin               // establishing this orch as coordinator for the term
	phaseRevoke              // revoking the current primary; waiting for revocation quorum
	phaseEstablish           // appointing new primary; waiting for appointment quorum
)

// phaseProgress tracks the in-flight proposal state within a phase.
type phaseProgress struct {
	proposal    ConsensusState
	quorum      Quorum          // proposed quorum (set during phaseEstablish)
	confirmers  map[NodeID]bool // poolers that voted YES for this proposal
	appliers    map[NodeID]bool // poolers that have applied this proposal
	timeoutTick int64
}

// poolerKnowledge is the orch's last-known view of a single pooler.
type poolerKnowledge struct {
	statusSeq      int64
	state          PoolerPersistentState
	applied        bool
	postgresStatus PostgresStatus
}

// OrchNode is the coordinator state machine. It discovers poolers via etcd indicators,
// runs the three-phase protocol (Begin → Revoke → Establish), and confirms the
// appointment once both the new primary and enough sync replicas have applied the
// Establish proposal.
//
// All state is ephemeral: if an orch crashes and restarts it simply begins a new term,
// learning cluster state from pooler status indicators before making any proposal.
type OrchNode struct {
	id              NodeID
	policy          DurabilityPolicy
	bootstrapPolicy DurabilityPolicy
	knownPoolers    map[NodeID]poolerKnowledge

	term   int64
	seqNum int64

	phase    orchPhase
	progress *phaseProgress

	// appointed is true once an Establish phase has achieved quorum.
	// It is cleared when the confirmed primary reports PostgresStopped so a new
	// election begins. confirmedState and confirmedQuorum are preserved for
	// revocation computation in the next round.
	appointed bool

	// confirmedState is the last ConsensusState that achieved Establish quorum.
	// nil until the first successful appointment. Preserved across re-elections so
	// that the revocation quorum can be computed for the new round.
	confirmedState *ConsensusState

	// confirmedQuorum is the Quorum for confirmedState. It knows how to evaluate
	// revocation: whether the old primary can still collect write acks.
	confirmedQuorum Quorum

	// backoffUntil is the tick before which this orch will not start a new election.
	// Set when we discover our view is stale (we were trying to act and learned about
	// a higher term, meaning another coordinator is actively working). The jittered
	// pause gives the winning coordinator time to complete before we compete again.
	backoffUntil   int64
	pendingBackoff bool // triggers backoffUntil = tick + jitter in the next advance call

	// rng provides jitter for the backoff duration, seeded deterministically from the
	// node ID so different orch instances naturally desynchronise their retries.
	rng *rand.Rand
}

// NewOrchNode creates a new coordinator node with the given identity and durability
// policies. rng is used to jitter backoff durations when the orch discovers a competing
// coordinator; pass a seeded *rand.Rand so tests remain fully deterministic.
//
// policy governs write-quorum decisions (Revoke and Establish phases). It is also
// used to evaluate whether the previous write quorum has been broken when checking
// Begin-phase quorum.
//
// bootstrapPolicy governs the Begin-phase quorum when there is no previous write
// quorum to revoke (cluster bootstrap). In production this is typically configured
// separately from the write policy — e.g. via etcd or a command-line flag — to
// express the minimum number of poolers that must accept the new coordinator before
// a fresh election can proceed.
func NewOrchNode(id NodeID, policy DurabilityPolicy, bootstrapPolicy DurabilityPolicy, rng *rand.Rand) *OrchNode {
	return &OrchNode{
		id:              id,
		policy:          policy,
		bootstrapPolicy: bootstrapPolicy,
		knownPoolers:    make(map[NodeID]poolerKnowledge),
		rng:             rng,
	}
}

// ID returns the orch node's unique identifier.
func (n *OrchNode) ID() NodeID {
	return n.id
}

// Step processes all indicators and drives the election state machine forward.
func (n *OrchNode) Step(tick int64, indicators []Indicator) []Request {
	for _, ind := range indicators {
		switch v := ind.(type) {
		case PoolerDiscoveredIndicator:
			n.handlePoolerDiscovered(v)
		case PoolerRemovedIndicator:
			n.handlePoolerRemoved(v)
		case PoolerStatusIndicator:
			n.handlePoolerStatus(v)
		case PoolerResponseIndicator:
			n.handlePoolerResponse(v)
		}
	}

	// If the confirmed primary has stopped, clear the appointment so we re-elect.
	// We preserve confirmedState and confirmedQuorum for revocation computation.
	if n.appointed && n.confirmedPrimaryIsStopped() {
		n.appointed = false
	}

	// Check phase timeout: escalate term and restart from idle.
	if n.progress != nil && tick >= n.progress.timeoutTick {
		n.term++
		n.phase = phaseIdle
		n.progress = nil
	}

	return n.advance(tick)
}

func (n *OrchNode) handlePoolerDiscovered(ind PoolerDiscoveredIndicator) {
	if _, exists := n.knownPoolers[ind.PoolerID]; !exists {
		n.knownPoolers[ind.PoolerID] = poolerKnowledge{}
	}
}

func (n *OrchNode) handlePoolerRemoved(ind PoolerRemovedIndicator) {
	delete(n.knownPoolers, ind.PoolerID)
}

func (n *OrchNode) handlePoolerStatus(ind PoolerStatusIndicator) {
	info, exists := n.knownPoolers[ind.PoolerID]
	if !exists {
		return
	}
	if ind.StatusSeq <= info.statusSeq {
		return // stale
	}
	n.knownPoolers[ind.PoolerID] = poolerKnowledge{
		statusSeq:      ind.StatusSeq,
		state:          ind.State,
		applied:        ind.Applied,
		postgresStatus: ind.PostgresStatus,
	}

	// If this pooler reports a higher term than we knew about, our view was stale.
	if ind.State.VotedTerm > n.term {
		n.term = ind.State.VotedTerm
		if n.phase != phaseIdle {
			// We were trying to act on out-of-date information. Abandon the current
			// election attempt and back off so a competing coordinator can make progress.
			n.pendingBackoff = true
			n.phase = phaseIdle
			n.progress = nil
		}
	}

	// Record applied status for the current in-flight proposal.
	if n.progress != nil && ind.Applied &&
		ind.State.VotedTerm == n.progress.proposal.VotingTerm &&
		ind.State.VotedSeqNum == n.progress.proposal.SeqNum {
		n.progress.appliers[ind.PoolerID] = true
	}
}

func (n *OrchNode) handlePoolerResponse(ind PoolerResponseIndicator) {
	if n.progress == nil {
		return
	}
	// Discard responses that do not match the current proposal: they are late
	// deliveries from a previous round (different term or seq num).
	if ind.VotingTerm != n.progress.proposal.VotingTerm ||
		ind.SeqNum != n.progress.proposal.SeqNum {
		return
	}
	if ind.Accepted {
		n.progress.confirmers[ind.FromPooler] = true
		return
	}
	// Rejection: adopt the higher term or escalate on same-term coordinator conflict.
	// In either case we learned that another coordinator is actively working, so we
	// signal a backoff: our view was stale when we started this election attempt.
	if ind.KnownTerm > n.term {
		n.term = ind.KnownTerm
		n.pendingBackoff = true
	} else if ind.KnownTerm == n.term && ind.KnownCoordID != n.id && ind.KnownCoordID != "" {
		n.term++
		n.pendingBackoff = true
	}
	n.phase = phaseIdle
	n.progress = nil
}

// advance transitions the phase state machine and emits proposals as needed.
func (n *OrchNode) advance(tick int64) []Request {
	// Materialise any pending backoff now that we have the current tick.
	if n.pendingBackoff {
		n.pendingBackoff = false
		n.backoffUntil = tick + n.jitterTicks()
	}

	switch n.phase {
	case phaseIdle:
		if !n.appointed {
			// Before starting a new election, check whether another coordinator has
			// already established a primary. If so, adopt that state so we don't
			// compete unnecessarily.
			if n.learnEstablishedPrimary() {
				return nil
			}
			if tick < n.backoffUntil {
				return nil // waiting for backoff to expire
			}
			if n.hasAvailablePoolers() {
				return n.startBegin(tick)
			}
		}

	case phaseBegin:
		if n.beginQuorumMet() {
			return n.transitionFromBegin(tick)
		}

	case phaseRevoke:
		if n.revokeQuorumMet() {
			return n.startEstablish(tick)
		}

	case phaseEstablish:
		if n.establishQuorumMet() {
			proposalCopy := n.progress.proposal
			n.confirmedState = &proposalCopy
			n.confirmedQuorum = n.progress.quorum
			n.appointed = true
			n.phase = phaseIdle
			n.progress = nil
		}
	}
	return nil
}

func (n *OrchNode) startBegin(tick int64) []Request {
	n.term++
	n.seqNum = 0

	// Carry forward the last confirmed topology so poolers know the coordinator
	// is changing, not the primary (that change comes in Revoke/Establish).
	proposal := ConsensusState{
		VotingTerm: n.term,
		CoordID:    n.id,
		SeqNum:     n.nextSeqNum(),
		Phase:      PhaseBegin,
	}
	if n.confirmedState != nil {
		proposal.PrimaryTerm = n.confirmedState.PrimaryTerm
		proposal.Primary = n.confirmedState.Primary
		proposal.SyncReplicas = n.confirmedState.SyncReplicas
	}

	n.phase = phaseBegin
	n.progress = &phaseProgress{
		proposal:    proposal,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + electionTimeoutTicks,
	}
	return []Request{BroadcastStateRequest{State: proposal}}
}

// transitionFromBegin advances from the Begin phase.
// If there is no previous primary to revoke, it skips straight to Establish in the
// same tick (revocation is a no-op: nothing holds a write quorum).
func (n *OrchNode) transitionFromBegin(tick int64) []Request {
	if n.confirmedState == nil || n.confirmedState.Primary == "" {
		return n.startEstablish(tick)
	}
	return n.startRevoke(tick)
}

func (n *OrchNode) startRevoke(tick int64) []Request {
	oldPrimaryTerm := n.confirmedState.PrimaryTerm

	proposal := ConsensusState{
		VotingTerm: n.term,
		CoordID:    n.id,
		SeqNum:     n.nextSeqNum(),
		Phase:      PhaseRevoke,
		// PrimaryTerm=0 and Primary="" signal no primary is currently appointed.
	}

	n.phase = phaseRevoke
	n.progress = &phaseProgress{
		proposal:    proposal,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + electionTimeoutTicks,
	}
	return []Request{BroadcastStateRequest{
		State:               proposal,
		ExpectedPrimaryTerm: oldPrimaryTerm,
	}}
}

func (n *OrchNode) startEstablish(tick int64) []Request {
	candidates := n.buildCandidates()

	// If the current primary is still healthy, prefer it to avoid unnecessary failover.
	// If it crashed (or there is none), pass "" so the policy selects the best available
	// candidate — eventually by highest LSN once that data flows through pooler status.
	preferred := n.healthyConfirmedPrimary()

	quorum, ok := n.policy.ProposeQuorum(candidates, preferred)
	if !ok {
		// No valid quorum possible yet (e.g., insufficient healthy candidates).
		// Remain in the current phase until candidates appear or a timeout occurs.
		return nil
	}

	proposal := ConsensusState{
		VotingTerm:   n.term,
		CoordID:      n.id,
		SeqNum:       n.nextSeqNum(),
		Phase:        PhaseEstablish,
		PrimaryTerm:  n.term,
		Primary:      quorum.Primary(),
		SyncReplicas: quorum.SyncReplicas(),
	}

	n.phase = phaseEstablish
	n.progress = &phaseProgress{
		proposal:    proposal,
		quorum:      quorum,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + electionTimeoutTicks,
	}
	return []Request{BroadcastStateRequest{State: proposal}}
}

// beginQuorumMet returns true when enough poolers have committed to the new term
// that the previous write quorum can no longer form.
//
// When there is a previous write quorum (confirmedQuorum != nil), a pooler that
// commits a higher voting term stops acknowledging writes for the old primary term,
// so IsRevoked(confirmers) is the right condition: can the old primary still collect
// N acks from its sync replicas?
//
// On bootstrap (confirmedQuorum == nil), the bootstrapPolicy is asked to reconstruct
// a quorum over all currently known poolers; IsRevoked then checks whether the
// confirmer set satisfies that policy's threshold. Once learnEstablishedPrimary()
// sets confirmedQuorum (because the orch contacted poolers already in an established
// cohort), this bootstrap path is never taken again for the lifetime of the orch.
func (n *OrchNode) beginQuorumMet() bool {
	if n.confirmedQuorum != nil {
		return n.confirmedQuorum.IsRevoked(n.progress.confirmers)
	}
	q := n.bootstrapPolicy.ReconstructQuorum("", sortedmaps.Keys(n.knownPoolers))
	return q.IsRevoked(n.progress.confirmers)
}

// revokeQuorumMet returns true when the confirmed quorum reports that the old
// primary can no longer collect enough write acknowledgements.
func (n *OrchNode) revokeQuorumMet() bool {
	if n.confirmedState == nil || n.confirmedQuorum == nil {
		return true
	}
	return n.confirmedQuorum.IsRevoked(n.progress.appliers)
}

// establishQuorumMet returns true when the proposed quorum reports that the new
// primary and enough sync replicas have applied the Establish proposal.
func (n *OrchNode) establishQuorumMet() bool {
	if n.progress.quorum == nil {
		return false
	}
	return n.progress.quorum.IsEstablished(n.progress.appliers)
}

// buildCandidates converts the known pooler map into QuorumCandidates for the
// DurabilityPolicy. A pooler is healthy when postgres is not stopped; unknown
// status (not yet reported) is treated as healthy (potentially running).
func (n *OrchNode) buildCandidates() []QuorumCandidate {
	candidates := make([]QuorumCandidate, 0, len(n.knownPoolers))
	for id, info := range sortedmaps.All(n.knownPoolers) {
		candidates = append(candidates, QuorumCandidate{
			ID:      id,
			Healthy: info.postgresStatus != PostgresStopped,
		})
	}
	return candidates
}

// healthyConfirmedPrimary returns the current confirmed primary's ID if it is
// still healthy (not stopped), or "" if it has crashed or there is no confirmed
// primary. Used as a hint to the DurabilityPolicy to avoid unnecessary failovers.
func (n *OrchNode) healthyConfirmedPrimary() NodeID {
	if n.confirmedState == nil || n.confirmedState.Primary == "" {
		return ""
	}
	info, exists := n.knownPoolers[n.confirmedState.Primary]
	if !exists || info.postgresStatus == PostgresStopped {
		return "" // primary gone; let the policy pick the best available
	}
	return n.confirmedState.Primary
}

// hasAvailablePoolers returns true if at least one known pooler is not stopped.
func (n *OrchNode) hasAvailablePoolers() bool {
	for _, info := range sortedmaps.Values(n.knownPoolers) {
		if info.postgresStatus != PostgresStopped {
			return true
		}
	}
	return false
}

// confirmedPrimaryIsStopped returns true when the currently confirmed primary has
// reported PostgresStopped status or has been removed from the known poolers.
func (n *OrchNode) confirmedPrimaryIsStopped() bool {
	if n.confirmedState == nil || n.confirmedState.Primary == "" {
		return false
	}
	info, exists := n.knownPoolers[n.confirmedState.Primary]
	if !exists {
		return true
	}
	return info.postgresStatus == PostgresStopped
}

func (n *OrchNode) nextSeqNum() int64 {
	n.seqNum++
	return n.seqNum
}

// jitterTicks returns a backoff duration in [electionTimeoutTicks, 2*electionTimeoutTicks).
// The jitter breaks symmetry between orchs that discover a conflict at the same tick.
func (n *OrchNode) jitterTicks() int64 {
	return electionTimeoutTicks + n.rng.Int64N(electionTimeoutTicks)
}

// learnEstablishedPrimary checks whether the pooler status reports already show
// that an Establish quorum has been satisfied (another coordinator completed an
// election). If so, it adopts the confirmed state so we don't compete unnecessarily.
func (n *OrchNode) learnEstablishedPrimary() bool {
	for primaryID, primaryInfo := range sortedmaps.All(n.knownPoolers) {
		if !primaryInfo.applied || primaryInfo.postgresStatus == PostgresStopped {
			continue
		}
		if primaryInfo.state.Role != RolePrimary {
			continue
		}

		// Reconstruct the quorum from the primary's committed sync-replica set so
		// IsEstablished checks the historically committed quorum members, not what
		// the policy would propose today.
		quorum := n.policy.ReconstructQuorum(primaryID, primaryInfo.state.SyncReplicas)

		// Build appliers: nodes that applied the same proposal as the primary.
		appliers := make(map[NodeID]bool)
		for id, info := range sortedmaps.All(n.knownPoolers) {
			if info.applied &&
				info.state.VotedTerm == primaryInfo.state.VotedTerm &&
				info.state.VotedSeqNum == primaryInfo.state.VotedSeqNum {
				appliers[id] = true
			}
		}

		if quorum.IsEstablished(appliers) {
			confirmed := ConsensusState{
				VotingTerm:   primaryInfo.state.PrimaryTerm,
				CoordID:      primaryInfo.state.VotedCoord,
				SeqNum:       primaryInfo.state.VotedSeqNum,
				Phase:        PhaseEstablish,
				PrimaryTerm:  primaryInfo.state.PrimaryTerm,
				Primary:      primaryID,
				SyncReplicas: primaryInfo.state.SyncReplicas,
			}
			n.confirmedState = &confirmed
			n.confirmedQuorum = quorum
			n.appointed = true
			return true
		}
	}
	return false
}

// electionTimeoutTicks is how long an orch waits for phase quorum before escalating the term.
const electionTimeoutTicks = 10
