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
)

// syncReplicaQuorum is the number of sync replicas that must acknowledge a write for it
// to be committed. This corresponds to PostgreSQL's "ANY k" in synchronous_standby_names.
// Revocation blocking sets are derived from this value.
//
// TODO: make this a configurable field on OrchNode to support different topologies.
const syncReplicaQuorum = 1

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
	id           NodeID
	knownPoolers map[NodeID]poolerKnowledge

	term   int64
	seqNum int64

	phase    orchPhase
	progress *phaseProgress

	// appointed is true once an Establish phase has achieved quorum.
	// It is cleared when the confirmed primary reports PostgresStopped so a new
	// election begins. confirmedState is preserved for revocation set computation.
	appointed bool

	// confirmedState is the last ConsensusState that achieved Establish quorum.
	// nil until the first successful appointment. Preserved across re-elections so
	// that revocationSets can be computed for the new round.
	confirmedState *ConsensusState

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

// NewOrchNode creates a new coordinator node with the given identity.
// rng is used to jitter backoff durations when the orch discovers a competing
// coordinator; pass a seeded *rand.Rand so tests remain fully deterministic.
func NewOrchNode(id NodeID, rng *rand.Rand) *OrchNode {
	return &OrchNode{
		id:           id,
		knownPoolers: make(map[NodeID]poolerKnowledge),
		rng:          rng,
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
	// We preserve confirmedState for revocation set computation in the next round.
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
	primary := n.selectPrimary()
	if primary == "" {
		// No available candidate yet; remain in current phase until one appears or timeout.
		return nil
	}

	// SyncReplicas: all available (non-stopped) non-primary poolers.
	var syncReplicas []NodeID
	for id, info := range n.knownPoolers {
		if id != primary && info.postgresStatus != PostgresStopped {
			syncReplicas = append(syncReplicas, id)
		}
	}
	slices.Sort(syncReplicas)

	proposal := ConsensusState{
		VotingTerm:   n.term,
		CoordID:      n.id,
		SeqNum:       n.nextSeqNum(),
		Phase:        PhaseEstablish,
		PrimaryTerm:  n.term,
		Primary:      primary,
		SyncReplicas: syncReplicas,
	}

	n.phase = phaseEstablish
	n.progress = &phaseProgress{
		proposal:    proposal,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + electionTimeoutTicks,
	}
	return []Request{BroadcastStateRequest{State: proposal}}
}

// beginQuorumMet returns true when a simple majority of known poolers have voted
// for this orch as coordinator at the current term.
func (n *OrchNode) beginQuorumMet() bool {
	count := 0
	for id := range n.progress.confirmers {
		if _, exists := n.knownPoolers[id]; exists {
			count++
		}
	}
	return count > len(n.knownPoolers)/2
}

// revokeQuorumMet returns true when at least one complete revocation set has all
// members reporting applied=true for the current revoke proposal.
func (n *OrchNode) revokeQuorumMet() bool {
	if n.confirmedState == nil {
		return true
	}
	for _, set := range revocationSets(*n.confirmedState) {
		allApplied := true
		for _, id := range set {
			if !n.progress.appliers[id] {
				allApplied = false
				break
			}
		}
		if allApplied {
			return true
		}
	}
	return false
}

// establishQuorumMet returns true when the new primary and at least syncReplicaQuorum
// sync replicas have all applied the Establish proposal.
func (n *OrchNode) establishQuorumMet() bool {
	primary := n.progress.proposal.Primary
	if !n.progress.appliers[primary] {
		return false
	}
	applied := 0
	for _, id := range n.progress.proposal.SyncReplicas {
		if n.progress.appliers[id] {
			applied++
		}
	}
	return applied >= syncReplicaQuorum
}

// revocationSets returns the minimal sets of poolers whose collective acceptance of
// the Revoke proposal guarantees the old primary can no longer commit writes.
//
// For a topology where writes require primary + ANY k of n sync replicas, a set S is
// a revocation set iff S intersects every possible write quorum. The minimal sets are:
//   - {primary}: present in every write quorum; its withdrawal alone suffices.
//   - {all n sync replicas}: ensures no replica quorum can form (for k=1, need all n).
//
// TODO: generalise for k > 1 (ANY k quorums produce smaller replica blocking sets).
func revocationSets(state ConsensusState) [][]NodeID {
	if state.Primary == "" {
		return nil
	}
	if len(state.SyncReplicas) == 0 {
		// No sync replicas: only the primary forms write quorums.
		return [][]NodeID{{state.Primary}}
	}
	allReplicas := make([]NodeID, len(state.SyncReplicas))
	copy(allReplicas, state.SyncReplicas)
	return [][]NodeID{
		{state.Primary},
		allReplicas,
	}
}

// selectPrimary returns the lowest-ID pooler that is not PostgresStopped.
// Unknown-status poolers are included (treated as potentially running).
func (n *OrchNode) selectPrimary() NodeID {
	var primary NodeID
	for id, info := range n.knownPoolers {
		if info.postgresStatus != PostgresStopped {
			if primary == "" || id < primary {
				primary = id
			}
		}
	}
	return primary
}

// hasAvailablePoolers returns true if at least one known pooler is not stopped.
func (n *OrchNode) hasAvailablePoolers() bool {
	for _, info := range n.knownPoolers {
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
// election). If so, it adopts the confirmed state and marks this orch as appointed,
// avoiding an unnecessary competing election.
func (n *OrchNode) learnEstablishedPrimary() bool {
	for primaryID, primaryInfo := range n.knownPoolers {
		if !primaryInfo.applied || primaryInfo.postgresStatus == PostgresStopped {
			continue
		}
		if primaryInfo.state.Role != RolePrimary {
			continue
		}
		// Count applied sync replicas that still point to this primary.
		applied := 0
		for _, replicaID := range primaryInfo.state.SyncReplicas {
			ri, exists := n.knownPoolers[replicaID]
			if exists && ri.applied && ri.state.Primary == primaryID {
				applied++
			}
		}
		if applied >= syncReplicaQuorum {
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
			n.appointed = true
			return true
		}
	}
	return false
}

// electionTimeoutTicks is how long an orch waits for phase quorum before escalating the term.
const electionTimeoutTicks = 10
