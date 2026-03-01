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

// orchPhase is the current stage of the three-phase coordinator state machine.
// The orch wins the coordinator role via the Begin vote, then appoints a primary
// through Revoke and Establish. "Election" refers specifically to the Begin vote;
// the broader process is called the orch's appointment or coordinator tenure.
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

// OrchNode is the coordinator state machine responsible for safely driving appointment
// and re-appointment of write quorums. It discovers poolers via etcd indicators, runs
// the three-phase protocol (Begin → Revoke → Establish), and confirms the appointment
// once the new primary and enough sync replicas have applied the Establish proposal.
//
// OrchNode concerns itself with executing the appointment protocol correctly; deciding
// whether re-appointment is needed (cluster-health evaluation) is a separate concern
// that should be kept as distinct as practical.
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
	// appointment begins. confirmedState and confirmedQuorum are preserved for
	// revocation computation in the next round.
	appointed bool

	// confirmedState is the last ConsensusState that achieved Establish quorum.
	// nil until the first successful appointment. Preserved across re-appointments so
	// that the revocation quorum can be computed for the new round.
	confirmedState *ConsensusState

	// confirmedQuorum is the Quorum for confirmedState. It knows how to evaluate
	// revocation: whether the old primary can still collect write acks.
	confirmedQuorum Quorum

	// backoffUntil is the tick before which this orch will not start a new appointment.
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
// a fresh appointment can proceed.
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

// Restart is called by the simulator when simulating a crash-restart. It clears all
// ephemeral orch state so the node starts fresh. Unlike PoolerNode, orch has no durable
// state: it re-learns cluster topology from pooler status indicators and discovery events
// before making any new proposals.
func (n *OrchNode) Restart() {
	n.term = 0
	n.seqNum = 0
	n.phase = phaseIdle
	n.progress = nil
	n.appointed = false
	n.confirmedState = nil
	n.confirmedQuorum = nil
	n.backoffUntil = 0
	n.pendingBackoff = false
	n.knownPoolers = make(map[NodeID]poolerKnowledge)
}

// KnownPoolerIDs returns the IDs of all poolers this orch currently knows about,
// in sorted order. Used by the simulation discovery node to detect gaps in the
// orch's membership view (e.g., after a crash-restart clears knownPoolers) and
// deliver the missing poolers via the normal discovery path.
func (n *OrchNode) KnownPoolerIDs() []NodeID {
	return sortedmaps.Keys(n.knownPoolers)
}

// AppointmentStageComplete reports whether this orch has completed the given phase
// in its current primary appointment — the process (Begin → Revoke → Establish) by
// which an orch wins the coordinator role and appoints a new primary.
//
// Returns false for ALL phases when this orch is not actively running an appointment:
// when idle before winning the Begin vote, still in the Begin phase gathering votes,
// or after Restart() clears all state.
//
//   - PhaseBegin:     orch won the Begin vote (a quorum accepted it as coordinator).
//     True while in Revoke or Establish, or after Establish completes.
//   - PhaseRevoke:    Revoke completed or was skipped (true while in Establish or after).
//   - PhaseEstablish: Establish quorum met — a primary has been appointed.
//
// Example: AppointmentStageComplete(PhaseBegin) && !AppointmentStageComplete(PhaseEstablish)
// identifies an orch that won the coordinator vote but has not yet appointed a primary.
// Useful in simulation conditions that count pre-establish orch crashes.
func (n *OrchNode) AppointmentStageComplete(phase ConsensusPhase) bool {
	switch phase {
	case PhaseBegin:
		return n.phase == phaseRevoke || n.phase == phaseEstablish || n.appointed
	case PhaseRevoke:
		return n.phase == phaseEstablish || n.appointed
	case PhaseEstablish:
		return n.appointed
	default:
		return false
	}
}

// Step processes all indicators and drives the coordinator state machine forward.
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

	// If the confirmed primary has stopped, clear the appointment so we begin
	// a new appointment. We preserve confirmedState and confirmedQuorum for revocation computation.
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
	if ind.State.Committed.CoordTerm > n.term {
		n.term = ind.State.Committed.CoordTerm
		if n.phase != phaseIdle {
			// We were trying to act on out-of-date information. Abandon the current
			// appointment attempt and back off so a competing coordinator can make progress.
			n.pendingBackoff = true
			n.phase = phaseIdle
			n.progress = nil
		}
	}

	// Record applied status for the current in-flight proposal.
	if n.progress != nil && ind.Applied &&
		ind.State.Committed.CoordTerm == n.progress.proposal.CoordTerm &&
		ind.State.Committed.SeqNum == n.progress.proposal.SeqNum {
		n.progress.appliers[ind.PoolerID] = true
	}
}

func (n *OrchNode) handlePoolerResponse(ind PoolerResponseIndicator) {
	if n.progress == nil {
		return
	}
	// Discard responses that do not match the current proposal: they are late
	// deliveries from a previous round (different term or seq num).
	if ind.CoordTerm != n.progress.proposal.CoordTerm ||
		ind.SeqNum != n.progress.proposal.SeqNum {
		return
	}
	if ind.Accepted {
		n.progress.confirmers[ind.FromPooler] = true
		return
	}
	// Rejection: adopt the higher term or escalate on same-term coordinator conflict.
	// In either case we learned that another coordinator is actively working, so we
	// signal a backoff: our view was stale when we started this appointment attempt.
	if ind.KnownTerm > n.term {
		n.term = ind.KnownTerm
		n.pendingBackoff = true
	} else if ind.KnownTerm == n.term && ind.KnownCoordID != n.id && ind.KnownCoordID != "" {
		n.term++
		n.pendingBackoff = true
	}
	n.phase = phaseIdle
	n.progress = nil

	// Process any piggybacked fresh status now that we are back in idle, so
	// knownPoolers is up to date for the next advance() cycle.
	if ind.FreshStatus != nil {
		n.handlePoolerStatus(*ind.FreshStatus)
	}

	// TODO(retry): When the rejection was due to a stale view (StaleTerm,
	// PrimaryTermMismatch, CohortMembership) and the piggybacked fresh status shows
	// no competing coordinator is active at the higher term, we could skip the backoff
	// and retry immediately with our updated knowledge. Currently we always backoff,
	// which is safe but adds latency. Implementing this requires distinguishing
	// "stale view, no active competitor" from "lost a race with another orch."
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
			// Before starting a new appointment, check whether another coordinator has
			// already established a primary. If so, adopt that state so we don't
			// compete unnecessarily.
			n.learnEstablishedQuorum()
			if n.appointed {
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
		ProposalID: ProposalID{
			CoordTerm: n.term,
			CoordID:   n.id,
			SeqNum:    n.nextSeqNum(),
		},
		Phase:         PhaseBegin,
		CohortMembers: n.cohortMembersFromStatus(),
	}
	if n.confirmedState != nil {
		proposal.PrimaryTerm = n.confirmedState.PrimaryTerm
		proposal.Primary = n.confirmedState.Primary
	}

	n.phase = phaseBegin
	n.progress = &phaseProgress{
		proposal:    proposal,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + appointmentPhaseTimeoutTicks,
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
		ProposalID: ProposalID{
			CoordTerm: n.term,
			CoordID:   n.id,
			SeqNum:    n.nextSeqNum(),
		},
		Phase:         PhaseRevoke,
		CohortMembers: n.cohortMembersFromStatus(),
		// PrimaryTerm=0 and Primary="" signal no primary is currently appointed.
	}

	n.phase = phaseRevoke
	n.progress = &phaseProgress{
		proposal:    proposal,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + appointmentPhaseTimeoutTicks,
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

	spec, err := quorum.Serialize()
	if err != nil {
		// Serialization failure is a programming error; treat it as no quorum available.
		return nil
	}

	proposal := ConsensusState{
		ProposalID: ProposalID{
			CoordTerm: n.term,
			CoordID:   n.id,
			SeqNum:    n.nextSeqNum(),
		},
		Phase:         PhaseEstablish,
		PrimaryTerm:   n.term,
		Primary:       quorum.Primary(),
		QuorumSpec:    spec,
		CohortMembers: sortedmaps.Keys(n.knownPoolers),
	}

	n.phase = phaseEstablish
	n.progress = &phaseProgress{
		proposal:    proposal,
		quorum:      quorum,
		confirmers:  make(map[NodeID]bool),
		appliers:    make(map[NodeID]bool),
		timeoutTick: tick + appointmentPhaseTimeoutTicks,
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
// On bootstrap (confirmedQuorum == nil), the bootstrapPolicy is asked whether the
// confirmer set satisfies the bootstrap threshold over all known poolers. Once
// learnEstablishedQuorum() sets confirmedQuorum (because the orch contacted poolers
// already in an established cohort), this bootstrap path is never taken again.
func (n *OrchNode) beginQuorumMet() bool {
	if n.confirmedQuorum != nil {
		return n.confirmedQuorum.IsRevoked(n.progress.confirmers)
	}
	return n.bootstrapPolicy.CheckBootstrapQuorum(sortedmaps.Keys(n.knownPoolers), n.progress.confirmers)
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

// jitterTicks returns a backoff duration in [appointmentPhaseTimeoutTicks, 2*appointmentPhaseTimeoutTicks).
// The jitter breaks symmetry between orchs that discover a conflict at the same tick.
func (n *OrchNode) jitterTicks() int64 {
	return appointmentPhaseTimeoutTicks + n.rng.Int64N(appointmentPhaseTimeoutTicks)
}

// proposalKey uniquely identifies an Establish proposal across the cluster.
// For Establish proposals CoordTerm == PrimaryTerm, so CoordTerm alone is sufficient
// to identify the term at which a primary was appointed.
type proposalKey struct {
	coordTerm int64
	seqNum    int64
	coordID   NodeID
}

// learnEstablishedQuorum checks whether the pooler status reports already show
// that an Establish quorum has been satisfied (another coordinator completed an
// appointment). It updates confirmedState and confirmedQuorum when an established
// quorum is found, and sets n.appointed = true when the primary is currently running.
//
// Unlike the naive approach of only examining poolers with Role=Primary, this
// groups ALL poolers that have a persisted QuorumSpec by their proposal identity.
// A pooler that applied (Applied=true) and then stopped is still evidence that the
// quorum was established; excluding stopped primaries would prevent learning the
// quorum after a crash, which is precisely when learning it matters most.
//
// If the primary is stopped, confirmedState and confirmedQuorum are still recorded
// (for accurate revocation computation in the upcoming Begin) but n.appointed is
// left false so advance() proceeds to startBegin().
//
// This is a best-effort reconciliation of potentially stale, inconsistent status
// signals: it resolves what the quorum demonstrably agreed on, but the result may
// be out of date by the time Begin is sent. That's fine — Begin rejections from
// poolers with higher terms will surface any gaps and force a retry with fresher data.
func (n *OrchNode) learnEstablishedQuorum() {
	// Collect candidates: any pooler that persisted a QuorumSpec from an Establish
	// proposal. Group by the proposal's unique identity.
	//
	// NOTE(correctness gap): we group by the pooler's current (VotedTerm, VotedSeqNum),
	// not by the primary term at which the Establish actually ran. After a Begin-only cycle
	// (orch increments VotingTerm without completing a new Establish), poolers advance
	// VotedTerm but QuorumSpec and Applied=true remain from the old Establish. The result:
	// we build a candidate keyed to the Begin proposal, find no appliers (Applied=false for
	// Begin since there is no role change), and incorrectly skip a still-intact quorum.
	//
	// The correct fix is to group by PrimaryTerm (the voting term at which the Establish ran,
	// preserved through subsequent Begin/Revoke proposals) and match poolers by Applied=true
	// AND PrimaryTerm == group.PrimaryTerm. Tracked in README Stage 1 TODO.
	type candidate struct {
		quorumSpec []byte
		appliers   map[NodeID]bool
	}
	candidates := make(map[proposalKey]*candidate)
	// keys is built alongside candidates to avoid a separate non-deterministic
	// map range when collecting keys for sorting.
	var keys []proposalKey

	for id, info := range sortedmaps.All(n.knownPoolers) {
		if len(info.state.QuorumSpec) == 0 {
			continue
		}
		key := proposalKey{
			coordTerm: info.state.Committed.CoordTerm,
			seqNum:    info.state.Committed.SeqNum,
			coordID:   info.state.Committed.CoordID,
		}
		c, ok := candidates[key]
		if !ok {
			c = &candidate{
				quorumSpec: info.state.QuorumSpec,
				appliers:   make(map[NodeID]bool),
			}
			candidates[key] = c
			keys = append(keys, key)
		}
		if info.applied {
			c.appliers[id] = true
		}
	}

	if len(keys) == 0 {
		return
	}

	// Sort candidates by CoordTerm descending so we adopt the most recent quorum.
	slices.SortFunc(keys, func(a, b proposalKey) int {
		if a.coordTerm != b.coordTerm {
			if a.coordTerm > b.coordTerm {
				return -1
			}
			return 1
		}
		if a.seqNum != b.seqNum {
			if a.seqNum > b.seqNum {
				return -1
			}
			return 1
		}
		if a.coordID < b.coordID {
			return -1
		}
		if a.coordID > b.coordID {
			return 1
		}
		return 0
	})

	for _, key := range keys {
		c := candidates[key]

		// Deserialise the quorum so IsEstablished uses the historically committed
		// membership, not what the policy would propose today.
		quorum, err := n.policy.DeserializeQuorum(c.quorumSpec)
		if err != nil {
			continue
		}

		// IsEstablished requires both the primary and enough sync replicas to have
		// applied the proposal. We do not infer establishment from replicas alone —
		// without the primary's confirmation we cannot rule out that a higher-term
		// appointment was made and we simply haven't seen it yet.
		if !quorum.IsEstablished(c.appliers) {
			continue
		}

		// Adopt this quorum: record confirmedState and confirmedQuorum so the Begin
		// phase carries the correct PrimaryTerm/Primary and uses the right revocation
		// quorum even if a fresh appointment turns out to be needed.
		confirmed := ConsensusState{
			ProposalID: ProposalID{
				CoordTerm: key.coordTerm,
				CoordID:   key.coordID,
				SeqNum:    key.seqNum,
			},
			Phase:       PhaseEstablish,
			PrimaryTerm: key.coordTerm,
			Primary:     quorum.Primary(),
			QuorumSpec:  c.quorumSpec,
		}
		n.confirmedState = &confirmed
		n.confirmedQuorum = quorum

		// If the primary is currently stopped, consensus is still intact but the
		// cluster needs a new appointment. Record the quorum (above) so the upcoming
		// Begin has accurate revocation data, but leave n.appointed false so
		// advance() proceeds to startBegin().
		if pi, ok := n.knownPoolers[quorum.Primary()]; ok && pi.postgresStatus == PostgresStopped {
			return
		}
		n.appointed = true
		return
	}
}

// cohortMembersFromStatus returns the IDs of all known poolers that have reported
// CohortMember=true in their status. Used to populate CohortMembers on Begin and
// Revoke proposals so that established poolers can verify they belong to the cohort
// the orch is acting on. An empty result signals to established poolers that this
// orch is performing a bootstrap (it has not yet learned the existing cohort).
func (n *OrchNode) cohortMembersFromStatus() []NodeID {
	var members []NodeID
	for id, info := range sortedmaps.All(n.knownPoolers) {
		if info.state.CohortMember {
			members = append(members, id)
		}
	}
	return members
}

// appointmentPhaseTimeoutTicks is how long an orch waits for a phase to achieve quorum
// before escalating the voting term and restarting the appointment from Begin.
const appointmentPhaseTimeoutTicks = 10
