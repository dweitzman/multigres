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

// orchNode is the coordinator state machine. It discovers poolers via etcd indicators,
// proposes ConsensusState values, and waits for quorum confirmation from poolers.
//
// All state is ephemeral: if an orch crashes and restarts it simply begins a new round
// at a higher voting term, learning cluster state from pooler status indicators before
// making any proposal.
type OrchNode struct {
	id           NodeID
	knownPoolers map[NodeID]poolerInfo // keyed by pooler NodeID

	// Current coordination term and sequence counter
	term   int64
	seqNum int64

	// Non-nil while waiting for pooler confirmations on the active proposal.
	// nil means no proposal is in flight; the orch will try to appoint on the next tick.
	pending *pendingAppointment

	// appointed is true once a primary has been confirmed by quorum.
	// The orch does not re-appoint while appointed is true.
	appointed bool
}

type poolerInfo struct {
	statusSeq int64
	state     PoolerPersistentState
}

type pendingAppointment struct {
	state         ConsensusState
	confirmations map[NodeID]bool
	timeoutTick   int64
}

// NewOrchNode creates a new coordinator node with the given identity.
func NewOrchNode(id NodeID) *OrchNode {
	return &OrchNode{
		id:           id,
		knownPoolers: make(map[NodeID]poolerInfo),
	}
}

// ID returns the orch node's unique identifier.
func (n *OrchNode) ID() NodeID {
	return n.id
}

// Step processes all indicators that arrived this tick and returns requests.
// This is the sole entry point for all state changes; it has no side effects.
func (n *OrchNode) Step(tick int64, indicators []Indicator) []Request {
	var requests []Request

	for _, ind := range indicators {
		switch v := ind.(type) {
		case PoolerDiscoveredIndicator:
			n.handlePoolerDiscovered(v)
		case PoolerRemovedIndicator:
			n.handlePoolerRemoved(v)
		case PoolerStatusIndicator:
			n.handlePoolerStatus(v)
		case PoolerResponseIndicator:
			requests = append(requests, n.handlePoolerResponse(v)...)
		}
	}

	// Time-based: check for appointment timeout
	if n.pending != nil && tick >= n.pending.timeoutTick {
		n.term++
		n.pending = nil
	}

	// If we have no pending proposal, no confirmed primary, and we know poolers, try to appoint
	if n.pending == nil && !n.appointed && len(n.knownPoolers) > 0 {
		requests = append(requests, n.startAppointment(tick)...)
	}

	return requests
}

func (n *OrchNode) handlePoolerDiscovered(ind PoolerDiscoveredIndicator) {
	if _, exists := n.knownPoolers[ind.PoolerID]; !exists {
		n.knownPoolers[ind.PoolerID] = poolerInfo{}
	}
}

func (n *OrchNode) handlePoolerRemoved(ind PoolerRemovedIndicator) {
	delete(n.knownPoolers, ind.PoolerID)
	// If the removed pooler was in our pending confirmation set, quorum may be affected
	// (re-evaluated on next response or timeout)
}

func (n *OrchNode) handlePoolerStatus(ind PoolerStatusIndicator) {
	info, exists := n.knownPoolers[ind.PoolerID]
	if !exists {
		return // unknown pooler; ignore until discovered via etcd
	}
	if ind.StatusSeq <= info.statusSeq {
		return // stale update; discard
	}
	n.knownPoolers[ind.PoolerID] = poolerInfo{
		statusSeq: ind.StatusSeq,
		state:     ind.State,
	}

	// If the pooler is ahead of our term, adopt it so our next proposal escalates
	if ind.State.VotedTerm > n.term {
		n.term = ind.State.VotedTerm
		n.pending = nil
	}
}

func (n *OrchNode) handlePoolerResponse(ind PoolerResponseIndicator) []Request {
	if n.pending == nil {
		return nil // no active proposal; ignore
	}
	if ind.Accepted {
		n.pending.confirmations[ind.FromPooler] = true
		if n.hasQuorum() {
			n.appointed = true
			n.pending = nil
		}
		return nil
	}

	// Rejection: adopt the higher term seen by the pooler and restart
	if ind.KnownTerm > n.term {
		n.term = ind.KnownTerm
	} else if ind.KnownTerm == n.term && ind.KnownCoordID != n.id && ind.KnownCoordID != "" {
		// Another coordinator won our term; escalate
		n.term++
	}
	n.pending = nil
	return nil
}

// startAppointment proposes a new ConsensusState to all known poolers.
// It selects the pooler with the lowest NodeID as primary for determinism in tests.
func (n *OrchNode) startAppointment(tick int64) []Request {
	n.term++
	n.seqNum = 0

	// Pick primary: pooler with the lowest NodeID for determinism
	var primary NodeID
	for id := range n.knownPoolers {
		if primary == "" || id < primary {
			primary = id
		}
	}

	state := ConsensusState{
		VotingTerm:  n.term,
		CoordID:     n.id,
		SeqNum:      n.seqNum,
		PrimaryTerm: n.term,
		Primary:     primary,
	}

	n.pending = &pendingAppointment{
		state:         state,
		confirmations: make(map[NodeID]bool),
		timeoutTick:   tick + appointmentTimeoutTicks,
	}

	return []Request{BroadcastStateRequest{State: state}}
}

func (n *OrchNode) hasQuorum() bool {
	return len(n.pending.confirmations) > len(n.knownPoolers)/2
}

// appointmentTimeoutTicks is how long an orch waits for quorum before escalating the term.
const appointmentTimeoutTicks = 10
