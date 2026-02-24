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

package leader_election

import (
	"fmt"
	"math/rand/v2"
)

// NodeID uniquely identifies a node in the election protocol
type NodeID string

// Simple leader election protocol for validating DST framework
// Rules:
// 1. Leader sends heartbeats every heartbeatInterval ticks
// 2. Followers reset election timeout on heartbeat
// 3. If election timeout expires, become candidate and request votes
// 4. Majority votes → become leader
// 5. Higher term always wins

// Role represents the node's current role
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Indicator is an incoming observation
type Indicator interface {
	isIndicator()
}

// TickIndicator signals time passing
type TickIndicator struct {
	CurrentTick int64
}

func (TickIndicator) isIndicator() {}

// VoteRequestIndicator is a vote request from a candidate
type VoteRequestIndicator struct {
	From NodeID
	Term int64
}

func (VoteRequestIndicator) isIndicator() {}

// VoteResponseIndicator is a vote response
type VoteResponseIndicator struct {
	From    NodeID
	Term    int64
	Granted bool
}

func (VoteResponseIndicator) isIndicator() {}

// HeartbeatIndicator is a heartbeat from the leader
type HeartbeatIndicator struct {
	From NodeID
	Term int64
}

func (HeartbeatIndicator) isIndicator() {}

// StepDownRequestIndicator is a request from an enforcer for a node to step down
type StepDownRequestIndicator struct {
	From          NodeID
	Reason        string
	SuggestedTerm int64 // Term the requester has observed
}

func (StepDownRequestIndicator) isIndicator() {}

// Request is an outgoing desire for I/O
type Request interface {
	isRequest()
}

// VoteRequest requests a vote
type VoteRequest struct {
	To   NodeID
	Term int64
}

func (VoteRequest) isRequest() {}

// VoteResponse responds to a vote request
type VoteResponse struct {
	To      NodeID
	Term    int64
	Granted bool
}

func (VoteResponse) isRequest() {}

// HeartbeatRequest sends a heartbeat
type HeartbeatRequest struct {
	To   NodeID
	Term int64
}

func (HeartbeatRequest) isRequest() {}

// LogRequest emits a log message
type LogRequest struct {
	Level   string
	Message string
}

func (LogRequest) isRequest() {}

// StepDownRequest requests a node to step down
type StepDownRequest struct {
	To            NodeID
	Reason        string
	SuggestedTerm int64 // Term the requester has observed; node should step down to at least this + 1
}

func (StepDownRequest) isRequest() {}

// ElectionNode implements a simple leader election protocol
type ElectionNode struct {
	id    NodeID
	peers []NodeID

	// State
	currentTick  int64
	role         Role
	term         int64
	votedFor     NodeID
	votesGranted map[NodeID]bool

	// Timing (in ticks)
	electionTimeoutTicks   int64
	heartbeatIntervalTicks int64
	electionDeadlineTick   int64
	nextHeartbeatTick      int64

	// Configuration
	rng             *rand.Rand
	buggyVotes      bool // Intentional bug: sometimes drop votes
	buggyQuorum     bool // Intentional bug: sometimes become leader without quorum
	buggyHeartbeats bool // Intentional bug: sometimes drop heartbeats
}

// NodeConfig configures an election node
type NodeConfig struct {
	ID                     NodeID
	Peers                  []NodeID
	ElectionTimeoutTicks   int64
	HeartbeatIntervalTicks int64
	Seed                   int64
	BuggyVotes             bool // Drop 10% of vote responses
	BuggyQuorum            bool // Sometimes become leader without quorum
	BuggyHeartbeats        bool // Drop 50% of heartbeats
}

// NewElectionNode creates a new election node
func NewElectionNode(cfg NodeConfig) *ElectionNode {
	rng := rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(cfg.Seed)))
	jitter := rng.Int64N(cfg.ElectionTimeoutTicks / 2)

	return &ElectionNode{
		id:                     cfg.ID,
		peers:                  cfg.Peers,
		currentTick:            0,
		role:                   Follower,
		term:                   0,
		votedFor:               "",
		votesGranted:           make(map[NodeID]bool),
		electionTimeoutTicks:   cfg.ElectionTimeoutTicks,
		heartbeatIntervalTicks: cfg.HeartbeatIntervalTicks,
		electionDeadlineTick:   cfg.ElectionTimeoutTicks + jitter,
		nextHeartbeatTick:      0,
		rng:                    rng,
		buggyVotes:             cfg.BuggyVotes,
		buggyQuorum:            cfg.BuggyQuorum,
		buggyHeartbeats:        cfg.BuggyHeartbeats,
	}
}

// ID returns the node's unique identifier
func (n *ElectionNode) ID() NodeID {
	return n.id
}

// GetDebugState returns the node's internal state for debugging
func (n *ElectionNode) GetDebugState() any {
	return map[string]any{
		"role":        n.role.String(),
		"term":        n.term,
		"votedFor":    n.votedFor,
		"currentTick": n.currentTick,
	}
}

// Step processes an indicator and returns requests
func (n *ElectionNode) Step(ind Indicator) []Request {
	switch i := ind.(type) {
	case TickIndicator:
		n.currentTick = i.CurrentTick
		return n.handleTick()

	case VoteRequestIndicator:
		return n.handleVoteRequest(i)

	case VoteResponseIndicator:
		return n.handleVoteResponse(i)

	case HeartbeatIndicator:
		return n.handleHeartbeat(i)

	case StepDownRequestIndicator:
		return n.handleStepDownRequest(i)

	default:
		return nil
	}
}

// GetState returns the current state (for testing/inspection)
func (n *ElectionNode) GetState() (Role, int64) {
	return n.role, n.term
}

func (n *ElectionNode) handleTick() []Request {
	switch n.role {
	case Follower:
		// Check if election timeout expired
		if n.currentTick >= n.electionDeadlineTick {
			return n.startElection()
		}

	case Candidate:
		// Check if election timeout expired (retry election)
		if n.currentTick >= n.electionDeadlineTick {
			return n.startElection()
		}

	case Leader:
		// Send heartbeats
		if n.currentTick >= n.nextHeartbeatTick {
			return n.sendHeartbeats()
		}
	}

	return nil
}

func (n *ElectionNode) startElection() []Request {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.votesGranted = map[NodeID]bool{n.id: true}

	// Reset election deadline with jitter
	jitter := n.rng.Int64N(n.electionTimeoutTicks / 2)
	n.electionDeadlineTick = n.currentTick + n.electionTimeoutTicks + jitter

	var requests []Request
	requests = append(requests, LogRequest{
		Level:   "info",
		Message: fmt.Sprintf("[%s] Starting election for term %d at tick %d", n.id, n.term, n.currentTick),
	})

	// Request votes from all peers
	for _, peer := range n.peers {
		requests = append(requests, VoteRequest{
			To:   peer,
			Term: n.term,
		})
	}

	return requests
}

func (n *ElectionNode) handleVoteRequest(ind VoteRequestIndicator) []Request {
	// If term is higher, step down
	if ind.Term > n.term {
		n.stepDown(ind.Term)
	}

	granted := false
	if ind.Term == n.term && (n.votedFor == "" || n.votedFor == ind.From) {
		granted = true
		n.votedFor = ind.From
		// Reset election deadline (we're supporting this candidate)
		jitter := n.rng.Int64N(n.electionTimeoutTicks / 2)
		n.electionDeadlineTick = n.currentTick + n.electionTimeoutTicks + jitter
	}

	// BUG: Intentionally drop some vote responses (deterministically)
	if n.buggyVotes && n.rng.Float64() < 0.1 {
		return []Request{
			LogRequest{
				Level:   "debug",
				Message: fmt.Sprintf("[%s] BUG: Dropping vote response to %s", n.id, ind.From),
			},
		}
	}

	return []Request{
		VoteResponse{
			To:      ind.From,
			Term:    n.term,
			Granted: granted,
		},
		LogRequest{
			Level:   "debug",
			Message: fmt.Sprintf("[%s] Vote response to %s: granted=%v (term %d)", n.id, ind.From, granted, n.term),
		},
	}
}

func (n *ElectionNode) handleVoteResponse(ind VoteResponseIndicator) []Request {
	// Ignore if we're not a candidate
	if n.role != Candidate {
		return nil
	}

	// If term is higher, step down
	if ind.Term > n.term {
		n.stepDown(ind.Term)
		return nil
	}

	// Ignore stale responses
	if ind.Term < n.term {
		return nil
	}

	if ind.Granted {
		n.votesGranted[ind.From] = true

		// BUG: Sometimes become leader without quorum (99% chance for testing)
		// In real code this would be 5%, but we make it high to ensure the test catches it
		if n.buggyQuorum && n.rng.Float64() < 0.99 {
			return n.becomeLeader()
		}

		// Check if we have majority
		majority := (len(n.peers)+1)/2 + 1
		if len(n.votesGranted) >= majority {
			return n.becomeLeader()
		}
	}

	return nil
}

func (n *ElectionNode) handleHeartbeat(ind HeartbeatIndicator) []Request {
	// If term is higher or equal, step down and reset election timeout
	if ind.Term >= n.term {
		if n.role != Follower {
			n.stepDown(ind.Term)
		}
		n.term = ind.Term

		// Reset election deadline
		jitter := n.rng.Int64N(n.electionTimeoutTicks / 2)
		n.electionDeadlineTick = n.currentTick + n.electionTimeoutTicks + jitter

		return []Request{
			LogRequest{
				Level:   "debug",
				Message: fmt.Sprintf("[%s] Received heartbeat from %s (term %d)", n.id, ind.From, ind.Term),
			},
		}
	}

	return nil
}

func (n *ElectionNode) becomeLeader() []Request {
	n.role = Leader
	n.nextHeartbeatTick = n.currentTick + n.heartbeatIntervalTicks

	return []Request{
		LogRequest{
			Level:   "info",
			Message: fmt.Sprintf("[%s] Became leader for term %d at tick %d", n.id, n.term, n.currentTick),
		},
	}
}

func (n *ElectionNode) sendHeartbeats() []Request {
	n.nextHeartbeatTick = n.currentTick + n.heartbeatIntervalTicks

	// BUG: Sometimes drop heartbeats (50% chance) to trigger re-elections
	if n.buggyHeartbeats && n.rng.Float64() < 0.50 {
		return []Request{
			LogRequest{
				Level:   "debug",
				Message: fmt.Sprintf("[%s] BUG: Dropping heartbeats at tick %d", n.id, n.currentTick),
			},
		}
	}

	var requests []Request
	for _, peer := range n.peers {
		requests = append(requests, HeartbeatRequest{
			To:   peer,
			Term: n.term,
		})
	}

	requests = append(requests, LogRequest{
		Level:   "debug",
		Message: fmt.Sprintf("[%s] Sending heartbeats (term %d, tick %d)", n.id, n.term, n.currentTick),
	})

	return requests
}

func (n *ElectionNode) stepDown(newTerm int64) {
	n.role = Follower
	n.term = newTerm
	n.votedFor = ""
	n.votesGranted = make(map[NodeID]bool)
}

func (n *ElectionNode) handleStepDownRequest(ind StepDownRequestIndicator) []Request {
	// Ignore stale requests (requester's view is behind our current state)
	if ind.SuggestedTerm < n.term {
		return nil
	}

	// If we're a leader, step down
	if n.role == Leader {
		// Step down to at least the suggested term + 1
		// This gives other nodes a chance to become leader
		newTerm := ind.SuggestedTerm + 1
		if newTerm <= n.term {
			newTerm = n.term + 1
		}
		n.stepDown(newTerm)
		return []Request{
			LogRequest{
				Level:   "info",
				Message: fmt.Sprintf("[%s] Stepping down as leader (term %d -> %d) due to request from %s: %s", n.id, n.term, newTerm, ind.From, ind.Reason),
			},
		}
	}
	return nil
}

// TermLimitEnforcerNode monitors leader tenure and requests step-downs after a timeout
type TermLimitEnforcerNode struct {
	id    NodeID
	peers []NodeID

	// State - completely different from ElectionNode!
	currentTick          int64
	currentLeader        NodeID
	leaderSince          int64
	observedTerm         int64            // Last term observed from heartbeats
	effectiveTenureLimit int64            // Tenure limit with jitter applied when leader was detected
	leaderTenureLimit    int64            // Base tenure limit
	stepDownRequestsSent map[NodeID]int64 // track when we last sent a request

	// Configuration
	rng *rand.Rand
}

// TermLimitEnforcerConfig configures a term limit enforcer node
type TermLimitEnforcerConfig struct {
	ID                NodeID
	Peers             []NodeID
	LeaderTenureLimit int64 // Max ticks a leader should serve
	Seed              int64
}

// NewTermLimitEnforcerNode creates a new term limit enforcer node
func NewTermLimitEnforcerNode(cfg TermLimitEnforcerConfig) *TermLimitEnforcerNode {
	rng := rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(cfg.Seed)))

	return &TermLimitEnforcerNode{
		id:                   cfg.ID,
		peers:                cfg.Peers,
		currentTick:          0,
		currentLeader:        "",
		leaderSince:          0,
		observedTerm:         0,
		effectiveTenureLimit: 0, // Will be set when leader is detected
		leaderTenureLimit:    cfg.LeaderTenureLimit,
		stepDownRequestsSent: make(map[NodeID]int64),
		rng:                  rng,
	}
}

// ID returns the enforcer's unique identifier
func (o *TermLimitEnforcerNode) ID() NodeID {
	return o.id
}

// GetDebugState returns the enforcer's internal state for debugging
func (o *TermLimitEnforcerNode) GetDebugState() any {
	return map[string]any{
		"currentLeader":        o.currentLeader,
		"leaderSince":          o.leaderSince,
		"observedTerm":         o.observedTerm,
		"effectiveTenureLimit": o.effectiveTenureLimit,
		"currentTick":          o.currentTick,
	}
}

// Step processes an indicator and returns requests
func (o *TermLimitEnforcerNode) Step(ind Indicator) []Request {
	switch i := ind.(type) {
	case TickIndicator:
		o.currentTick = i.CurrentTick
		return o.checkLeaderTenure()

	case HeartbeatIndicator:
		return o.observeHeartbeat(i)

	default:
		// Observer doesn't care about votes or step-down requests
		return nil
	}
}

func (o *TermLimitEnforcerNode) observeHeartbeat(ind HeartbeatIndicator) []Request {
	// Track the term from heartbeats
	o.observedTerm = ind.Term

	// Track current leader
	if ind.From != o.currentLeader {
		// New leader detected
		o.currentLeader = ind.From
		o.leaderSince = o.currentTick

		// Apply jitter once when leader is detected
		jitter := o.rng.Int64N(5) // 0-4 ticks of jitter
		o.effectiveTenureLimit = o.leaderTenureLimit + jitter
	}

	return nil
}

func (o *TermLimitEnforcerNode) checkLeaderTenure() []Request {
	// No current leader
	if o.currentLeader == "" {
		return nil
	}

	// Calculate tenure
	tenure := o.currentTick - o.leaderSince

	// Check if leader has served too long
	if tenure >= o.effectiveTenureLimit {
		// Check if we've already sent a request recently (avoid spam)
		lastSent, exists := o.stepDownRequestsSent[o.currentLeader]
		if exists && o.currentTick-lastSent < 10 {
			// Sent a request within last 10 ticks, don't spam
			return nil
		}

		// Send step-down request with the observed term
		o.stepDownRequestsSent[o.currentLeader] = o.currentTick

		return []Request{
			StepDownRequest{
				To:            o.currentLeader,
				Reason:        fmt.Sprintf("leader tenure exceeded %d ticks (current: %d)", o.effectiveTenureLimit, tenure),
				SuggestedTerm: o.observedTerm,
			},
		}
	}

	return nil
}
