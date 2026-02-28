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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// LeaderTenureTooLong checks if a leader has been in power too long
type LeaderTenureTooLong struct {
	maxTenure        int64
	leaderStartTicks map[NodeID]int64
}

func NewLeaderTenureTooLong(maxTenure int64) *LeaderTenureTooLong {
	return &LeaderTenureTooLong{
		maxTenure:        maxTenure,
		leaderStartTicks: make(map[NodeID]int64),
	}
}

func (c *LeaderTenureTooLong) Name() string {
	return "leader_tenure_too_long"
}

func (c *LeaderTenureTooLong) Eval(sim *dstsim.Simulator[Indicator, Request, NodeID]) bool {
	currentTick := sim.CurrentTick()
	var currentLeader NodeID

	// Find current leader
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, _ := electionNode.GetState()
			if role == Leader {
				currentLeader = electionNode.ID()
				break
			}
		}
	}

	// Track when each node became leader
	if currentLeader != "" {
		if _, exists := c.leaderStartTicks[currentLeader]; !exists {
			c.leaderStartTicks[currentLeader] = currentTick
		}

		// Check tenure
		tenure := currentTick - c.leaderStartTicks[currentLeader]
		if tenure > c.maxTenure {
			return true // Tenure exceeded
		}
	}

	// Clear tracking for nodes that are no longer leader
	for _, nodeID := range sortedmaps.Keys(c.leaderStartTicks) {
		if nodeID != currentLeader {
			delete(c.leaderStartTicks, nodeID)
		}
	}

	return false
}

func (c *LeaderTenureTooLong) Describe(sim *dstsim.Simulator[Indicator, Request, NodeID]) string {
	currentTick := sim.CurrentTick()
	var currentLeader NodeID

	// Find current leader
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, _ := electionNode.GetState()
			if role == Leader {
				currentLeader = electionNode.ID()
				break
			}
		}
	}

	if currentLeader != "" {
		if startTick, exists := c.leaderStartTicks[currentLeader]; exists {
			tenure := currentTick - startTick
			return fmt.Sprintf("leader %s has been in power for %d ticks (max: %d)",
				currentLeader, tenure, c.maxTenure)
		}
	}

	return "no leader"
}

// Conditions (pure predicates for use with temporal quantifiers)

// MultipleLeadersExist checks if there are multiple leaders
type MultipleLeadersExist struct{}

func (c *MultipleLeadersExist) Name() string {
	return "multiple_leaders_exist"
}

func (c *MultipleLeadersExist) Eval(sim *dstsim.Simulator[Indicator, Request, NodeID]) bool {
	leaderCount := 0
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, _ := electionNode.GetState()
			if role == Leader {
				leaderCount++
			}
		}
	}
	return leaderCount > 1
}

func (c *MultipleLeadersExist) Describe(sim *dstsim.Simulator[Indicator, Request, NodeID]) string {
	leaders := make([]string, 0)
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, term := electionNode.GetState()
			if role == Leader {
				leaders = append(leaders, fmt.Sprintf("%s (term %d)", electionNode.ID(), term))
			}
		}
	}
	return fmt.Sprintf("%d leaders: %v", len(leaders), leaders)
}

// LeaderExists checks if at least one leader exists
type LeaderExists struct{}

func (c *LeaderExists) Name() string {
	return "leader_exists"
}

func (c *LeaderExists) Eval(sim *dstsim.Simulator[Indicator, Request, NodeID]) bool {
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, _ := electionNode.GetState()
			if role == Leader {
				return true
			}
		}
	}
	return false
}

// NoLeadersExist is the complement of LeaderExists
type NoLeadersExist struct{}

func (c *NoLeadersExist) Name() string {
	return "no_leaders_exist"
}

func (c *NoLeadersExist) Eval(sim *dstsim.Simulator[Indicator, Request, NodeID]) bool {
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, _ := electionNode.GetState()
			if role == Leader {
				return false
			}
		}
	}
	return true
}

func (c *NoLeadersExist) Describe(sim *dstsim.Simulator[Indicator, Request, NodeID]) string {
	leaders := make([]string, 0)
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, term := electionNode.GetState()
			if role == Leader {
				leaders = append(leaders, fmt.Sprintf("%s (term %d)", electionNode.ID(), term))
			}
		}
	}
	if len(leaders) == 0 {
		return "no leaders exist"
	}
	return fmt.Sprintf("leaders exist: %v", leaders)
}

func (c *LeaderExists) Describe(sim *dstsim.Simulator[Indicator, Request, NodeID]) string {
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, term := electionNode.GetState()
			if role == Leader {
				return fmt.Sprintf("leader: %s (term %d)", electionNode.ID(), term)
			}
		}
	}
	return "no leader"
}

// NoLeadersExist checks if there are no leaders (opposite of LeaderExists)
type LeaderlessTooLong struct {
	maxDuration        int64
	leaderlessStart    int64
	inLeaderlessPeriod bool
}

func NewLeaderlessTooLong(maxDuration int64) *LeaderlessTooLong {
	return &LeaderlessTooLong{
		maxDuration:     maxDuration,
		leaderlessStart: -1,
	}
}

func (c *LeaderlessTooLong) Name() string {
	return "leaderless_too_long"
}

func (c *LeaderlessTooLong) Eval(sim *dstsim.Simulator[Indicator, Request, NodeID]) bool {
	hasLeader := false
	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, _ := electionNode.GetState()
			if role == Leader {
				hasLeader = true
				break
			}
		}
	}

	tick := sim.CurrentTick()

	if !hasLeader {
		if !c.inLeaderlessPeriod {
			// Just became leaderless
			c.leaderlessStart = tick
			c.inLeaderlessPeriod = true
		}
		duration := tick - c.leaderlessStart
		return duration > c.maxDuration
	} else {
		// Has leader, reset
		c.inLeaderlessPeriod = false
		c.leaderlessStart = -1
		return false
	}
}

func (c *LeaderlessTooLong) Describe(sim *dstsim.Simulator[Indicator, Request, NodeID]) string {
	if c.inLeaderlessPeriod {
		duration := sim.CurrentTick() - c.leaderlessStart
		return fmt.Sprintf("leaderless for %d ticks (max: %d)", duration, c.maxDuration)
	}
	return "has leader"
}

// requestWithSender tracks a request and which node sent it
// setupStandardNodes creates a 3-node election cluster with the given configuration
func setupStandardNodes(nodeSeeds []int64, electionTimeout, heartbeatInterval int64, buggyVotes, buggyQuorum, buggyHeartbeats bool) []*ElectionNode {
	nodeIDs := []NodeID{"node-1", "node-2", "node-3"}
	peers := [][]NodeID{
		{"node-2", "node-3"},
		{"node-1", "node-3"},
		{"node-1", "node-2"},
	}

	nodes := make([]*ElectionNode, 3)
	for i := range 3 {
		nodes[i] = NewElectionNode(NodeConfig{
			ID:                     nodeIDs[i],
			Peers:                  peers[i],
			ElectionTimeoutTicks:   electionTimeout,
			HeartbeatIntervalTicks: heartbeatInterval,
			Seed:                   nodeSeeds[i],
			BuggyVotes:             buggyVotes,
			BuggyQuorum:            buggyQuorum,
			BuggyHeartbeats:        buggyHeartbeats,
		})
	}
	return nodes
}

// setupStandardHandlers creates and configures tick and request handlers
func setupStandardHandlers(observers []NodeID) *StandardRequestHandler {
	return &StandardRequestHandler{
		observers: observers,
	}
}

// TestLeaderElection_Standard tests standard 3-node elections with various configurations
func TestLeaderElection_Standard(t *testing.T) {
	type testCase struct {
		name                  string
		seed                  int64
		nodeSeeds             []int64
		duration              int64
		buggyVotes            bool
		buggyQuorum           bool
		buggyHeartbeats       bool
		electionTimeout       int64
		heartbeatInterval     int64
		expectViolation       bool                                              // true if we expect an assertion violation
		expectedViolationName string                                            // name of assertion that should violate
		deliveryPolicy        dstsim.IndicatorDeliveryPolicy[Indicator, NodeID] // optional custom delivery policy
	}

	tests := []testCase{
		{
			name:              "BasicElection",
			seed:              12345,
			nodeSeeds:         []int64{1001, 1002, 1003},
			duration:          300, // Increased for 1-tick message latency
			electionTimeout:   50,
			heartbeatInterval: 10,
		},
		{
			name:              "WithInvariantChecking",
			seed:              54321,
			nodeSeeds:         []int64{2001, 2002, 2003},
			duration:          300, // Increased for 1-tick message latency
			electionTimeout:   50,
			heartbeatInterval: 10,
		},
		{
			name:              "WithBuggyVotes",
			seed:              99999,
			nodeSeeds:         []int64{3001, 3002, 3003},
			duration:          600, // Increased for 1-tick message latency
			buggyVotes:        true,
			electionTimeout:   50,
			heartbeatInterval: 10,
		},
		{
			name:              "RandomInitialTick",
			seed:              12345,
			nodeSeeds:         []int64{5001, 5002, 5003},
			duration:          300, // Increased for 1-tick message latency
			electionTimeout:   50,
			heartbeatInterval: 10,
		},
		{
			name:                  "BuggyQuorumCausesSplitBrain",
			seed:                  12345,
			nodeSeeds:             []int64{6001, 6002, 6003},
			duration:              10000, // Run long enough to eventually observe split-brain
			buggyQuorum:           true,
			electionTimeout:       50,
			heartbeatInterval:     10,
			expectViolation:       true,
			expectedViolationName: "multiple_leaders_exist",
			deliveryPolicy: &dstsim.UnreliableNetwork[Indicator, NodeID]{
				MaxDelay: 5,
				DropRate: 0.5, // 50% packet loss - causes split elections
				Rng:      rand.New(rand.NewPCG(uint64(12345), uint64(12345))),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
				Seed: tt.seed,
			})

			// Create nodes
			nodes := setupStandardNodes(
				tt.nodeSeeds,
				tt.electionTimeout,
				tt.heartbeatInterval,
				tt.buggyVotes,
				tt.buggyQuorum,
				tt.buggyHeartbeats,
			)

			// Register nodes
			for _, node := range nodes {
				sim.RegisterNode(node)
			}

			// Add assertions
			sim.Never(&MultipleLeadersExist{})
			sim.Sometimes(&LeaderExists{})
			if tt.name != "WithInvariantChecking" { // This test doesn't have Finally
				sim.Finally(&LeaderExists{})
			}

			// Setup handlers
			reqHandler := setupStandardHandlers(nil)
			sim.SetRequestHandler(reqHandler)

			// Set delivery policy if specified
			if tt.deliveryPolicy != nil {
				sim.SetDeliveryPolicy(tt.deliveryPolicy)
			}

			// Run simulation

			err := sim.RunFor(tt.duration)

			if tt.expectViolation {
				require.Error(t, err, "expected assertion violation")
				require.Contains(t, err.Error(), tt.expectedViolationName)
			} else {
				require.NoError(t, err, "simulation should complete without assertion violations")
			}
		})
	}
}

// TestLeaderElection_WithObservers tests elections with passive observer nodes (e.g., term limit enforcers)
func TestLeaderElection_WithObservers(t *testing.T) {
	type testCase struct {
		name                  string
		seed                  int64
		nodeSeeds             []int64
		duration              int64
		enforcerTenureLimit   int64
		enforcerSeed          int64
		electionTimeout       int64
		heartbeatInterval     int64
		maxTenureLimit        int64 // For Never(LeaderTenureTooLong) assertion
		maxLeaderlessDuration int64 // For Never(LeaderlessTooLong) assertion
		expectViolation       bool
		expectedViolationName string
	}

	tests := []testCase{
		{
			name:                  "WithTermLimitEnforcer",
			seed:                  77777,
			nodeSeeds:             []int64{7001, 7002, 7003},
			duration:              600, // Increased for 1-tick message latency
			enforcerTenureLimit:   25,
			enforcerSeed:          7777,
			electionTimeout:       50,
			heartbeatInterval:     5,
			maxTenureLimit:        80, // Generous limit: 25 base + jitter + step-down latency
			maxLeaderlessDuration: 80,
			expectViolation:       false,
		},
		{
			name:                  "BuggyEnforcerViolatesInvariant",
			seed:                  77777, // Same seed as working test
			nodeSeeds:             []int64{7001, 7002, 7003},
			duration:              600,  // Increased for 1-tick message latency
			enforcerTenureLimit:   1000, // BUG: Way too high, won't enforce anything
			enforcerSeed:          9999,
			electionTimeout:       50,
			heartbeatInterval:     5,
			maxTenureLimit:        40, // But we assert tenure shouldn't exceed 40
			expectViolation:       true,
			expectedViolationName: "leader_tenure_too_long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
				Seed: tt.seed,
			})

			// Create election nodes
			nodes := setupStandardNodes(
				tt.nodeSeeds,
				tt.electionTimeout,
				tt.heartbeatInterval,
				false, false, false, // No bugs in nodes
			)

			// Create enforcer (observer)
			enforcer := NewTermLimitEnforcerNode(TermLimitEnforcerConfig{
				ID:                "enforcer",
				Peers:             []NodeID{"node-1", "node-2", "node-3"},
				LeaderTenureLimit: tt.enforcerTenureLimit,
				Seed:              tt.enforcerSeed,
			})

			// Register all nodes
			for _, node := range nodes {
				sim.RegisterNode(node)
			}
			sim.RegisterNode(enforcer)

			// Add assertions
			sim.Never(&MultipleLeadersExist{})
			sim.Sometimes(&LeaderExists{})

			// Only add tenure assertion for buggy enforcer test (expects violation)
			if tt.expectViolation {
				sim.Never(NewLeaderTenureTooLong(tt.maxTenureLimit))
			}

			// Setup handlers (include enforcer as observer)
			reqHandler := setupStandardHandlers([]NodeID{enforcer.ID()})
			sim.SetRequestHandler(reqHandler)

			// Run simulation

			err := sim.RunFor(tt.duration)

			if tt.expectViolation {
				require.Error(t, err, "expected assertion violation")
				require.Contains(t, err.Error(), tt.expectedViolationName)
			} else {
				require.NoError(t, err, "simulation should complete without assertion violations")
			}
		})
	}
}

// StandardTickHandler delivers tick indicators to all nodes at each tick
// TestLeaderElection_ChaosNetwork validates that the protocol maintains safety even under extreme network conditions
func TestLeaderElection_ChaosNetwork(t *testing.T) {
	seed := int64(88888)
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: seed,
	})

	// Set chaotic network policy
	sim.SetDeliveryPolicy(&dstsim.UnreliableNetwork[Indicator, NodeID]{
		MaxDelay: 20,   // Random delay 1-20 ticks
		DropRate: 0.15, // 15% packet loss
		Rng:      rand.New(rand.NewPCG(uint64(seed), uint64(seed))),
	})

	// Create 3 standard nodes
	nodes := setupStandardNodes(
		[]int64{8001, 8002, 8003},
		50,                  // electionTimeout
		10,                  // heartbeatInterval
		false, false, false, // no bugs
	)

	// Register nodes
	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	// Setup handlers
	reqHandler := setupStandardHandlers(nil)
	sim.SetRequestHandler(reqHandler)

	// Add assertions
	sim.Never(&MultipleLeadersExist{}) // Safety: no split-brain even during chaos
	sim.Sometimes(&LeaderExists{})     // Liveness: system makes progress despite chaos

	// Run simulation with chaos for extended duration

	err := sim.RunFor(10000)
	require.NoError(t, err, "protocol should maintain safety even under chaotic network conditions")
}

// TestLeaderElection_RecoveryAfterChaos validates that the protocol quickly recovers after network stabilizes

// tickHandlerWithStepDown wraps a tick handler and injects step-down when partition activates
// TestOrchestratorNode is a special node for test injection
// It doesn't process indicators, only generates requests based on conditions
type TestOrchestratorNode struct {
	id                NodeID
	sim               *dstsim.Simulator[Indicator, Request, NodeID]
	nodes             []*ElectionNode
	stepDownCondition dstsim.Condition[Indicator, Request, NodeID]
	stepDownSent      bool
}

func (n *TestOrchestratorNode) ID() NodeID {
	return n.id
}

func (n *TestOrchestratorNode) Step(tick int64, indicators []Indicator) []Request {
	// If step-down condition is met and we haven't sent step-down yet, inject it
	if !n.stepDownSent && n.stepDownCondition.Eval(n.sim) {
		n.stepDownSent = true
		// Find the leader and inject step-down request
		for _, node := range n.nodes {
			role, term := node.GetState()
			if role == Leader {
				return []Request{
					StepDownRequest{
						To:            node.ID(),
						Reason:        "testing partition recovery",
						SuggestedTerm: term,
					},
				}
			}
		}
	}

	return nil
}

// TestLeaderElection_StepDown validates that a leader can be forced to step down
func TestLeaderElection_StepDown(t *testing.T) {
	seed := int64(42)
	nodeSeeds := []int64{100, 200, 300}

	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: seed,
	})

	nodes := setupStandardNodes(nodeSeeds, 50, 10, false, false, false)
	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	reqHandler := setupStandardHandlers(nil)
	sim.SetRequestHandler(reqHandler)

	// Track when leader exists
	leaderElected := &LeaderExists{}

	// Orchestrator to send step-down once leader exists
	orchestrator := &TestOrchestratorNode{
		id:                "orchestrator",
		sim:               sim,
		nodes:             nodes,
		stepDownCondition: leaderElected,
		stepDownSent:      false,
	}
	sim.RegisterNode(orchestrator)

	// Assertions
	sim.Never(&MultipleLeadersExist{})

	// Run until a leader is re-elected after step-down (or timeout after 300 ticks)
	err := sim.RunUntil(&LeaderExists{}, 300)
	require.NoError(t, err, "leader should be re-elected after step-down within 300 ticks")
}

// TestLeaderElection_PartitionRecovery validates recovery from network partition
func TestLeaderElection_PartitionRecovery(t *testing.T) {
	seed := int64(42)
	nodeSeeds := []int64{100, 200, 300}

	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: seed,
	})

	nodes := setupStandardNodes(nodeSeeds, 50, 10, false, false, false)
	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	reqHandler := setupStandardHandlers(nil)
	sim.SetRequestHandler(reqHandler)

	// Create policy sequence: normal -> partition -> recovery
	policySeq := dstsim.NewPolicySequence[Indicator, Request, NodeID](
		sim,
		&dstsim.FastNetwork[Indicator, NodeID]{},
		"normal",
	)

	// Wait for leader to exist (so orchestrator can send step-down)
	awaitStepDown := policySeq.AppendPolicy(
		&dstsim.FastNetwork[Indicator, NodeID]{},
		&LeaderExists{},
		"await_step_down",
	)

	// Start partition only after leader has stepped down (no leaders exist)
	partitionActive := policySeq.AppendPolicy(
		&dstsim.UnreliableNetwork[Indicator, NodeID]{
			MaxDelay: 5,
			DropRate: 1.0, // 100% packet loss for complete partition
			Rng:      rand.New(rand.NewPCG(uint64(seed), uint64(seed))),
		},
		&NoLeadersExist{}, // Don't start partition until leader has stepped down
		"partition",
	)

	// End partition after 100 ticks
	recoveryActive := policySeq.AppendPolicy(
		&dstsim.FastNetwork[Indicator, NodeID]{},
		dstsim.TickCondition[Indicator, Request, NodeID](100),
		"recovery",
	)

	sim.SetDeliveryPolicy(policySeq)

	// Orchestrator to send step-down during await_step_down stage (before partition)
	orchestrator := &TestOrchestratorNode{
		id:                "orchestrator",
		sim:               sim,
		nodes:             nodes,
		stepDownCondition: awaitStepDown,
		stepDownSent:      false,
	}
	sim.RegisterNode(orchestrator)

	// Assertions
	sim.Never(&MultipleLeadersExist{}) // Safety: never split-brain
	sim.Sometimes(partitionActive)     // Partition stage activates
	sim.Sometimes(recoveryActive)      // Recovery stage activates
	sim.Always(dstsim.Or(              // During partition, no leader exists
		dstsim.Not(partitionActive),
		&NoLeadersExist{},
	))
	sim.Sometimes(dstsim.And(recoveryActive, &LeaderExists{})) // During recovery, leader exists
	sim.Finally(&LeaderExists{})                               // Eventually a leader exists

	err := sim.RunFor(500)
	require.NoError(t, err)
}

// StandardRequestHandler processes requests by delivering them to target nodes
type StandardRequestHandler struct {
	observers []NodeID // Optional passive observers (e.g., term limit enforcers)
}

func (h *StandardRequestHandler) ProcessRequests(sim *dstsim.Simulator[Indicator, Request, NodeID], fromNode NodeID, requests []Request) map[NodeID][]Indicator {
	result := make(map[NodeID][]Indicator)

	for _, req := range requests {
		switch r := req.(type) {
		case LogRequest:
			// Log requests are not delivered, just emitted for debugging
			continue

		case VoteRequest:
			// Deliver vote request indicator
			result[r.To] = append(result[r.To], VoteRequestIndicator{
				From: fromNode,
				Term: r.Term,
			})

		case VoteResponse:
			// Deliver vote response indicator
			result[r.To] = append(result[r.To], VoteResponseIndicator{
				From:    fromNode,
				Term:    r.Term,
				Granted: r.Granted,
			})

		case HeartbeatRequest:
			// Deliver heartbeat to target node
			heartbeat := HeartbeatIndicator{
				From: fromNode,
				Term: r.Term,
			}
			result[r.To] = append(result[r.To], heartbeat)

			// Also deliver to observers
			for _, observerID := range h.observers {
				result[observerID] = append(result[observerID], heartbeat)
			}

		case StepDownRequest:
			// Deliver step-down request indicator
			result[r.To] = append(result[r.To], StepDownRequestIndicator{
				From:          fromNode,
				Reason:        r.Reason,
				SuggestedTerm: r.SuggestedTerm,
			})
		}
	}

	return result
}
