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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
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
	for nodeID := range c.leaderStartTicks {
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

// LeaderlessTooLong checks if the system has been without a leader for too long
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
type requestWithSender struct {
	from    NodeID
	request Request
}

// RandomDelayInterceptor adds random delays to messages
type RandomDelayInterceptor struct {
	seed         int64
	maxDelay     int64
	messageCount int64
}

func (i *RandomDelayInterceptor) InterceptIndicator(currentTick int64, target NodeID, indicator Indicator) int64 {
	// Deliver immediately for ticks (no delay on clock signals)
	if _, ok := indicator.(TickIndicator); ok {
		return 0
	}

	// Add random delay to other indicators
	i.messageCount++
	delay := (i.seed + currentTick + i.messageCount) % i.maxDelay
	return delay
}

func (i *RandomDelayInterceptor) InterceptRequest(currentTick int64, from NodeID, request Request) int64 {
	// Not used in this test (we don't intercept requests, only indicators)
	return 0
}

// processRequests recursively processes all requests until none remain
// observers is an optional list of node IDs that should passively receive heartbeats (e.g., term limit enforcers)
// TestLeaderElection_BasicElection tests that a 3-node cluster elects exactly one leader
func TestLeaderElection_BasicElection(t *testing.T) {
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 12345,
	})

	// Create 3 nodes
	nodes := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   1001,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   1002,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   1003,
		}),
	}

	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	// Add assertions: safety and liveness
	sim.Never(&MultipleLeadersExist{}) // Safety: no split brain
	sim.Sometimes(&LeaderExists{})     // Liveness: leader must be elected at least once
	sim.Finally(&LeaderExists{})       // Finally: must have exactly one leader at end

	// Set up handlers to automate simulation
	nodeIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	reqHandler := &StandardRequestHandler{}
	sim.SetTickHandler(&StandardTickHandler{nodes: nodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Run simulation - handlers automatically deliver ticks and process requests
	initialTick := sim.CurrentTick()
	err := sim.RunUntil(initialTick + 200)
	require.NoError(t, err, "simulation should complete without assertion violations")
}

// TestLeaderElection_WithInvariantChecking tests that invariants are checked during simulation
func TestLeaderElection_WithInvariantChecking(t *testing.T) {
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 54321,
	})

	nodes := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   2001,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   2002,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   2003,
		}),
	}

	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	// Add assertions
	sim.Never(&MultipleLeadersExist{}) // Safety: no split brain
	sim.Sometimes(&LeaderExists{})     // Liveness: leader must be elected

	// Set up handlers to automate simulation
	nodeIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	reqHandler := &StandardRequestHandler{}
	sim.SetTickHandler(&StandardTickHandler{nodes: nodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Run simulation - handlers automatically deliver ticks and process requests
	initialTick := sim.CurrentTick()
	err := sim.RunUntil(initialTick + 200)
	require.NoError(t, err, "simulation should complete without assertion violations")
}

// TestLeaderElection_WithBuggyVotes tests that DST can detect bugs deterministically
func TestLeaderElection_WithBuggyVotes(t *testing.T) {
	// With buggyVotes=true, some vote responses are dropped (10% probability)
	// With this specific seed, we know exactly what happens

	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 99999,
	})

	initialTick := sim.CurrentTick()

	nodes := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   3001,
			BuggyVotes:             true,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   3002,
			BuggyVotes:             true,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   3003,
			BuggyVotes:             true,
		}),
	}

	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	sim.Never(&MultipleLeadersExist{}) // Safety: no split brain
	sim.Sometimes(&LeaderExists{})     // Liveness: leader must be elected
	sim.Finally(&LeaderExists{})       // Finally: must have leader at end

	// Set up handlers to automate simulation
	nodeIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	reqHandler := &StandardRequestHandler{}
	sim.SetTickHandler(&StandardTickHandler{nodes: nodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Run simulation for exactly 500 ticks
	err := sim.RunUntil(initialTick + 500)
	require.NoError(t, err, "simulation should complete without assertion violations")

	// Assertions verify correct behavior (no manual checks needed)
}

// TestLeaderElection_InvariantDetection demonstrates that DST catches bugs deterministically
func TestLeaderElection_InvariantDetection(t *testing.T) {
	// Try many seeds to find one where the probabilistic bug triggers
	// Once found, that seed will ALWAYS trigger the bug (deterministic)

	for seed := int64(1); seed <= 10000; seed++ {
		sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
			Seed: seed,
		})

		nodes := []*ElectionNode{
			NewElectionNode(NodeConfig{
				ID:                     NodeID("node-1"),
				Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
				ElectionTimeoutTicks:   20,
				HeartbeatIntervalTicks: 10,
				Seed:                   seed * 100,
				BuggyQuorum:            true,
				BuggyHeartbeats:        true,
			}),
			NewElectionNode(NodeConfig{
				ID:                     NodeID("node-2"),
				Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
				ElectionTimeoutTicks:   20,
				HeartbeatIntervalTicks: 10,
				Seed:                   seed*100 + 1,
				BuggyQuorum:            true,
				BuggyHeartbeats:        true,
			}),
			NewElectionNode(NodeConfig{
				ID:                     NodeID("node-3"),
				Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
				ElectionTimeoutTicks:   20,
				HeartbeatIntervalTicks: 10,
				Seed:                   seed*100 + 2,
				BuggyQuorum:            true,
				BuggyHeartbeats:        true,
			}),
		}

		for _, node := range nodes {
			sim.RegisterNode(node)
		}

		// Add assertion so RunUntil() will catch violations
		sim.Never(&MultipleLeadersExist{})

		// Set up interceptor to add random delays
		sim.SetInterceptor(&RandomDelayInterceptor{
			seed:     seed,
			maxDelay: 6,
		})

		// Set up handlers to automate simulation
		nodeIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
		reqHandler := &StandardRequestHandler{}
		sim.SetTickHandler(&StandardTickHandler{nodes: nodeIDs, requestHandler: reqHandler})
		sim.SetRequestHandler(reqHandler)

		// Run simulation - handlers automatically deliver ticks and process requests
		initialTick := sim.CurrentTick()
		if err := sim.RunUntil(initialTick + 1000); err != nil {
			// Check if it's an assertion violation (our bug!)
			var assertErr *dstsim.AssertionViolation
			if errors.As(err, &assertErr) {
				t.Logf("✓ Bug triggered with seed %d at tick %d: %v", seed, assertErr.Tick, assertErr.Error())
				assert.Contains(t, assertErr.Error(), "multiple_leaders_exist")
				return // Test passed!
			}
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	t.Fatal("Bug was not triggered in 10000 seeds - increase seed range or simulation length")
}

// TestConditionFailsAtTick is a test condition that becomes true at a specific tick
type TestConditionFailsAtTick struct {
	failAtTick int64
}

func (c *TestConditionFailsAtTick) Name() string {
	return "test_condition_fails_at_tick"
}

func (c *TestConditionFailsAtTick) Eval(sim *dstsim.Simulator[Indicator, Request, NodeID]) bool {
	return sim.CurrentTick() == c.failAtTick
}

func (c *TestConditionFailsAtTick) Describe(sim *dstsim.Simulator[Indicator, Request, NodeID]) string {
	return fmt.Sprintf("simulated failure at tick %d", c.failAtTick)
}

// TestLeaderElection_SimulatorStopsOnViolation demonstrates that RunUntil() stops on assertion violations
func TestLeaderElection_SimulatorStopsOnViolation(t *testing.T) {
	// This test verifies that sim.RunUntil() returns an error when an assertion is violated

	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 88888,
	})

	node := NewElectionNode(NodeConfig{
		ID:                     NodeID("node-1"),
		Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
		ElectionTimeoutTicks:   50,
		HeartbeatIntervalTicks: 10,
		Seed:                   6001,
	})

	sim.RegisterNode(node)

	// Get the initial tick and set a condition to fail 10 ticks later
	initialTick := sim.CurrentTick()
	failTick := initialTick + 10
	sim.Never(&TestConditionFailsAtTick{failAtTick: failTick})

	// Run simulation - should stop at failTick due to assertion violation
	err := sim.RunUntil(initialTick + 100)

	// Verify that we got an assertion violation error
	require.Error(t, err, "simulator should return error on assertion violation")

	var assertErr *dstsim.AssertionViolation
	require.ErrorAs(t, err, &assertErr, "error should be AssertionViolation type")

	assert.Equal(t, failTick, assertErr.Tick, "violation should be detected at failTick")
	assert.Contains(t, assertErr.Description, "simulated failure", "violation message should be included")

	t.Logf("✓ Simulator stopped at tick %d with error: %v", assertErr.Tick, err)
}

// TestLeaderElection_FastForward demonstrates fast-forwarding through time
func TestLeaderElection_FastForward(t *testing.T) {
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 11111,
	})

	nodes := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   4001,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   4002,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   4003,
		}),
	}

	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	// Set up handlers to automate simulation
	nodeIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	reqHandler := &StandardRequestHandler{}
	sim.SetTickHandler(&StandardTickHandler{nodes: nodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Add assertions
	sim.Never(&MultipleLeadersExist{}) // Safety: no split brain
	sim.Finally(&LeaderExists{})       // Finally: must have leader at end

	// Fast-forward through 10,000 ticks (would be 1000 seconds at 100ms/tick)
	// This executes instantly in DST
	initialTick := sim.CurrentTick()
	err := sim.RunUntil(initialTick + 10000)
	require.NoError(t, err, "simulation should complete without assertion violations")
}

// TestLeaderElection_RandomInitialTick tests that simulations work correctly with randomized initial ticks
func TestLeaderElection_RandomInitialTick(t *testing.T) {
	// Simulator always starts at a random initial tick based on seed
	// With seed 12345, we should get a deterministic initial tick
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 12345,
	})
	initialTick := sim.CurrentTick()

	// Test that election works correctly regardless of initial tick
	nodes := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   5001,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   5002,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 10,
			Seed:                   5003,
		}),
	}

	for _, node := range nodes {
		sim.RegisterNode(node)
	}

	// Add assertions
	sim.Never(&MultipleLeadersExist{}) // Safety: no split brain
	sim.Finally(&LeaderExists{})       // Finally: must have leader at end

	// Set up handlers to automate simulation
	nodeIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	reqHandler := &StandardRequestHandler{}
	sim.SetTickHandler(&StandardTickHandler{nodes: nodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Run simulation for 200 ticks from the random initial tick
	err := sim.RunUntil(initialTick + 200)
	require.NoError(t, err, "simulation should complete without assertion violations")
}

// TestLeaderElection_WithTermLimitEnforcer tests that a term limit enforcer node can monitor and force leader rotation
func TestLeaderElection_WithTermLimitEnforcer(t *testing.T) {
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 77777,
	})

	// Create 3 voting nodes
	// Use normal election timeout (50 ticks) to get initial leader
	// The observer (with 25-tick tenure limit) will force step-downs faster than
	// natural re-elections would occur
	voterIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	voters := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 5, // Faster heartbeats so observer sees leader quickly
			Seed:                   7001,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 5,
			Seed:                   7002,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 5,
			Seed:                   7003,
		}),
	}

	for _, node := range voters {
		sim.RegisterNode(node)
	}

	// Create term limit enforcer node (non-voting, different state)
	enforcer := NewTermLimitEnforcerNode(TermLimitEnforcerConfig{
		ID:                NodeID("observer-1"),
		Peers:             voterIDs,
		LeaderTenureLimit: 25, // Force rotation after 25 ticks
		Seed:              8888,
	})
	sim.RegisterNode(enforcer)

	// Add assertions: safety and liveness
	sim.Never(&MultipleLeadersExist{})    // Safety: no split brain
	sim.Sometimes(&LeaderExists{})        // Liveness: leader must be elected at least once
	sim.Never(NewLeaderTenureTooLong(40)) // Safety: enforcer limits tenure (25 + jitter 0-4 + processing)
	sim.Never(NewLeaderlessTooLong(60))   // Bounded liveness: max 60 ticks without leader (election timeout is 50)

	// Set up handlers to automate simulation (include enforcer in nodes list)
	allNodeIDs := append(voterIDs, enforcer.ID())
	reqHandler := &StandardRequestHandler{observers: []NodeID{enforcer.ID()}}
	sim.SetTickHandler(&StandardTickHandler{nodes: allNodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Run simulation for 500 ticks (enough for multiple rotations)
	initialTick := sim.CurrentTick()

	// Run simulation for 500 ticks (enough for multiple rotations)
	err := sim.RunUntil(initialTick + 499)
	require.NoError(t, err, "simulation should complete - assertions verify enforcer works correctly")
}

// TestLeaderElection_BuggyEnforcerViolatesInvariant tests that the tenure invariant catches bugs
func TestLeaderElection_BuggyEnforcerViolatesInvariant(t *testing.T) {
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 77777, // Use same seed as working test
	})

	// Create 3 voting nodes (same config as working test)
	voterIDs := []NodeID{NodeID("node-1"), NodeID("node-2"), NodeID("node-3")}
	voters := []*ElectionNode{
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-1"),
			Peers:                  []NodeID{NodeID("node-2"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 5,
			Seed:                   7001,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-2"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-3")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 5,
			Seed:                   7002,
		}),
		NewElectionNode(NodeConfig{
			ID:                     NodeID("node-3"),
			Peers:                  []NodeID{NodeID("node-1"), NodeID("node-2")},
			ElectionTimeoutTicks:   50,
			HeartbeatIntervalTicks: 5,
			Seed:                   7003,
		}),
	}

	for _, node := range voters {
		sim.RegisterNode(node)
	}

	// Create buggy enforcer with a very high tenure limit (essentially broken)
	enforcer := NewTermLimitEnforcerNode(TermLimitEnforcerConfig{
		ID:                NodeID("buggy-enforcer"),
		Peers:             voterIDs,
		LeaderTenureLimit: 1000, // BUG: Way too high, won't enforce anything
		Seed:              9999,
	})
	sim.RegisterNode(enforcer)

	// Add assertions - buggy enforcer will violate tenure limit
	sim.Never(&MultipleLeadersExist{})    // Safety: no split brain
	sim.Sometimes(&LeaderExists{})        // Liveness: leader must be elected
	sim.Never(NewLeaderTenureTooLong(40)) // This should be violated due to buggy enforcer

	// Set up handlers to automate simulation (include enforcer in nodes list)
	allNodeIDs := append(voterIDs, enforcer.ID())
	reqHandler := &StandardRequestHandler{observers: []NodeID{enforcer.ID()}}
	sim.SetTickHandler(&StandardTickHandler{nodes: allNodeIDs, requestHandler: reqHandler})
	sim.SetRequestHandler(reqHandler)

	// Run simulation and expect the tenure assertion to fail
	initialTick := sim.CurrentTick()

	// Run simulation - should fail with assertion violation
	err := sim.RunUntil(initialTick + 499)
	require.Error(t, err, "simulation should fail due to buggy enforcer not enforcing tenure limit")

	var assertErr *dstsim.AssertionViolation
	require.ErrorAs(t, err, &assertErr, "error should be an assertion violation")
	assert.Contains(t, assertErr.ConditionName, "leader_tenure_too_long", "should violate tenure limit assertion")
}

// StandardTickHandler delivers tick indicators to all nodes at each tick
type StandardTickHandler struct {
	nodes          []NodeID
	requestHandler *StandardRequestHandler
}

func (h *StandardTickHandler) OnTick(sim *dstsim.Simulator[Indicator, Request, NodeID], tick int64) {
	// Deliver the same tick value to all nodes
	for _, nodeID := range h.nodes {
		requests := sim.DeliverIndicator(nodeID, TickIndicator{CurrentTick: tick})
		if h.requestHandler != nil {
			h.requestHandler.ProcessRequests(sim, nodeID, requests)
		}
	}
}

// StandardRequestHandler processes requests by delivering them to target nodes
type StandardRequestHandler struct {
	observers []NodeID // Optional passive observers (e.g., term limit enforcers)
}

func (h *StandardRequestHandler) ProcessRequests(sim *dstsim.Simulator[Indicator, Request, NodeID], fromNode NodeID, requests []Request) {
	queue := make([]requestWithSender, 0)
	for _, req := range requests {
		queue = append(queue, requestWithSender{from: fromNode, request: req})
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		switch r := item.request.(type) {
		case LogRequest:
			// Log requests are not delivered, just emitted for debugging
			continue

		case VoteRequest:
			// Deliver vote request to target node
			responses := sim.DeliverIndicator(r.To, VoteRequestIndicator{
				From: item.from,
				Term: r.Term,
			})
			for _, resp := range responses {
				queue = append(queue, requestWithSender{from: r.To, request: resp})
			}

		case VoteResponse:
			// Deliver vote response to target node
			responses := sim.DeliverIndicator(r.To, VoteResponseIndicator{
				From:    item.from,
				Term:    r.Term,
				Granted: r.Granted,
			})
			for _, resp := range responses {
				queue = append(queue, requestWithSender{from: r.To, request: resp})
			}

		case HeartbeatRequest:
			// Deliver heartbeat to target node
			responses := sim.DeliverIndicator(r.To, HeartbeatIndicator{
				From: item.from,
				Term: r.Term,
			})
			for _, resp := range responses {
				queue = append(queue, requestWithSender{from: r.To, request: resp})
			}

			// Also deliver to observers and queue their responses
			for _, observerID := range h.observers {
				obsResponses := sim.DeliverIndicator(observerID, HeartbeatIndicator{
					From: item.from,
					Term: r.Term,
				})
				for _, obsResp := range obsResponses {
					queue = append(queue, requestWithSender{from: observerID, request: obsResp})
				}
			}

		case StepDownRequest:
			// Deliver step-down request to target node
			responses := sim.DeliverIndicator(r.To, StepDownRequestIndicator{
				From:          item.from,
				Reason:        r.Reason,
				SuggestedTerm: r.SuggestedTerm,
			})
			for _, resp := range responses {
				queue = append(queue, requestWithSender{from: r.To, request: resp})
			}
		}
	}
}
