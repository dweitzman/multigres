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

// SingleLeaderInvariant checks that at most one leader exists
type SingleLeaderInvariant struct{}

func (inv *SingleLeaderInvariant) Check(sim *dstsim.Simulator[Indicator, Request, NodeID]) []string {
	leaders := make([]string, 0)

	for _, node := range sim.Nodes() {
		if electionNode, ok := node.(*ElectionNode); ok {
			role, term := electionNode.GetState()
			if role == Leader {
				leaders = append(leaders, fmt.Sprintf("%s (term %d)", electionNode.ID(), term))
			}
		}
	}

	if len(leaders) > 1 {
		return []string{fmt.Sprintf("multiple leaders detected: %v", leaders)}
	}

	return nil
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
func processRequests(sim *dstsim.Simulator[Indicator, Request, NodeID], from NodeID, requests []Request) {
	queue := make([]requestWithSender, 0)
	for _, req := range requests {
		queue = append(queue, requestWithSender{from: from, request: req})
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		switch r := item.request.(type) {
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
		}
	}
}

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

	// Add single leader invariant
	sim.AddInvariant(&SingleLeaderInvariant{})

	// Simulate message delivery: when a node requests something, deliver it
	// We'll inject ticks and handle requests manually for this simple example

	for tick := range int64(200) {
		// Deliver tick to all nodes
		for _, node := range nodes {
			requests := sim.DeliverIndicator(node.ID(), TickIndicator{CurrentTick: tick})
			processRequests(sim, node.ID(), requests)
		}
	}

	// Verify exactly one leader was elected
	leaderCount := 0
	var leaderID NodeID
	for _, node := range nodes {
		role, _ := node.GetState()
		if role == Leader {
			leaderCount++
			leaderID = node.ID()
		}
	}

	assert.Equal(t, 1, leaderCount, "should have exactly one leader")
	t.Logf("Leader elected: %s", leaderID)
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

	// Add invariant that should not be violated
	sim.AddInvariant(&SingleLeaderInvariant{})

	// Run simulation with invariant checking
	for tick := range int64(200) {
		for _, node := range nodes {
			requests := sim.DeliverIndicator(node.ID(), TickIndicator{CurrentTick: tick})

			for _, req := range requests {
				switch r := req.(type) {
				case VoteRequest:
					sim.DeliverIndicator(r.To, VoteRequestIndicator{
						From: node.ID(),
						Term: r.Term,
					})
				case VoteResponse:
					sim.DeliverIndicator(r.To, VoteResponseIndicator{
						From:    node.ID(),
						Term:    r.Term,
						Granted: r.Granted,
					})
				case HeartbeatRequest:
					sim.DeliverIndicator(r.To, HeartbeatIndicator{
						From: node.ID(),
						Term: r.Term,
					})
				}
			}
		}

		// Check invariants manually (in real version, RunUntil would do this)
		for _, inv := range []*SingleLeaderInvariant{{}} {
			violations := inv.Check(sim)
			require.Empty(t, violations, "invariant violated at tick %d: %v", tick, violations)
		}
	}
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

	sim.AddInvariant(&SingleLeaderInvariant{})

	// Run simulation for exactly 500 ticks
	endTick := initialTick + 500
	for tick := initialTick; tick < endTick; tick++ {
		for _, node := range nodes {
			requests := sim.DeliverIndicator(node.ID(), TickIndicator{CurrentTick: tick})
			processRequests(sim, node.ID(), requests)
		}
	}

	// With seed 99999, exactly one leader should be elected
	leaderCount := 0
	var leaderID NodeID
	for _, node := range nodes {
		role, _ := node.GetState()
		if role == Leader {
			leaderCount++
			leaderID = node.ID()
		}
	}

	// Deterministic assertion: with this seed, we always get exactly 1 leader
	assert.Equal(t, 1, leaderCount, "seed 99999 should produce exactly 1 leader")
	t.Logf("Leader elected with buggy votes: %s", leaderID)
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

		// Add invariant so RunUntil() will catch violations
		sim.AddInvariant(&SingleLeaderInvariant{})

		// Set up interceptor to add random delays
		sim.SetInterceptor(&RandomDelayInterceptor{
			seed:     seed,
			maxDelay: 6,
		})

		// Run simulation - interceptor will automatically delay messages
		for tick := range int64(1000) {
			for _, node := range nodes {
				requests := sim.DeliverIndicator(node.ID(), TickIndicator{CurrentTick: tick})
				processRequests(sim, node.ID(), requests)
			}

			// Run to this tick to process scheduled actions
			if err := sim.RunUntil(tick); err != nil {
				// Check if it's an invariant violation (our bug!)
				var invErr *dstsim.InvariantViolation
				if errors.As(err, &invErr) {
					t.Logf("✓ Bug triggered with seed %d at tick %d: %v", seed, invErr.Tick, invErr.Violations)
					assert.Contains(t, invErr.Violations[0], "multiple leaders")
					return // Test passed!
				}
				t.Fatalf("Unexpected error: %v", err)
			}
		}
	}

	t.Fatal("Bug was not triggered in 10000 seeds - increase seed range or simulation length")
}

// AlwaysFailInvariant is a test invariant that fails at a specific tick
type AlwaysFailInvariant struct {
	failAtTick int64
}

func (inv *AlwaysFailInvariant) Check(sim *dstsim.Simulator[Indicator, Request, NodeID]) []string {
	// Fail at exact tick
	if sim.CurrentTick() == inv.failAtTick {
		return []string{fmt.Sprintf("test invariant: simulated failure at tick %d", inv.failAtTick)}
	}
	return nil
}

// TestLeaderElection_SimulatorStopsOnViolation demonstrates that RunUntil() stops on invariant violations
func TestLeaderElection_SimulatorStopsOnViolation(t *testing.T) {
	// This test verifies that sim.RunUntil() returns an error when an invariant is violated

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

	// Get the initial tick and set invariant to fail 10 ticks later
	initialTick := sim.CurrentTick()
	failTick := initialTick + 10
	sim.AddInvariant(&AlwaysFailInvariant{failAtTick: failTick})

	// Run simulation - should stop at failTick due to invariant violation
	err := sim.RunUntil(initialTick + 100)

	// Verify that we got an invariant violation error
	require.Error(t, err, "simulator should return error on invariant violation")

	var invErr *dstsim.InvariantViolation
	require.ErrorAs(t, err, &invErr, "error should be InvariantViolation type")

	assert.Equal(t, failTick, invErr.Tick, "violation should be detected at failTick")
	assert.Contains(t, invErr.Violations[0], "simulated failure", "violation message should be included")

	t.Logf("✓ Simulator stopped at tick %d with error: %v", invErr.Tick, err)
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

	// Fast-forward through 10,000 ticks (would be 1000 seconds at 100ms/tick)
	// This executes instantly in DST
	for tick := range int64(10000) {
		for _, node := range nodes {
			requests := sim.DeliverIndicator(node.ID(), TickIndicator{CurrentTick: tick})
			processRequests(sim, node.ID(), requests)
		}
	}

	// After 10,000 ticks, we should still have exactly one leader
	leaderCount := 0
	for _, node := range nodes {
		role, _ := node.GetState()
		if role == Leader {
			leaderCount++
		}
	}

	assert.Equal(t, 1, leaderCount, "should maintain single leader over long simulation")
	t.Logf("Successfully fast-forwarded through 10,000 ticks")
}

// TestLeaderElection_RandomInitialTick tests that simulations work correctly with randomized initial ticks
func TestLeaderElection_RandomInitialTick(t *testing.T) {
	// Simulator always starts at a random initial tick based on seed
	// With seed 12345, we should get a deterministic initial tick
	sim := dstsim.NewSimulator[Indicator, Request, NodeID](dstsim.SimulatorOptions{
		Seed: 12345,
	})
	initialTick := sim.CurrentTick()
	// This exact value is deterministic for seed 12345 (with rand/v2)
	assert.Equal(t, int64(898743), initialTick, "seed 12345 should produce initial tick 898743")
	t.Logf("Initial tick (seed=12345): %d", initialTick)

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

	// Run simulation for 200 ticks from the random initial tick
	endTick := initialTick + 200
	for tick := initialTick; tick < endTick; tick++ {
		for _, node := range nodes {
			requests := sim.DeliverIndicator(node.ID(), TickIndicator{CurrentTick: tick})
			processRequests(sim, node.ID(), requests)
		}
	}

	// Verify exactly one leader was elected
	leaderCount := 0
	for _, node := range nodes {
		role, _ := node.GetState()
		if role == Leader {
			leaderCount++
		}
	}

	assert.Equal(t, 1, leaderCount, "should elect exactly one leader regardless of initial tick")
	t.Logf("Successfully elected leader with random initial tick %d", initialTick)
}
