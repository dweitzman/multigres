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

package consensus_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// memStorage is an in-memory PoolerStorage implementation for tests.
type memStorage struct {
	state consensus.PoolerPersistentState
}

func (s *memStorage) Save(state consensus.PoolerPersistentState) error {
	s.state = state
	return nil
}

// consensusHandler converts consensus Requests into Indicators and routes them.
// It also handles PoolerMembershipRequest from the discovery node by broadcasting
// PoolerDiscoveredIndicator / PoolerRemovedIndicator to all registered OrchNodes.
type consensusHandler struct {
	sim          *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]
	statusSeqMap map[consensus.NodeID]int64 // monotonically increasing per pooler
}

func (h *consensusHandler) orchIDs() []consensus.NodeID {
	var ids []consensus.NodeID
	for _, node := range h.sim.Nodes() {
		if _, ok := node.(*consensus.OrchNode); ok {
			ids = append(ids, node.ID())
		}
	}
	return ids
}

func (h *consensusHandler) poolerIDs() []consensus.NodeID {
	var ids []consensus.NodeID
	for _, node := range h.sim.Nodes() {
		if _, ok := node.(*consensus.PoolerNode); ok {
			ids = append(ids, node.ID())
		}
	}
	return ids
}

func (h *consensusHandler) ProcessRequests(
	sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
	fromNode consensus.NodeID,
	requests []consensus.Request,
) map[consensus.NodeID][]consensus.Indicator {
	result := make(map[consensus.NodeID][]consensus.Indicator)
	for _, req := range requests {
		switch r := req.(type) {
		case consensus.BroadcastStateRequest:
			targets := r.Targets
			if targets == nil {
				targets = h.poolerIDs()
			}
			for _, pid := range targets {
				result[pid] = append(result[pid], consensus.OrchStateIndicator{
					FromOrch:            fromNode,
					State:               r.State,
					ExpectedPrimaryTerm: r.ExpectedPrimaryTerm,
				})
			}
		case consensus.PoolerResponseRequest:
			result[r.ToOrch] = append(result[r.ToOrch], consensus.PoolerResponseIndicator{
				FromPooler:   fromNode,
				Accepted:     r.Accepted,
				KnownTerm:    r.KnownTerm,
				KnownCoordID: r.KnownCoordID,
			})
		case consensus.PoolerStatusUpdateRequest:
			h.statusSeqMap[fromNode]++
			seq := h.statusSeqMap[fromNode]
			for _, oid := range h.orchIDs() {
				result[oid] = append(result[oid], consensus.PoolerStatusIndicator{
					PoolerID:       fromNode,
					StatusSeq:      seq,
					State:          r.State,
					Applied:        r.Applied,
					PostgresStatus: r.PostgresStatus,
				})
			}
		case consensus.TerminateRequest:
			result[r.Target] = append(result[r.Target], consensus.TerminateIndicator{})
		case consensus.PoolerMembershipRequest:
			for _, oid := range h.orchIDs() {
				for _, pid := range r.Discovered {
					result[oid] = append(result[oid], consensus.PoolerDiscoveredIndicator{PoolerID: pid})
				}
				for _, pid := range r.Removed {
					result[oid] = append(result[oid], consensus.PoolerRemovedIndicator{PoolerID: pid})
				}
			}
		}
	}
	return result
}

// --- Standard invariants ---
//
// standardInvariants returns the invariants that should hold in every simulation.
// Register them with sim.Always() at the start of each test.
func standardInvariants() []dstsim.Condition[consensus.Indicator, consensus.Request, consensus.NodeID] {
	return []dstsim.Condition[consensus.Indicator, consensus.Request, consensus.NodeID]{
		&atMostOnePrimary{},
		&appliedMonotonicity{},
	}
}

// atMostOnePrimary is a safety invariant: no two PoolerNodes may simultaneously
// have committed Role=Primary. A stronger write-quorum invariant (checking that
// at most one primary has enough active sync replicas) can be layered on top.
type atMostOnePrimary struct{}

func (c *atMostOnePrimary) Name() string { return "at_most_one_primary" }
func (c *atMostOnePrimary) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	count := 0
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			if p.CommittedState().Role == consensus.RolePrimary {
				count++
			}
		}
	}
	return count <= 1
}

func (c *atMostOnePrimary) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	var primaries []string
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			if p.CommittedState().Role == consensus.RolePrimary {
				primaries = append(primaries, string(node.ID()))
			}
		}
	}
	return fmt.Sprintf("nodes with committed primary role: %v", primaries)
}

// appliedMonotonicity is a safety invariant: once a pooler persists Applied=true
// for a given proposal (identified by VotedTerm+VotedSeqNum), Applied must never
// revert to false for that same proposal. Applied may only be false on a new
// proposal (higher term or higher seqnum within the same term).
type appliedMonotonicity struct {
	prev map[consensus.NodeID]consensus.PoolerPersistentState
}

func (c *appliedMonotonicity) Name() string { return "applied_monotonicity" }

func (c *appliedMonotonicity) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	if c.prev == nil {
		c.prev = make(map[consensus.NodeID]consensus.PoolerPersistentState)
	}
	for _, node := range sim.Nodes() {
		p, ok := node.(*consensus.PoolerNode)
		if !ok {
			continue
		}
		curr := p.CommittedState()
		prev, hasPrev := c.prev[p.ID()]
		if hasPrev && prev.Applied && !curr.Applied {
			// Applied reverted — only acceptable if the proposal advanced.
			advanced := curr.VotedTerm > prev.VotedTerm ||
				(curr.VotedTerm == prev.VotedTerm && curr.VotedSeqNum > prev.VotedSeqNum)
			if !advanced {
				return false
			}
		}
		c.prev[p.ID()] = curr
	}
	return true
}

func (c *appliedMonotonicity) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	var violations []string
	for _, node := range sim.Nodes() {
		p, ok := node.(*consensus.PoolerNode)
		if !ok {
			continue
		}
		curr := p.CommittedState()
		prev := c.prev[p.ID()]
		if prev.Applied && !curr.Applied {
			violations = append(violations, fmt.Sprintf(
				"node %v: Applied reverted without proposal advance (term=%d seq=%d → term=%d seq=%d)",
				p.ID(), prev.VotedTerm, prev.VotedSeqNum, curr.VotedTerm, curr.VotedSeqNum,
			))
		}
	}
	return fmt.Sprintf("applied monotonicity violations: %v", violations)
}

// --- Liveness conditions ---

// activePrimaryExists is true when any PoolerNode is an active primary:
// committed to the primary role, applied, and postgres is running.
type activePrimaryExists struct{}

func (c *activePrimaryExists) Name() string { return "active_primary_exists" }
func (c *activePrimaryExists) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok && p.IsActivePrimary() {
			return true
		}
	}
	return false
}

func (c *activePrimaryExists) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			s := p.CommittedState()
			return fmt.Sprintf("node %v role=%v applied=%v postgres=%v",
				node.ID(), s.Role, s.Applied, p.PostgresStatus())
		}
	}
	return "no poolers"
}

// --- Test IDs ---

const (
	orchA   consensus.NodeID = "orch-a"
	orchB   consensus.NodeID = "orch-b"
	pooler1 consensus.NodeID = "pooler-1"
	pooler2 consensus.NodeID = "pooler-2"
	pooler3 consensus.NodeID = "pooler-3"
	// discoveryID is the NodeID used for the discovery node; it must not
	// collide with any orch or pooler ID.
	discoveryID consensus.NodeID = "discovery"
)

// newHappyPathSim creates a simulator with 2 orchs, 3 poolers, and a discovery node.
func newHappyPathSim(t *testing.T, seed int64) (
	*dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
	map[consensus.NodeID]*memStorage,
) {
	t.Helper()
	sim := dstsim.NewSimulator[consensus.Indicator, consensus.Request, consensus.NodeID](
		dstsim.SimulatorOptions{Seed: seed},
	)

	handler := &consensusHandler{sim: sim, statusSeqMap: make(map[consensus.NodeID]int64)}
	sim.SetRequestHandler(handler)

	// Register orch nodes, each with a deterministically seeded RNG so tests are
	// reproducible yet the two orchs have different jitter values.
	sim.RegisterNode(consensus.NewOrchNode(orchA, rand.New(rand.NewPCG(uint64(seed), 0))))
	sim.RegisterNode(consensus.NewOrchNode(orchB, rand.New(rand.NewPCG(uint64(seed+1), 0))))

	// Register pooler nodes with in-memory storage
	stores := make(map[consensus.NodeID]*memStorage)
	for _, id := range []consensus.NodeID{pooler1, pooler2, pooler3} {
		store := &memStorage{}
		stores[id] = store
		sim.RegisterNode(consensus.NewPoolerNode(id, store, nil))
	}

	// Register the discovery node — it will detect poolers on its first tick
	// and emit PoolerMembershipRequests to inform the orchs.
	sim.RegisterNode(newDiscoveryNode(discoveryID, sim))

	return sim, stores
}

// --- Tests ---

// TestHappyPath_PrimaryElected verifies that under a reliable fast network,
// a primary is eventually appointed and the split-brain invariants are never violated.
func TestHappyPath_PrimaryElected(t *testing.T) {
	sim, _ := newHappyPathSim(t, 42)
	for _, inv := range standardInvariants() {
		sim.Always(inv)
	}

	h := dstsim.NewSimulationTestHelper(t, sim)
	h.RequireRunUntil(&activePrimaryExists{}, 200)
}
