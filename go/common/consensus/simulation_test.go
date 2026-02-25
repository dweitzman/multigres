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
	sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]
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

// --- Conditions ---

// primaryExists is true when any PoolerNode reports role=Primary.
type primaryExists struct{}

func (c *primaryExists) Name() string { return "primary_exists" }
func (c *primaryExists) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			if p.CommittedState().Role == consensus.RolePrimary {
				return true
			}
		}
	}
	return false
}

func (c *primaryExists) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			s := p.CommittedState()
			return fmt.Sprintf("node %v role=%v primaryTerm=%d", node.ID(), s.Role, s.PrimaryTerm)
		}
	}
	return "no poolers"
}

// atMostOnePrimary is an invariant: no two PoolerNodes may simultaneously have role=Primary.
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
	return fmt.Sprintf("primaries: %v", primaries)
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

	handler := &consensusHandler{sim: sim}
	sim.SetRequestHandler(handler)

	// Register orch nodes
	sim.RegisterNode(consensus.NewOrchNode(orchA))
	sim.RegisterNode(consensus.NewOrchNode(orchB))

	// Register pooler nodes with in-memory storage
	stores := make(map[consensus.NodeID]*memStorage)
	for _, id := range []consensus.NodeID{pooler1, pooler2, pooler3} {
		store := &memStorage{}
		stores[id] = store
		sim.RegisterNode(consensus.NewPoolerNode(id, store))
	}

	// Register the discovery node — it will detect poolers on its first tick
	// and emit PoolerMembershipRequests to inform the orchs.
	sim.RegisterNode(newDiscoveryNode(discoveryID, sim))

	return sim, stores
}

// --- Tests ---

// TestHappyPath_PrimaryElected verifies that under a reliable fast network,
// a primary is eventually appointed and the split-brain invariant is never violated.
func TestHappyPath_PrimaryElected(t *testing.T) {
	sim, _ := newHappyPathSim(t, 42)
	sim.Always(&atMostOnePrimary{})

	h := dstsim.NewSimulationTestHelper(t, sim)
	h.RequireRunUntil(&primaryExists{}, 200)
}
