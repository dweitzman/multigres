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

package simulation

import (
	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// SimCoordNode wraps a CoordNode and simulates its etcd watch stream for
// pooler membership. Each tick it discovers all SimPoolers registered in the
// simulator and reconciles the coordinator's known-pooler list against that
// set, injecting PoolerDiscoveredIndicator and PoolerRemovedIndicator events
// until the coordinator's view converges.
type SimCoordNode struct {
	node *consensus.CoordNode
	sim  *simType
}

// NewSimCoordNode creates a SimCoordNode wrapping the given CoordNode.
// sim is used each tick to discover registered SimPoolers.
func NewSimCoordNode(node *consensus.CoordNode, sim *simType) *SimCoordNode {
	return &SimCoordNode{
		node: node,
		sim:  sim,
	}
}

// Node returns the wrapped CoordNode.
func (s *SimCoordNode) Node() *consensus.CoordNode {
	return s.node
}

// ID returns the coordinator's unique identifier.
func (s *SimCoordNode) ID() consensus.NodeID {
	return s.node.ID()
}

// Step discovers all SimPoolers registered in the simulator, reconciles that
// set against the coordinator's known-pooler list, injects any necessary
// membership indicators, then calls CoordNode.Step. Implements dstsim.Node.
func (s *SimCoordNode) Step(tick int64, inds []consensus.Indicator) []consensus.Request {
	// Build desired set from all SimPoolers registered in the simulator.
	desired := make(map[consensus.NodeID]bool)
	for _, n := range s.sim.Nodes() {
		if sp, ok := n.(*SimPooler); ok {
			desired[sp.ID()] = true
		}
	}

	known := make(map[consensus.NodeID]bool)
	for _, id := range s.node.KnownPoolerIDs() {
		known[id] = true
	}

	var membershipInds []consensus.Indicator
	for _, id := range sortedmaps.Keys(desired) {
		if !known[id] {
			membershipInds = append(membershipInds, consensus.PoolerDiscoveredIndicator{PoolerID: id})
		}
	}
	for _, id := range sortedmaps.Keys(known) {
		if !desired[id] {
			membershipInds = append(membershipInds, consensus.PoolerRemovedIndicator{PoolerID: id})
		}
	}

	allInds := append(membershipInds, inds...)
	return s.node.Step(tick, allInds)
}
