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
// pooler membership. Each tick it reconciles the coordinator's known-pooler
// list against a desired membership set, injecting PoolerDiscoveredIndicator
// and PoolerRemovedIndicator events until the coordinator's view converges.
//
// This mirrors how each real coordinator maintains an independent etcd watch
// stream: different SimCoordNodes can discover poolers at different ticks,
// simulating realistic watch stream latency between coordinators.
type SimCoordNode struct {
	node    *consensus.CoordNode
	desired map[consensus.NodeID]bool
}

// NewSimCoordNode creates a SimCoordNode wrapping the given CoordNode.
func NewSimCoordNode(node *consensus.CoordNode) *SimCoordNode {
	return &SimCoordNode{
		node:    node,
		desired: make(map[consensus.NodeID]bool),
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

// AddPooler adds poolerID to the desired membership set. The coordinator will
// receive PoolerDiscoveredIndicator for this pooler on the next Step call (and
// on every subsequent tick until its known-pooler list reflects it).
func (s *SimCoordNode) AddPooler(id consensus.NodeID) {
	s.desired[id] = true
}

// RemovePooler removes poolerID from the desired membership set. The
// coordinator will receive PoolerRemovedIndicator until its known list no
// longer includes it.
func (s *SimCoordNode) RemovePooler(id consensus.NodeID) {
	delete(s.desired, id)
}

// Step reconciles the coordinator's known-pooler list against the desired set,
// injects any necessary membership indicators, then calls CoordNode.Step.
// Implements dstsim.Node.
func (s *SimCoordNode) Step(tick int64, inds []consensus.Indicator) []consensus.Request {
	// Compute membership indicators from the difference between desired and known.
	known := make(map[consensus.NodeID]bool)
	for _, id := range s.node.KnownPoolerIDs() {
		known[id] = true
	}

	var membershipInds []consensus.Indicator
	for _, id := range sortedmaps.Keys(s.desired) {
		if !known[id] {
			membershipInds = append(membershipInds, consensus.PoolerDiscoveredIndicator{PoolerID: id})
		}
	}
	for _, id := range sortedmaps.Keys(known) {
		if !s.desired[id] {
			membershipInds = append(membershipInds, consensus.PoolerRemovedIndicator{PoolerID: id})
		}
	}

	allInds := append(membershipInds, inds...)
	return s.node.Step(tick, allInds)
}
