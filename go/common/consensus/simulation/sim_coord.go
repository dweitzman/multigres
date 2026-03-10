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

// discoverySourceID is the from-node ID used when SimCoordNode enqueues
// PoolerDiscoveredIndicator and PoolerRemovedIndicator events into the
// simulator's delivery pipeline. Tests that configure chaos delivery managers
// should treat this source as reliable (no drops, delays, or duplicates) since
// coordinator membership convergence must not be subject to the same chaos as
// regular inter-node traffic. Use PerSourceDeliveryManager with a zero-chaos
// override for this ID to achieve that.
const discoverySourceID consensus.NodeID = "discovery"

// SimCoordNode wraps a CoordNode and simulates its etcd watch stream for
// pooler membership. Each tick it computes the desired pooler set from all
// SimPoolers registered in the simulator, diffs it against what was last
// emitted, and enqueues PoolerDiscoveredIndicator / PoolerRemovedIndicator
// directly into the simulator's delivery pipeline (bypassing the request
// handler). This makes membership events visible in the trace and subject to
// the configured delivery manager—but callers should configure a reliable
// delivery override for discoverySourceID so membership events are never
// dropped or reordered.
//
// On coordinator crash-restart, lastDesired is cleared so all currently
// registered poolers appear as discovered on the next tick, mirroring how a
// restarted coordinator re-reads the full etcd membership snapshot.
type SimCoordNode struct {
	node        *consensus.CoordNode
	sim         *simType
	lastDesired map[consensus.NodeID]bool // poolers included in the last membership emission
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

// Restart clears all ephemeral coordinator state and resets lastDesired so
// all currently registered poolers are re-discovered on the next tick.
// Implements dstsim.Restartable.
func (s *SimCoordNode) Restart() {
	s.node.Restart()
	s.lastDesired = nil
}

// Step computes the desired pooler membership from all SimPoolers registered
// in the simulator, diffs it against the previously emitted set, and enqueues
// PoolerDiscoveredIndicator / PoolerRemovedIndicator into the simulator's
// delivery pipeline for any changes. Implements dstsim.Node.
func (s *SimCoordNode) Step(tick int64, inds []consensus.Indicator) []consensus.Request {
	// Compute desired set from all SimPoolers registered in the simulator.
	desired := make(map[consensus.NodeID]bool)
	for _, n := range s.sim.Nodes() {
		if sp, ok := n.(*SimPooler); ok {
			desired[sp.ID()] = true
		}
	}

	// Enqueue membership changes into the delivery pipeline (visible in trace).
	for _, id := range sortedmaps.Keys(desired) {
		if !s.lastDesired[id] {
			s.sim.EnqueueDirect(discoverySourceID, s.node.ID(), consensus.PoolerDiscoveredIndicator{PoolerID: id})
		}
	}
	for _, id := range sortedmaps.Keys(s.lastDesired) {
		if !desired[id] {
			s.sim.EnqueueDirect(discoverySourceID, s.node.ID(), consensus.PoolerRemovedIndicator{PoolerID: id})
		}
	}
	s.lastDesired = desired

	return s.node.Step(tick, inds)
}
