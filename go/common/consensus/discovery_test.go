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
	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// discoveryNode simulates the etcd watcher that runs alongside each orch process.
// Each tick it reads the current set of PoolerNode instances from the simulator,
// diffs against its last-known set, and emits a PoolerMembershipRequest for any changes.
//
// In production, the equivalent logic watches an etcd prefix and buffers membership
// changes for delivery to OrchNode on the next Step() call. Here the simulator's
// Nodes() method serves as the etcd state source.
//
// The node's ID is discoveryNodeID; it should be registered in the simulator but will
// never receive any indicators (all messages flow outward).
type discoveryNode struct {
	id        consensus.NodeID
	sim       *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]
	lastKnown map[consensus.NodeID]bool // pooler IDs seen in the previous tick
}

func newDiscoveryNode(
	id consensus.NodeID,
	sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
) *discoveryNode {
	return &discoveryNode{
		id:        id,
		sim:       sim,
		lastKnown: make(map[consensus.NodeID]bool),
	}
}

func (d *discoveryNode) ID() consensus.NodeID { return d.id }

func (d *discoveryNode) Step(tick int64, indicators []consensus.Indicator) []consensus.Request {
	// Build current pooler set by inspecting the simulator
	current := make(map[consensus.NodeID]bool)
	for _, node := range d.sim.Nodes() {
		if _, ok := node.(*consensus.PoolerNode); ok {
			current[node.ID()] = true
		}
	}

	var discovered, removed []consensus.NodeID
	for _, id := range sortedmaps.Keys(current) {
		if !d.lastKnown[id] {
			discovered = append(discovered, id)
		}
	}
	for _, id := range sortedmaps.Keys(d.lastKnown) {
		if !current[id] {
			removed = append(removed, id)
		}
	}
	d.lastKnown = current

	if len(discovered) == 0 && len(removed) == 0 {
		return nil
	}
	return []consensus.Request{consensus.PoolerMembershipRequest{
		Discovered: discovered,
		Removed:    removed,
	}}
}
