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
// diffs against each orch's KnownPoolerIDs(), and emits a targeted PoolerMembershipRequest
// for any gaps.
//
// By reading each orch's actual known state rather than tracking a separate last-known
// set, the discoveryNode naturally handles orch crash-restarts: when Restart() clears an
// orch's knownPoolers, the discoveryNode sees all current poolers as newly discovered
// and sends them on the next tick — through the normal ordered delivery path (with delay),
// not as an instantaneous blast. Duplicate discover/remove events are harmless because
// OrchNode handles them idempotently.
//
// In production, the equivalent logic watches an etcd prefix and buffers membership
// changes for delivery to OrchNode on the next Step() call. Here the simulator's
// Nodes() method serves as the etcd state source.
//
// The node's ID is discoveryNodeID; it should be registered in the simulator but will
// never receive any indicators (all messages flow outward).
type discoveryNode struct {
	id  consensus.NodeID
	sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]
}

func newDiscoveryNode(
	id consensus.NodeID,
	sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
) *discoveryNode {
	return &discoveryNode{id: id, sim: sim}
}

func (d *discoveryNode) ID() consensus.NodeID { return d.id }

func (d *discoveryNode) Step(tick int64, indicators []consensus.Indicator) []consensus.Request {
	// Build current pooler set by inspecting the simulator.
	current := make(map[consensus.NodeID]bool)
	for _, node := range d.sim.Nodes() {
		if _, ok := node.(*consensus.PoolerNode); ok {
			current[node.ID()] = true
		}
	}

	// For each orch, diff the orch's current knowledge against the actual pooler set
	// and emit a targeted PoolerMembershipRequest for any gaps. Using a separate request
	// per orch ensures each gets its own delivery scheduling (independent delay per the
	// network policy), modeling independent etcd watch streams.
	var requests []consensus.Request
	for _, node := range d.sim.Nodes() {
		orch, ok := node.(*consensus.OrchNode)
		if !ok {
			continue
		}

		knownByOrch := make(map[consensus.NodeID]bool)
		for _, id := range orch.KnownPoolerIDs() {
			knownByOrch[id] = true
		}

		var discovered, removed []consensus.NodeID
		for _, id := range sortedmaps.Keys(current) {
			if !knownByOrch[id] {
				discovered = append(discovered, id)
			}
		}
		for _, id := range sortedmaps.Keys(knownByOrch) {
			if !current[id] {
				removed = append(removed, id)
			}
		}

		if len(discovered) == 0 && len(removed) == 0 {
			continue
		}
		requests = append(requests, consensus.PoolerMembershipRequest{
			TargetOrch: orch.ID(),
			Discovered: discovered,
			Removed:    removed,
		})
	}

	return requests
}
