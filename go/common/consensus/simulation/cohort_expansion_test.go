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
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// TestCohortExpansion verifies the normal-path cohort expansion protocol: a
// coordinator incrementally adds observer replicas to the cohort and upgrades
// the durability policy as the cluster grows, without coordinator elections.
//
// Setup: node1 is pre-initialized as a 1-node primary with AtLeast(1) rules.
// The coordinator targets AtLeast(3), upgrading the policy as the cohort grows.
//
// Stages:
//  1. node2 joins: coordinator expands cohort to [node1, node2], policy → AtLeast(2).
//  2. node3 joins: coordinator expands cohort to [node1, node2, node3], policy → AtLeast(3).
//  3. node4 joins: target policy is already satisfied with 3 nodes; cohort expands
//     to [node1..node4] but policy stays AtLeast(3).
func TestCohortExpansion(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
		node3ID consensus.NodeID = "node-3"
		node4ID consensus.NodeID = "node-4"
	)

	sim := newTestSim(coordID)

	// Pre-initialize node1 as a primary with a single-node bootstrap policy.
	seedRules := &consensus.DurabilityRules{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedRules, sim)
	sim.RegisterNode(pooler1)

	// Coordinator targets AtLeast(3) so it upgrades the durability policy as nodes join.
	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(3)), sim)
	sim.RegisterNode(coord)

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Stage 1: node2 joins as an observer streaming from node1.
	// Coordinator writes new rules adding node2 to the cohort.
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	th.RequireWithinTicks(&allHaveAppliedRules{
		poolers:     []*SimPooler{pooler1, pooler2},
		members:     []consensus.NodeID{node1ID, node2ID},
		wantAtLeast: 2,
	}, 200)

	// Stage 2: node3 joins; the coordinator upgrades the policy to AtLeast(3).
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler3)
	th.RequireWithinTicks(&allHaveAppliedRules{
		poolers:     []*SimPooler{pooler1, pooler2, pooler3},
		members:     []consensus.NodeID{node1ID, node2ID, node3ID},
		wantAtLeast: 3,
	}, 200)

	// Stage 3: node4 joins; AtLeast(3) is already achievable with 3 nodes so the
	// policy stays AtLeast(3) while the cohort grows to include node4.
	pooler4 := newReplicaPooler(node4ID, node1ID, sim)
	sim.RegisterNode(pooler4)
	th.RequireWithinTicks(&allHaveAppliedRules{
		poolers:     []*SimPooler{pooler1, pooler2, pooler3, pooler4},
		members:     []consensus.NodeID{node1ID, node2ID, node3ID, node4ID},
		wantAtLeast: 3,
	}, 200)
}
