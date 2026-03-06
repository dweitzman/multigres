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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// --- Helpers ---

// anyNThreshold returns the ack threshold of an AnyN policy via a structural
// interface cast. Returns (0, false) if the policy does not expose AckThreshold.
func anyNThreshold(p consensus.DurabilityPolicy) (int, bool) {
	type ackThresholder interface {
		AckThreshold() int
	}
	a, ok := p.(ackThresholder)
	if !ok {
		return 0, false
	}
	return a.AckThreshold(), true
}

// simPoolerByID looks up a SimPooler by node ID in the simulator.
func simPoolerByID(sim *simType, id consensus.NodeID) *SimPooler {
	for _, node := range sim.Nodes() {
		if sp, ok := node.(*SimPooler); ok && sp.ID() == id {
			return sp
		}
	}
	return nil
}

// --- Conditions ---

// allHaveAppliedPolicy is true when every pooler in the set satisfies all of:
//
//  1. Committed policy has exactly the expected cohort members (any order).
//  2. Committed policy is an AnyN(wantAnyN) policy.
//  3. For the primary: syncStandbys equals cohort minus self, and syncPolicy is AnyN(wantAnyN).
//  4. For replicas: primaryConnInfo matches the committed primary.
//
// This checks not only that the policy was written and persisted, but that the
// corresponding replication settings were applied consistently.
type allHaveAppliedPolicy struct {
	poolers  []*SimPooler
	members  []consensus.NodeID
	wantAnyN int
}

func (c *allHaveAppliedPolicy) Name() string {
	return fmt.Sprintf("all_have_applied_policy{cohort=%v anyN=%d}", c.members, c.wantAnyN)
}

func (c *allHaveAppliedPolicy) Eval(_ *simType) bool {
	for _, sp := range c.poolers {
		state := sp.Node().CommittedState()
		policy := state.Policy
		if policy == nil || len(policy.CohortMembers) != len(c.members) {
			return false
		}
		for _, id := range c.members {
			if !slices.Contains(policy.CohortMembers, id) {
				return false
			}
		}
		n, ok := anyNThreshold(policy.Policy)
		if !ok || n != c.wantAnyN {
			return false
		}

		switch state.Role {
		case consensus.RolePrimary:
			// Sync standbys must be exactly the cohort minus this node.
			expectedStandbys := make([]consensus.NodeID, 0, len(c.members)-1)
			for _, id := range c.members {
				if id != sp.ID() {
					expectedStandbys = append(expectedStandbys, id)
				}
			}
			gotStandbys := slices.Clone(sp.syncStandbys)
			slices.Sort(gotStandbys)
			slices.Sort(expectedStandbys)
			if !slices.Equal(gotStandbys, expectedStandbys) {
				return false
			}
			sn, sok := anyNThreshold(sp.syncPolicy)
			if !sok || sn != c.wantAnyN {
				return false
			}
		case consensus.RoleReplica:
			// primaryConnInfo must match the committed primary.
			if sp.primaryConnInfo != state.Primary {
				return false
			}
		}
	}
	return true
}

func (c *allHaveAppliedPolicy) Describe(_ *simType) string {
	var lines []string
	for _, sp := range c.poolers {
		state := sp.Node().CommittedState()
		policy := state.Policy
		var policyStr string
		if policy == nil {
			policyStr = "no policy"
		} else {
			policyStr = fmt.Sprintf("cohort=%v policy=%T", policy.CohortMembers, policy.Policy)
		}
		replicationStr := fmt.Sprintf("primaryConnInfo=%v syncStandbys=%v syncPolicy=%T",
			sp.primaryConnInfo, sp.syncStandbys, sp.syncPolicy)
		lines = append(lines, fmt.Sprintf("  %v(role=%v): %v | %v",
			sp.ID(), state.Role, policyStr, replicationStr))
	}
	return "pooler state:\n" + strings.Join(lines, "\n")
}

// syncStandbysAreReplicas is a safety invariant: every node in the primary's sync
// standbys list must be configured to replicate from that primary (primaryConnInfo
// equals the primary's ID). A violation means the primary could deadlock waiting
// for ACKs from a node that is not streaming WAL from it.
type syncStandbysAreReplicas struct{}

func (c *syncStandbysAreReplicas) Name() string { return "sync_standbys_are_replicas" }

func (c *syncStandbysAreReplicas) Eval(sim *simType) bool {
	for _, node := range sim.Nodes() {
		sp, ok := node.(*SimPooler)
		if !ok || sp.Node().CommittedState().Role != consensus.RolePrimary {
			continue
		}
		for _, standbyID := range sp.syncStandbys {
			standby := simPoolerByID(sim, standbyID)
			if standby == nil || standby.primaryConnInfo != sp.ID() {
				return false
			}
		}
	}
	return true
}

func (c *syncStandbysAreReplicas) Describe(sim *simType) string {
	var lines []string
	for _, node := range sim.Nodes() {
		sp, ok := node.(*SimPooler)
		if !ok {
			continue
		}
		state := sp.Node().CommittedState()
		lines = append(lines, fmt.Sprintf("  %v: role=%v syncStandbys=%v primaryConnInfo=%v",
			sp.ID(), state.Role, sp.syncStandbys, sp.primaryConnInfo))
	}
	return "node sync state:\n" + strings.Join(lines, "\n")
}

// --- Test helpers ---

// newPrimaryPooler creates a SimPooler pre-initialized as the cluster primary.
// seedPolicy is written to storage before NewPoolerNode loads it, so the node
// starts in the primary role with the given policy already committed.
func newPrimaryPooler(id consensus.NodeID, seedPolicy *consensus.DurabilityPolicyRecord, sim *simType) *SimPooler {
	store := &MemStorage{}
	store.state = consensus.PoolerPersistentState{
		Role:    consensus.RolePrimary,
		Primary: id,
		Policy:  seedPolicy,
	}
	return NewSimPooler(consensus.NewPoolerNode(id, store), sim)
}

// newReplicaPooler creates a SimPooler pre-initialized as a replica streaming from
// primaryID. It broadcasts its state on the first tick so the coordinator discovers
// it as an observer on the same tick it learns about its existence.
func newReplicaPooler(id, primaryID consensus.NodeID, sim *simType) *SimPooler {
	store := &MemStorage{}
	store.state = consensus.PoolerPersistentState{
		Role:    consensus.RoleReplica,
		Primary: primaryID,
	}
	return NewSimPooler(consensus.NewPoolerNode(id, store), sim)
}

// --- Tests ---

// TestCohortExpansion verifies the normal-path cohort expansion protocol: a
// coordinator incrementally adds observer replicas to the cohort and upgrades
// the durability policy as the cluster grows, without coordinator elections.
//
// Setup: node1 is pre-initialized as a 1-node primary with AnyN(0) policy.
// The coordinator targets AnyN(2), upgrading the policy as the cohort grows.
//
// Stages:
//  1. 1-node cluster: verify stability before expansion begins.
//  2. node2 joins: coordinator expands cohort to [node1, node2], policy → AnyN(1).
//  3. node3 joins: coordinator expands cohort to [node1, node2, node3], policy → AnyN(2).
//  4. node4 joins: target policy is already satisfied with 3 nodes; cohort expands
//     to [node1..node4] but policy stays AnyN(2).
func TestCohortExpansion(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
		node3ID consensus.NodeID = "node-3"
		node4ID consensus.NodeID = "node-4"
	)

	sim := dstsim.NewSimulator[consensus.Indicator, consensus.Request, consensus.NodeID](
		dstsim.SimulatorOptions{Seed: 42},
	)
	sim.SetRequestHandler(NewHandler(coordID))

	// Pre-initialize node1 as a primary with a single-node bootstrap policy.
	seedPolicy := &consensus.DurabilityPolicyRecord{
		ID:            "v0",
		CohortMembers: []consensus.NodeID{node1ID},
		Policy:        consensus.AnyNPolicy(0),
	}
	pooler1 := newPrimaryPooler(node1ID, seedPolicy, sim)
	sim.RegisterNode(pooler1)

	// Coordinator targets AnyN(2) so it upgrades the durability policy as nodes join.
	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AnyNPolicy(2)), sim)
	sim.RegisterNode(coord)

	// Safety invariant: every sync standby must be actively streaming from the primary.
	// Violated if the primary requires ACKs from a node that isn't replicating from it.
	sim.Always(&syncStandbysAreReplicas{})

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Stage 1: single-node cluster — verify stability before expansion begins.
	th.RequireAdvance(10)

	// Stage 2: node2 joins as an observer streaming from node1.
	// Coordinator writes a new policy adding node2 to the cohort.
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	th.RequireWithinTicks(&allHaveAppliedPolicy{
		poolers:  []*SimPooler{pooler1, pooler2},
		members:  []consensus.NodeID{node1ID, node2ID},
		wantAnyN: 1,
	}, 200)

	// Stage 3: node3 joins; the coordinator upgrades the policy to AnyN(2).
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler3)
	th.RequireWithinTicks(&allHaveAppliedPolicy{
		poolers:  []*SimPooler{pooler1, pooler2, pooler3},
		members:  []consensus.NodeID{node1ID, node2ID, node3ID},
		wantAnyN: 2,
	}, 200)

	// Stage 4: node4 joins; AnyN(2) is already achievable with 3 nodes so the
	// policy stays AnyN(2) while the cohort grows to include node4.
	pooler4 := newReplicaPooler(node4ID, node1ID, sim)
	sim.RegisterNode(pooler4)
	th.RequireWithinTicks(&allHaveAppliedPolicy{
		poolers:  []*SimPooler{pooler1, pooler2, pooler3, pooler4},
		members:  []consensus.NodeID{node1ID, node2ID, node3ID, node4ID},
		wantAnyN: 2,
	}, 200)
}
