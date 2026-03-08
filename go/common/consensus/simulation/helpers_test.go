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

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// --- Simulator factory ---

// newTestSim creates a Simulator pre-configured for consensus simulation tests:
//   - Seed 42 for deterministic randomness
//   - request routing via NewHandler(coordID)
//   - nonRevokedSyncStandbysAreReplicas registered as a safety invariant
//
// Register nodes on the returned simulator, then create a SimulationTestHelper.
func newTestSim(coordID consensus.NodeID) *simType {
	sim := dstsim.NewSimulator[consensus.Indicator, consensus.Request, consensus.NodeID](
		dstsim.SimulatorOptions{Seed: 42},
	)
	sim.SetRequestHandler(NewHandler(coordID))
	sim.Always(&nonRevokedSyncStandbysAreReplicas{})
	return sim
}

// --- Node factories ---

// newPrimaryPooler creates a SimPooler pre-initialized as the cluster primary.
// seedRules is written to storage before NewPoolerNode loads it, so the node
// starts in the primary role with the given rules already committed.
func newPrimaryPooler(id consensus.NodeID, seedRules *consensus.DurabilityRules, sim *simType) *SimPooler {
	store := &MemStorage{}
	store.state = consensus.PoolerPersistentState{
		Role:    consensus.RolePrimary,
		Primary: id,
		Rules:   seedRules,
	}
	return NewSimPooler(consensus.NewPoolerNode(id, store, consensus.NodeProperties{}), sim)
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
	return NewSimPooler(consensus.NewPoolerNode(id, store, consensus.NodeProperties{}), sim)
}

// --- Helpers ---

// anyNThreshold returns the ack threshold of an AnyN policy via a structural
// interface cast. Returns (0, false) if the policy does not expose AckThreshold.
func anyNThreshold(p consensus.AckPolicy) (int, bool) {
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

// allHaveAppliedRules is true when every pooler in the set satisfies all of:
//
//  1. Committed rules have exactly the expected cohort members (any order).
//  2. Committed rules use an AnyN(wantAnyN) policy.
//  3. For the primary: syncStandbys equals cohort minus self, and syncPolicy is AnyN(wantAnyN).
//  4. For replicas: primaryConnInfo matches the committed primary.
//
// This checks not only that the rules were written and persisted, but that the
// corresponding replication settings were applied consistently.
type allHaveAppliedRules struct {
	poolers  []*SimPooler
	members  []consensus.NodeID
	wantAnyN int
}

func (c *allHaveAppliedRules) Name() string {
	return fmt.Sprintf("all_have_applied_rules{cohort=%v anyN=%d}", c.members, c.wantAnyN)
}

func (c *allHaveAppliedRules) Eval(_ *simType) bool {
	for _, sp := range c.poolers {
		state := sp.Node().CommittedState()
		rules := state.Rules
		if rules == nil || len(rules.Members) != len(c.members) {
			return false
		}
		for _, wantID := range c.members {
			found := false
			for _, m := range rules.Members {
				if m.ID == wantID {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		n, ok := anyNThreshold(rules.Policy)
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
			gotStandbys := make([]consensus.NodeID, len(sp.gucSyncStandbys))
			for i, m := range sp.gucSyncStandbys {
				gotStandbys[i] = m.ID
			}
			slices.Sort(gotStandbys)
			slices.Sort(expectedStandbys)
			if !slices.Equal(gotStandbys, expectedStandbys) {
				return false
			}
			sn, sok := anyNThreshold(sp.gucSyncPolicy)
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

func (c *allHaveAppliedRules) Describe(_ *simType) string {
	var lines []string
	for _, sp := range c.poolers {
		state := sp.Node().CommittedState()
		rules := state.Rules
		var rulesStr string
		if rules == nil {
			rulesStr = "no rules"
		} else {
			memberIDs := make([]consensus.NodeID, len(rules.Members))
			for i, m := range rules.Members {
				memberIDs[i] = m.ID
			}
			rulesStr = fmt.Sprintf("seq=%d cohort=%v policy=%T", rules.Seq, memberIDs, rules.Policy)
		}
		standbyIDs := make([]consensus.NodeID, len(sp.gucSyncStandbys))
		for i, m := range sp.gucSyncStandbys {
			standbyIDs[i] = m.ID
		}
		replicationStr := fmt.Sprintf("primaryConnInfo=%v syncStandbys=%v syncPolicy=%T",
			sp.primaryConnInfo, standbyIDs, sp.gucSyncPolicy)
		lines = append(lines, fmt.Sprintf("  %v(role=%v): %v | %v",
			sp.ID(), state.Role, rulesStr, replicationStr))
	}
	return "pooler state:\n" + strings.Join(lines, "\n")
}

// nonRevokedSyncStandbysAreReplicas is a safety invariant: every non-revoked
// node in the primary's sync standbys list must be configured to replicate from
// that primary (primaryConnInfo equals the primary's ID). A violation means the
// primary could deadlock waiting for ACKs from a node that was never set up to
// stream WAL from it.
//
// Revoked nodes (isRevoked() == true) are excluded: they have intentionally
// stopped replicating as part of the coordinator-led recruitment protocol, so
// the primary will correctly stall on their ACKs until new rules are established.
type nonRevokedSyncStandbysAreReplicas struct{}

func (c *nonRevokedSyncStandbysAreReplicas) Name() string {
	return "non_revoked_sync_standbys_are_replicas"
}

func (c *nonRevokedSyncStandbysAreReplicas) Eval(sim *simType) bool {
	for _, node := range sim.Nodes() {
		sp, ok := node.(*SimPooler)
		if !ok || sp.Node().CommittedState().Role != consensus.RolePrimary {
			continue
		}
		for _, standby := range sp.gucSyncStandbys {
			standbyNode := simPoolerByID(sim, standby.ID)
			if standbyNode == nil || standbyNode.isRevoked() {
				continue
			}
			if standbyNode.primaryConnInfo != sp.ID() {
				return false
			}
		}
	}
	return true
}

func (c *nonRevokedSyncStandbysAreReplicas) Describe(sim *simType) string {
	var lines []string
	for _, node := range sim.Nodes() {
		sp, ok := node.(*SimPooler)
		if !ok {
			continue
		}
		state := sp.Node().CommittedState()
		standbyIDs := make([]consensus.NodeID, len(sp.gucSyncStandbys))
		for i, m := range sp.gucSyncStandbys {
			standbyIDs[i] = m.ID
		}
		lines = append(lines, fmt.Sprintf("  %v: role=%v revoked=%v syncStandbys=%v primaryConnInfo=%v",
			sp.ID(), state.Role, sp.isRevoked(), standbyIDs, sp.primaryConnInfo))
	}
	return "node sync state:\n" + strings.Join(lines, "\n")
}
