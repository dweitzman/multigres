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
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// --- Delivery manager helpers ---

// reliableMembership wraps inner with a PerSourceDeliveryManager that routes
// discoverySourceID traffic through a zero-chaos (reliable) delivery manager.
// All other traffic goes through inner unchanged.
//
// Use this whenever applying a chaos delivery manager to the simulation: it
// ensures that coordinator membership events (PoolerDiscoveredIndicator /
// PoolerRemovedIndicator) are never dropped, delayed, or duplicated regardless
// of the chaos settings in inner, keeping membership convergence decoupled from
// network chaos.
func reliableMembership(inner dstsim.IndicatorDeliveryManager[consensus.Indicator, consensus.NodeID]) dstsim.IndicatorDeliveryManager[consensus.Indicator, consensus.NodeID] {
	return &dstsim.PerSourceDeliveryManager[consensus.Indicator, consensus.NodeID]{
		Default: inner,
		Overrides: map[consensus.NodeID]dstsim.IndicatorDeliveryManager[consensus.Indicator, consensus.NodeID]{
			discoverySourceID: &dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{},
		},
	}
}

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
// seedTerm is written to storage before NewPoolerNode loads it, so the node
// starts in the primary role with the given rules already committed.
func newPrimaryPooler(id consensus.NodeID, seedTerm *consensus.Term, sim *simType) *SimPooler {
	store := &MemStorage{}
	store.state = consensus.PoolerPersistentState{
		Role:       consensus.RolePrimary,
		Primary:    id,
		CachedTerm: seedTerm,
	}
	return NewSimPooler(consensus.NewPoolerNode(id, store, consensus.NodeProperties{}), sim, seedTerm)
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
	return NewSimPooler(consensus.NewPoolerNode(id, store, consensus.NodeProperties{}), sim, nil)
}

// --- Helpers ---

// atLeastThreshold returns the AtLeast threshold of an AtLeastPolicy via a
// structural interface cast. Returns (0, false) if the policy does not expose
// AtLeastThreshold.
func atLeastThreshold(p consensus.DurabilityPolicy) (int, bool) {
	type atLeastThresholder interface {
		AtLeastThreshold() int
	}
	a, ok := p.(atLeastThresholder)
	if !ok {
		return 0, false
	}
	return a.AtLeastThreshold(), true
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

// --- Generic value-tracking conditions ---

// valueNeverDecreases is a safety invariant: the value returned by getValue
// must be monotonically non-decreasing across ticks. Tracks the maximum ever
// seen; returns false if the current value drops below that maximum.
// Register with sim.Always to assert this holds for the entire simulation.
type valueNeverDecreases[T cmp.Ordered] struct {
	name     string
	getValue func(*simType) T
	max      T
	hasMax   bool
}

func (c *valueNeverDecreases[T]) Name() string { return c.name }

func (c *valueNeverDecreases[T]) Eval(sim *simType) bool {
	v := c.getValue(sim)
	if c.hasMax && v < c.max {
		return false // dropped below previously seen maximum
	}
	if !c.hasMax || v > c.max {
		c.max = v
		c.hasMax = true
	}
	return true
}

func (c *valueNeverDecreases[T]) Describe(sim *simType) string {
	return fmt.Sprintf("%s: max_seen=%v, current=%v", c.name, c.max, c.getValue(sim))
}

// valueNewMax fires whenever getValue(sim) strictly exceeds the maximum value
// seen so far. Drops below the previous maximum (e.g. a coordinator view
// resetting to 0 after a crash) are ignored. Use with dstsim.NewAtLeastNTimes
// to count genuine upward progress events rather than oscillations.
type valueNewMax[T cmp.Ordered] struct {
	name     string
	getValue func(*simType) T
	max      T
	hasMax   bool
}

func (c *valueNewMax[T]) Name() string { return c.name }

func (c *valueNewMax[T]) Eval(sim *simType) bool {
	v := c.getValue(sim)
	if !c.hasMax {
		c.max = v
		c.hasMax = true
		return false // first evaluation: establish baseline, don't fire
	}
	if v > c.max {
		c.max = v
		return true
	}
	return false
}

func (c *valueNewMax[T]) Describe(_ *simType) string {
	return fmt.Sprintf("%s: max_seen=%v", c.name, c.max)
}

// --- Conditions ---

// allHaveAppliedRules is true when every pooler in the set satisfies all of:
//
//  1. Committed rules have exactly the expected cohort members (any order).
//  2. Committed rules use an AtLeast(wantAtLeast) policy.
//  3. For the primary: syncStandbys equals cohort minus self, and syncPolicy is AtLeast(wantAtLeast).
//  4. For replicas: primaryConnInfo matches the committed primary.
//
// This checks not only that the rules were written and persisted, but that the
// corresponding replication settings were applied consistently.
type allHaveAppliedRules struct {
	poolers     []*SimPooler
	members     []consensus.NodeID
	wantAtLeast int
}

func (c *allHaveAppliedRules) Name() string {
	return fmt.Sprintf("all_have_applied_rules{cohort=%v atLeast=%d}", c.members, c.wantAtLeast)
}

func (c *allHaveAppliedRules) Eval(_ *simType) bool {
	for _, sp := range c.poolers {
		state := sp.Node().CommittedState()
		term := state.CachedTerm
		if term == nil || len(term.Members) != len(c.members) {
			return false
		}
		for _, wantID := range c.members {
			found := false
			for _, m := range term.Members {
				if m.ID == wantID {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		n, ok := atLeastThreshold(term.Policy)
		if !ok || n != c.wantAtLeast {
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
			sn, sok := atLeastThreshold(sp.gucSyncPolicy)
			if !sok || sn != c.wantAtLeast {
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
		term := state.CachedTerm
		var rulesStr string
		if term == nil {
			rulesStr = "no rules"
		} else {
			memberIDs := make([]consensus.NodeID, len(term.Members))
			for i, m := range term.Members {
				memberIDs[i] = m.ID
			}
			rulesStr = fmt.Sprintf("seq=%d cohort=%v policy=%T", term.Seq, memberIDs, term.Policy)
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
