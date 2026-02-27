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
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// memStorage is an in-memory PoolerStorage implementation for tests.
// It simulates durable storage: Save writes to the struct field and Load reads
// it back, so a simulated crash-restart (which calls Restart() → storage.Load())
// correctly restores the last saved state just as reading from disk would.
type memStorage struct {
	state consensus.PoolerPersistentState
}

func (s *memStorage) Save(state consensus.PoolerPersistentState) error {
	s.state = state
	return nil
}

func (s *memStorage) Load() (consensus.PoolerPersistentState, error) {
	return s.state, nil
}

// flakyApplier is a RoleApplier that fails at a configurable rate, simulating
// a slow or unreliable postgres apply (e.g. pg_ctl promote taking multiple ticks).
// It tracks how many times Apply returned false so tests can assert that the
// failure scenario actually occurred (not that we passed trivially with no failures).
//
// lastApplied mirrors the postgres GUC files in production: it holds the last state
// for which Apply() returned true and persists across simulated crash-restarts
// (the flakyApplier instance itself is not restarted, only the PoolerNode is).
type flakyApplier struct {
	rng         *rand.Rand
	failRate    float64 // probability (0.0–1.0) of returning false each tick
	failures    int     // number of times Apply returned false
	lastApplied consensus.PoolerPersistentState
	hasApplied  bool
}

var _ consensus.RoleApplier = (*flakyApplier)(nil)

func (a *flakyApplier) Apply(state consensus.PoolerPersistentState) bool {
	if a.rng.Float64() < a.failRate {
		a.failures++
		return false
	}
	a.lastApplied = state
	a.hasApplied = true
	return true
}

// AppliedState returns the last state successfully applied, analogous to reading
// postgresql.conf / standby.signal from disk in production. It persists across
// PoolerNode.Restart() calls because the applier instance is not restarted.
func (a *flakyApplier) AppliedState() (consensus.PoolerPersistentState, bool) {
	return a.lastApplied, a.hasApplied
}

// consensusHandler converts consensus Requests into Indicators and routes them.
// It also handles PoolerMembershipRequest from the discovery node by broadcasting
// PoolerDiscoveredIndicator / PoolerRemovedIndicator to all registered OrchNodes.
type consensusHandler struct {
	sim          *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]
	statusSeqMap map[consensus.NodeID]int64 // monotonically increasing per pooler
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
		case consensus.PoolerStatusUpdateRequest:
			h.statusSeqMap[fromNode]++
			seq := h.statusSeqMap[fromNode]
			for _, oid := range h.orchIDs() {
				result[oid] = append(result[oid], consensus.PoolerStatusIndicator{
					PoolerID:       fromNode,
					StatusSeq:      seq,
					State:          r.State,
					Applied:        r.Applied,
					PostgresStatus: r.PostgresStatus,
					LastApplied:    r.LastApplied,
				})
			}
		case consensus.TerminateRequest:
			result[r.Target] = append(result[r.Target], consensus.TerminateIndicator{})
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

// --- Standard invariants ---
//
// standardInvariants returns the invariants that should hold in every simulation.
// Register them with sim.Always() at the start of each test.
func standardInvariants() []dstsim.Condition[consensus.Indicator, consensus.Request, consensus.NodeID] {
	return []dstsim.Condition[consensus.Indicator, consensus.Request, consensus.NodeID]{
		&atMostOneQuorum{},
		&appliedMonotonicity{},
	}
}

// atMostOneQuorum is the core safety invariant: at most one primary may hold a
// write quorum at any given time.
//
// A primary P holds a write quorum when at least one pooler in P's SyncReplicas
// set has its effective state pointing to P — i.e. postgres on that replica is
// currently configured to replicate from P (primary_conninfo = P).
//
// "Effective state" is what postgres is actually running with on disk right now:
// the last-applied state when a committed change is pending, or the committed
// state when Applied=true. This is the correct check because:
//   - A replica only ACKs writes to the primary named in its effective state.
//   - Two committed primaries is normal during re-election; what matters is
//     whether two primaries can simultaneously get ACKs.
//
// Note: it is valid for two nodes to have committed.Role=Primary simultaneously
// (e.g. a stale primary that hasn't received the revoke yet), as long as only one
// of them has replicas effectively streaming to it.
//
// TODO: Consider strengthening this invariant by also checking whether applying any
// pooler's pending committed state (when it differs from the effective state) would
// create a second quorum. If transitioning any node to its committed state immediately
// would yield two simultaneous primaries with quorum, the protocol has made a mistake
// that will become visible soon — detecting it early produces clearer failures.
type atMostOneQuorum struct{}

func (c *atMostOneQuorum) Name() string { return "at_most_one_quorum" }

func (c *atMostOneQuorum) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	type poolerInfo struct {
		id        consensus.NodeID
		effective consensus.PoolerPersistentState
	}
	var poolers []poolerInfo
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			poolers = append(poolers, poolerInfo{id: p.ID(), effective: p.EffectiveState()})
		}
	}

	quorumCount := 0
	for _, pi := range poolers {
		if pi.effective.Role != consensus.RolePrimary {
			continue
		}
		// Count how many of this primary's required sync replicas are effectively
		// streaming to it (i.e. have primary_conninfo pointing at this node).
		streaming := 0
		for _, srID := range pi.effective.SyncReplicas {
			for _, rp := range poolers {
				if rp.id == srID && rp.effective.Primary == pi.id {
					streaming++
					break
				}
			}
		}
		if streaming >= 1 { // syncReplicaQuorum = 1
			quorumCount++
		}
	}
	return quorumCount <= 1
}

func (c *atMostOneQuorum) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	var lines []string
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			eff := p.EffectiveState()
			committed := p.CommittedState()
			lines = append(lines, fmt.Sprintf("%v: effective(role=%v primary=%v syncReplicas=%v) committed(role=%v applied=%v)",
				node.ID(), eff.Role, eff.Primary, eff.SyncReplicas, committed.Role, committed.Applied))
		}
	}
	return "effective states:\n  " + strings.Join(lines, "\n  ")
}

// appliedMonotonicity is a safety invariant: once a pooler persists Applied=true
// for a given proposal (identified by VotedTerm+VotedSeqNum), Applied must never
// revert to false for that same proposal. Applied may only be false on a new
// proposal (higher term or higher seqnum within the same term).
type appliedMonotonicity struct {
	prev map[consensus.NodeID]consensus.PoolerPersistentState
}

func (c *appliedMonotonicity) Name() string { return "applied_monotonicity" }

func (c *appliedMonotonicity) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	if c.prev == nil {
		c.prev = make(map[consensus.NodeID]consensus.PoolerPersistentState)
	}
	for _, node := range sim.Nodes() {
		p, ok := node.(*consensus.PoolerNode)
		if !ok {
			continue
		}
		curr := p.CommittedState()
		prev, hasPrev := c.prev[p.ID()]
		if hasPrev && prev.Applied && !curr.Applied {
			// Applied reverted — only acceptable if the proposal advanced.
			advanced := curr.VotedTerm > prev.VotedTerm ||
				(curr.VotedTerm == prev.VotedTerm && curr.VotedSeqNum > prev.VotedSeqNum)
			if !advanced {
				return false
			}
		}
		c.prev[p.ID()] = curr
	}
	return true
}

func (c *appliedMonotonicity) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	var violations []string
	for _, node := range sim.Nodes() {
		p, ok := node.(*consensus.PoolerNode)
		if !ok {
			continue
		}
		curr := p.CommittedState()
		prev := c.prev[p.ID()]
		if prev.Applied && !curr.Applied {
			violations = append(violations, fmt.Sprintf(
				"node %v: Applied reverted without proposal advance (term=%d seq=%d → term=%d seq=%d)",
				p.ID(), prev.VotedTerm, prev.VotedSeqNum, curr.VotedTerm, curr.VotedSeqNum,
			))
		}
	}
	return fmt.Sprintf("applied monotonicity violations: %v", violations)
}

// --- Liveness conditions ---

// activePrimaryExists is true when any PoolerNode is an active primary:
// committed to the primary role, applied, and postgres is running.
type activePrimaryExists struct{}

func (c *activePrimaryExists) Name() string { return "active_primary_exists" }
func (c *activePrimaryExists) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok && p.IsActivePrimary() {
			return true
		}
	}
	return false
}

func (c *activePrimaryExists) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			s := p.CommittedState()
			return fmt.Sprintf("node %v role=%v applied=%v postgres=%v",
				node.ID(), s.Role, s.Applied, p.PostgresStatus())
		}
	}
	return "no poolers"
}

// noPrimaryActive is the complement of activePrimaryExists: true when no pooler is
// currently operating as an active primary. It is used as the "problem" phase in
// crash→recovery cycle detection with InOrder/AtLeastNTimes.
type noPrimaryActive struct{}

func (c *noPrimaryActive) Name() string { return "no_primary_active" }
func (c *noPrimaryActive) Eval(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	return !(&activePrimaryExists{}).Eval(sim)
}

func (c *noPrimaryActive) Describe(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	var states []string
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok {
			s := p.CommittedState()
			states = append(states, fmt.Sprintf("%v(role=%v applied=%v postgres=%v)",
				node.ID(), s.Role, s.Applied, p.PostgresStatus()))
		}
	}
	return fmt.Sprintf("no active primary; poolers: %v", states)
}

// minTotalApplyFailures is true when the total flakyApplier failure count across all
// poolers reaches at least min. Use it to assert the retry path was actually exercised.
type minTotalApplyFailures struct {
	appliers map[consensus.NodeID]*flakyApplier
	min      int
}

func (c *minTotalApplyFailures) Name() string {
	return fmt.Sprintf("min_apply_failures_%d", c.min)
}

func (c *minTotalApplyFailures) Eval(_ *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) bool {
	total := 0
	for _, a := range c.appliers {
		total += a.failures
	}
	return total >= c.min
}

func (c *minTotalApplyFailures) Describe(_ *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) string {
	total := 0
	for _, a := range c.appliers {
		total += a.failures
	}
	return fmt.Sprintf("total apply failures: %d (need >= %d)", total, c.min)
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

	handler := &consensusHandler{sim: sim, statusSeqMap: make(map[consensus.NodeID]int64)}
	sim.SetRequestHandler(handler)

	// Register orch nodes, each with a deterministically seeded RNG so tests are
	// reproducible yet the two orchs have different jitter values.
	sim.RegisterNode(consensus.NewOrchNode(orchA, rand.New(rand.NewPCG(uint64(seed), 0))))
	sim.RegisterNode(consensus.NewOrchNode(orchB, rand.New(rand.NewPCG(uint64(seed+1), 0))))

	// Register pooler nodes with in-memory storage
	stores := make(map[consensus.NodeID]*memStorage)
	for _, id := range []consensus.NodeID{pooler1, pooler2, pooler3} {
		store := &memStorage{}
		stores[id] = store
		sim.RegisterNode(consensus.NewPoolerNode(id, store, nil))
	}

	// Register the discovery node — it will detect poolers on its first tick
	// and emit PoolerMembershipRequests to inform the orchs.
	sim.RegisterNode(newDiscoveryNode(discoveryID, sim))

	return sim, stores
}

// --- Tests ---

// newFlakyApplierSim creates a simulator identical to newHappyPathSim except
// each pooler is given a flakyApplier at failRate (0.0–1.0) per tick.
// The returned appliers map lets callers inspect failure counts after the test.
func newFlakyApplierSim(t *testing.T, seed int64, failRate float64) (
	*dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
	map[consensus.NodeID]*memStorage,
	map[consensus.NodeID]*flakyApplier,
) {
	t.Helper()
	sim := dstsim.NewSimulator[consensus.Indicator, consensus.Request, consensus.NodeID](
		dstsim.SimulatorOptions{Seed: seed},
	)

	handler := &consensusHandler{sim: sim, statusSeqMap: make(map[consensus.NodeID]int64)}
	sim.SetRequestHandler(handler)

	sim.RegisterNode(consensus.NewOrchNode(orchA, rand.New(rand.NewPCG(uint64(seed), 0))))
	sim.RegisterNode(consensus.NewOrchNode(orchB, rand.New(rand.NewPCG(uint64(seed+1), 0))))

	stores := make(map[consensus.NodeID]*memStorage)
	appliers := make(map[consensus.NodeID]*flakyApplier)
	for i, id := range []consensus.NodeID{pooler1, pooler2, pooler3} {
		store := &memStorage{}
		stores[id] = store
		applier := &flakyApplier{
			rng:      rand.New(rand.NewPCG(uint64(seed+int64(i)+10), 0)),
			failRate: failRate,
		}
		appliers[id] = applier
		sim.RegisterNode(consensus.NewPoolerNode(id, store, applier))
	}

	sim.RegisterNode(newDiscoveryNode(discoveryID, sim))
	return sim, stores, appliers
}

// activePrimaryID returns the NodeID of the current active primary, or "" if none.
func activePrimaryID(sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]) consensus.NodeID {
	for _, node := range sim.Nodes() {
		if p, ok := node.(*consensus.PoolerNode); ok && p.IsActivePrimary() {
			return p.ID()
		}
	}
	return ""
}

// --- Crash driver ---

const crashDriverID consensus.NodeID = "crash-driver"

// crashPhase is the crash driver's state machine.
type crashPhase int

const (
	crashPhaseIdle        crashPhase = iota // waiting for an active primary to crash
	crashPhaseTerminating                   // TerminateRequest sent; stopping node next tick
	crashPhaseDown                          // node stopped; counting down to restart
)

// crashDriverNode is a simulation test node that autonomously drives the primary
// crash+restart cycle. On each iteration it:
//  1. Finds the active primary and sends a TerminateRequest, so the orch receives a
//     PostgresStopped status — triggering re-election and term advancement.
//  2. Stops the primary node one tick later, after the TerminateIndicator has been
//     delivered and processed (pooler reports postgresStatus=Stopped to all orchs).
//  3. Restarts it after downTicks ticks.
//
// Pair with RequireRunUntil(NewAtLeastNTimes(N, NewInOrder(noPrimaryActive, activePrimaryExists)))
// to drive N crash→recovery cycles.
type crashDriverNode struct {
	id         consensus.NodeID
	sim        *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID]
	phase      crashPhase
	target     consensus.NodeID
	resumeTick int64
	downTicks  int64
}

func newCrashDriverNode(
	id consensus.NodeID,
	sim *dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
	downTicks int64,
) *crashDriverNode {
	return &crashDriverNode{id: id, sim: sim, downTicks: downTicks}
}

func (n *crashDriverNode) ID() consensus.NodeID { return n.id }

func (n *crashDriverNode) Step(tick int64, _ []consensus.Indicator) []consensus.Request {
	switch n.phase {
	case crashPhaseIdle:
		if primaryID := activePrimaryID(n.sim); primaryID != "" {
			n.target = primaryID
			n.phase = crashPhaseTerminating
			return []consensus.Request{consensus.TerminateRequest{Target: primaryID}}
		}
	case crashPhaseTerminating:
		// TerminateIndicator was scheduled last tick and delivered this tick. Stop the node
		// now; StopNode is deferred to end of tick (after all nodes have stepped), so the
		// pooler processes the TerminateIndicator before being stopped.
		n.sim.StopNode(n.target)
		n.resumeTick = tick + n.downTicks
		n.phase = crashPhaseDown
	case crashPhaseDown:
		if tick >= n.resumeTick {
			n.sim.RestartNode(n.target)
			n.phase = crashPhaseIdle
		}
	}
	return nil
}

// newCrashDriverSim creates a simulator identical to newHappyPathSim with an
// autonomous crashDriverNode registered. The driver continuously crashes and
// restarts the active primary, driving crash→recovery cycles for crash tests.
func newCrashDriverSim(t *testing.T, seed int64) (
	*dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
	map[consensus.NodeID]*memStorage,
) {
	t.Helper()
	sim, stores := newHappyPathSim(t, seed)
	sim.RegisterNode(newCrashDriverNode(crashDriverID, sim, 5))
	return sim, stores
}

// newFlakyApplierCrashSim creates a simulator with flaky appliers and a crash driver.
func newFlakyApplierCrashSim(t *testing.T, seed int64, failRate float64) (
	*dstsim.Simulator[consensus.Indicator, consensus.Request, consensus.NodeID],
	map[consensus.NodeID]*memStorage,
	map[consensus.NodeID]*flakyApplier,
) {
	t.Helper()
	sim, stores, appliers := newFlakyApplierSim(t, seed, failRate)
	sim.RegisterNode(newCrashDriverNode(crashDriverID, sim, 5))
	return sim, stores, appliers
}

// --- Tests ---

// TestHappyPath_PrimaryElected verifies that under a reliable fast network,
// a primary is eventually appointed and the split-brain invariants are never violated.
func TestHappyPath_PrimaryElected(t *testing.T) {
	sim, _ := newHappyPathSim(t, 42)
	for _, inv := range standardInvariants() {
		sim.Always(inv)
	}

	h := dstsim.NewSimulationTestHelper(t, sim)
	h.RequireRunUntil(&activePrimaryExists{}, 200)
}

// TestFlakyApply_1000Failovers verifies that the cluster survives at least 1000
// crash→recovery cycles when apply operations fail intermittently (50% per tick).
// The crashDriverNode drives the crashes autonomously. All assertions are expressed
// as conditions passed to RequireRunUntil, which dumps recent trace on failure:
//   - 1000 complete noPrimaryActive→activePrimaryExists cycles
//   - at least 1000 distinct voting terms exercised (one per re-election)
//   - at least 1 apply failure observed (confirming the flaky retry path was hit)
func TestFlakyApply_1000Failovers(t *testing.T) {
	sim, _, appliers := newFlakyApplierCrashSim(t, 42, 0.5)
	for _, inv := range standardInvariants() {
		sim.Always(inv)
	}
	h := dstsim.NewSimulationTestHelper(t, sim)
	h.RequireRunUntil(
		dstsim.And(
			dstsim.NewAtLeastNTimes(1000, dstsim.NewInOrder(&noPrimaryActive{}, &activePrimaryExists{})),
			&minTotalApplyFailures{appliers: appliers, min: 1},
		),
		200_000,
	)
}

// TestPrimaryPooler_1000Crashes verifies that at least 1000 primary crash-restarts
// never violate safety invariants and that the cluster always recovers to an active
// primary. This exercises the full crash recovery path: Restart() clears in-memory
// state and restores committed state from durable storage. The crashDriverNode drives
// the crashes autonomously; all assertions are conditions passed to RequireRunUntil.
func TestPrimaryPooler_1000Crashes(t *testing.T) {
	sim, _ := newCrashDriverSim(t, 42)
	for _, inv := range standardInvariants() {
		sim.Always(inv)
	}
	h := dstsim.NewSimulationTestHelper(t, sim)
	h.RequireRunUntil(
		dstsim.NewAtLeastNTimes(1000, dstsim.NewInOrder(&noPrimaryActive{}, &activePrimaryExists{})),
		200_000,
	)
}
