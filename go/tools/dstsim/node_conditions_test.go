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

package dstsim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
)

// counterCond is the concrete NodeCondition type for *CounterNode simulations,
// used as a type alias to avoid repeating the four type parameters throughout tests.
type counterCond = dstsim.NodeCondition[*CounterNode, int, string, int]

func counterJustStopped() counterCond {
	return dstsim.JustStopped[*CounterNode, int, string, int]()
}

func counterPredicate(f func(*CounterNode) bool) counterCond {
	return dstsim.NodeConditionFunc[*CounterNode, int, string, int](
		func(n *CounterNode, _ *dstsim.Simulator[int, string, int]) bool {
			return f(n)
		},
	)
}

func newCounterSim(n int) *dstsim.Simulator[int, string, int] {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{})
	for i := range n {
		sim.RegisterNode(&CounterNode{id: i})
	}
	return sim
}

// TestJustStopped_FiresOnTransition verifies JustStopped fires on exactly the
// tick a node moves from running to stopped, and not before or after.
func TestJustStopped_FiresOnTransition(t *testing.T) {
	sim := newCounterSim(1)
	cond := dstsim.PerNodeCondition(func() counterCond { return counterJustStopped() })

	// Initial eval: node is running, no prior state → no transition.
	require.False(t, cond.Eval(sim))

	// Stop the node: transition running→stopped.
	sim.StopNode(0)
	require.True(t, cond.Eval(sim), "should fire on stop transition")

	// Node remains stopped: no new transition.
	require.False(t, cond.Eval(sim), "should not fire while remaining stopped")
}

// TestJustStopped_DoesNotFireOnResume verifies that resuming a stopped node
// does not trigger JustStopped.
func TestJustStopped_DoesNotFireOnResume(t *testing.T) {
	sim := newCounterSim(1)
	cond := dstsim.PerNodeCondition(func() counterCond { return counterJustStopped() })

	cond.Eval(sim) // init

	sim.StopNode(0)
	require.True(t, cond.Eval(sim)) // fires on stop

	sim.ResumeNode(0)
	require.False(t, cond.Eval(sim), "resume should not trigger JustStopped")
}

// TestPerNodeCondition_IndependentStatePerNode verifies that each node gets its
// own JustStopped instance — stopping one node does not affect the others.
func TestPerNodeCondition_IndependentStatePerNode(t *testing.T) {
	sim := newCounterSim(2) // nodes 0 and 1
	cond := dstsim.PerNodeCondition(func() counterCond { return counterJustStopped() })

	cond.Eval(sim) // init

	// Stop only node 0.
	sim.StopNode(0)
	require.True(t, cond.Eval(sim), "should fire for node 0")
	require.False(t, cond.Eval(sim), "should not fire again (node 0 remains stopped)")

	// Stop node 1 later — its JustStopped state is independent.
	sim.StopNode(1)
	require.True(t, cond.Eval(sim), "should fire for node 1")
	require.False(t, cond.Eval(sim), "should not fire again")
}

// TestNodeAnd_AllMustBeTrue verifies NodeAnd only returns true when all sub-conditions hold.
func TestNodeAnd_AllMustBeTrue(t *testing.T) {
	sim := newCounterSim(1)

	alwaysTrue := counterPredicate(func(*CounterNode) bool { return true })
	alwaysFalse := counterPredicate(func(*CounterNode) bool { return false })

	trueTrue := dstsim.PerNodeCondition(func() counterCond { return dstsim.NodeAnd(alwaysTrue, alwaysTrue) })
	trueFalse := dstsim.PerNodeCondition(func() counterCond { return dstsim.NodeAnd(alwaysTrue, alwaysFalse) })

	require.True(t, trueTrue.Eval(sim))
	require.False(t, trueFalse.Eval(sim))
}

// TestNodeNot_Inverts verifies NodeNot negates its sub-condition.
func TestNodeNot_Inverts(t *testing.T) {
	sim := newCounterSim(1)

	alwaysTrue := counterPredicate(func(*CounterNode) bool { return true })
	alwaysFalse := counterPredicate(func(*CounterNode) bool { return false })

	notTrue := dstsim.PerNodeCondition(func() counterCond { return dstsim.NodeNot(alwaysTrue) })
	notFalse := dstsim.PerNodeCondition(func() counterCond { return dstsim.NodeNot(alwaysFalse) })

	require.False(t, notTrue.Eval(sim))
	require.True(t, notFalse.Eval(sim))
}

// TestPerNodeCondition_TypeFiltering verifies that only nodes of type N are matched;
// other node types in the simulator are silently ignored.
func TestPerNodeCondition_TypeFiltering(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{})
	sim.RegisterNode(&CounterNode{id: 1})
	sim.RegisterNode(&TickingNode{id: 2}) // different concrete type

	calls := 0
	cond := dstsim.PerNodeCondition(func() counterCond {
		return counterPredicate(func(*CounterNode) bool {
			calls++
			return true
		})
	})

	cond.Eval(sim)
	require.Equal(t, 1, calls, "predicate should be called exactly once (for CounterNode only)")
}

// TestAtLeastNTimes_WithPerNodeCondition is an integration test that combines
// PerNodeCondition, JustStopped, and AtLeastNTimes to count individual stop events.
func TestAtLeastNTimes_WithPerNodeCondition(t *testing.T) {
	sim := newCounterSim(1)

	stopCount := dstsim.NewAtLeastNTimes(3,
		dstsim.PerNodeCondition(func() counterCond { return counterJustStopped() }),
	)

	stopCount.Eval(sim) // init

	for i := range 3 {
		require.False(t, stopCount.Eval(sim), "not yet satisfied before stop %d", i+1)
		sim.StopNode(0)
		stopCount.Eval(sim) // stop tick
		sim.ResumeNode(0)
		stopCount.Eval(sim) // resume tick (should not fire)
	}

	require.True(t, stopCount.Eval(sim), "should be satisfied after 3 stop events")
}
