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
	"bytes"
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
)

// TestTraceMethod tests that Trace() returns simulation events organized by tick
func TestTraceMethod(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	node := &TickingNode{id: 1}
	sim.RegisterNode(node)
	sim.SetRequestHandler(&TickRequestHandler{})

	// Run for a few ticks
	err := sim.RunFor(5)
	require.NoError(t, err)

	// Get trace
	trace := sim.Trace()

	// Verify trace is not empty
	require.NotEmpty(t, trace, "trace should contain events")

	// Verify trace is organized by tick with increasing tick numbers
	var lastTick int64 = -1
	for _, tickTrace := range trace {
		require.GreaterOrEqual(t, tickTrace.Tick, lastTick, "trace ticks should be non-decreasing")
		lastTick = tickTrace.Tick

		// Each tick trace should have NodeSteps
		require.NotEmpty(t, tickTrace.NodeSteps, "tick trace should have node activity")
	}
}

// TestTraceStructure tests that trace is organized by tick with node steps
func TestTraceStructure(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 42})

	// Create a node that generates multiple requests
	node := &TickingNode{id: 1}
	sim.RegisterNode(node)

	// Handler that sends multiple indicators back
	handler := &multiIndicatorHandler{indicatorsPerRequest: 3}
	sim.SetRequestHandler(handler)

	// Run for just 3 ticks
	err := sim.RunFor(3)
	require.NoError(t, err)

	trace := sim.Trace()

	// Verify trace is organized by tick
	require.NotEmpty(t, trace, "trace should not be empty")

	// Each TickTrace should have exactly one entry per tick
	ticksSeen := make(map[int64]bool)
	for _, tickTrace := range trace {
		require.False(t, ticksSeen[tickTrace.Tick], "should only have one TickTrace per tick")
		ticksSeen[tickTrace.Tick] = true

		// Verify NodeSteps contains node activity
		require.Contains(t, tickTrace.NodeSteps, 1, "should have activity for node 1")

		nodeStep := tickTrace.NodeSteps[1]

		// If node received multiple indicators, they should all be in the Indicators slice
		if len(nodeStep.Indicators) > 1 {
			t.Logf("Tick %d, Node 1: received %d indicators in one step (correct!)",
				tickTrace.Tick, len(nodeStep.Indicators))
		}

		// Node should have generated requests
		require.NotEmpty(t, nodeStep.Requests, "node should have generated requests")
	}
}

// TestTraceRetention tests that trace doesn't grow unbounded
func TestTraceRetention(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{
		Seed:           42,
		TraceRetention: 5, // Keep only 5 ticks
	})

	node := &AlwaysActiveNode{id: 1}
	sim.RegisterNode(node)
	sim.SetRequestHandler(&TickRequestHandler{})

	// Run for many more ticks than retention limit
	err := sim.RunFor(100)
	require.NoError(t, err)

	trace := sim.Trace()

	// With deterministic behavior and retention=5, we keep exactly the last 5 ticks
	require.Equal(t, 5, len(trace), "trace should contain exactly 5 ticks (retention limit)")
}

// TestDumpRecentTrace tests the DumpRecentTrace helper
func TestDumpRecentTrace(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{
		Seed:           42,
		TraceRetention: 10, // Keep only 10 ticks
	})

	node := &AlwaysActiveNode{id: 1}
	sim.RegisterNode(node)
	sim.SetRequestHandler(&TickRequestHandler{})

	// Run for more ticks than retention
	err := sim.RunFor(20)
	require.NoError(t, err)

	// Dump to buffer
	var buf bytes.Buffer
	sim.DumpRecentTrace(&buf, 5) // Dump last 5 ticks

	output := buf.String()

	// Verify output contains expected structure
	require.Contains(t, output, "Recent Trace", "should have trace header")
	require.Contains(t, output, "Tick", "should show tick numbers")
	require.Contains(t, output, "End of Trace", "should have trace footer")

	// Count how many tick sections appear - with deterministic behavior we get exactly 5
	tickCount := len(regexp.MustCompile(`--- Tick \d+ ---`).FindAllString(output, -1))
	require.Equal(t, 5, tickCount, "should dump exactly 5 ticks")
}

// AlwaysActiveNode generates a request on every tick, regardless of indicators
type AlwaysActiveNode struct {
	id int
}

func (n *AlwaysActiveNode) ID() int { return n.id }
func (n *AlwaysActiveNode) Step(tick int64, indicators []int) []string {
	// Always generate a request to ensure trace activity
	return []string{"tick"}
}

// TestDumpRecentTraceFormatting tests the detailed formatting of dumped traces
func TestDumpRecentTraceFormatting(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{
		Seed:           42,
		TraceRetention: 10,
	})

	// Create a node that always generates activity
	node := &AlwaysActiveNode{id: 1}
	sim.RegisterNode(node)

	// Handler that sends indicators back
	sim.SetRequestHandler(&TickRequestHandler{})

	// Run for exactly 3 ticks
	err := sim.RunFor(3)
	require.NoError(t, err)

	// Get the last 2 tick numbers for assertion
	trace := sim.Trace()
	require.Equal(t, 3, len(trace), "should have exactly 3 ticks in trace (ran for 3 ticks)")
	tick1 := trace[1].Tick
	tick2 := trace[2].Tick

	// Dump the last 2 ticks
	var buf bytes.Buffer
	sim.DumpRecentTrace(&buf, 2)

	output := buf.String()

	// Assert exact expected output with actual tick numbers
	expected := fmt.Sprintf(`
=== Recent Trace (last 2 ticks, %d-%d) ===

--- Tick %d ---
Node activity:
  1 received 1 indicators
    <- int 1
  1 generated 1 requests
    -> string tick

--- Tick %d ---
Node activity:
  1 received 1 indicators
    <- int 1
  1 generated 1 requests
    -> string tick

=== End of Trace ===
`, tick1, tick2, tick1, tick2)

	require.Equal(t, expected, output, "trace output should match expected format exactly")
}
