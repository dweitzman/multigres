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
	"fmt"

	"github.com/multigres/multigres/go/tools/dstsim"
)

// Simple counter node for testing
type CounterNode struct {
	id    int
	count int
}

func (n *CounterNode) ID() int { return n.id }
func (n *CounterNode) Step(tick int64, indicators []int) []string {
	n.count++
	return nil
}

// TickingNode generates a self-message every tick to ensure message flow
// count tracks the number of indicators received, not steps
type TickingNode struct {
	id        int
	count     int
	firstStep bool
}

func (n *TickingNode) ID() int { return n.id }
func (n *TickingNode) Step(tick int64, indicators []int) []string {
	// Count delivered indicators (what the test actually wants to measure)
	n.count += len(indicators)

	// Only generate a request when we receive indicators (or on first step)
	// This prevents infinite feedback loops
	if len(indicators) > 0 || !n.firstStep {
		n.firstStep = true
		return []string{"tick"}
	}
	return nil
}

// TickRequestHandler processes tick requests by sending indicators back to the node
type TickRequestHandler struct{}

func (h *TickRequestHandler) ProcessRequests(sim *dstsim.Simulator[int, string, int], fromNode int, requests []string) map[int][]int {
	// Send an indicator back to the same node for each tick request
	result := make(map[int][]int)
	for range requests {
		result[fromNode] = append(result[fromNode], 1) // Send a simple indicator
	}
	return result
}

// Condition: counter equals specific value
type CounterEquals struct {
	nodeID int
	value  int
}

func (c *CounterEquals) Name() string { return fmt.Sprintf("counter_equals_%d", c.value) }
func (c *CounterEquals) Eval(sim *dstsim.Simulator[int, string, int]) bool {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return counter.count == c.value
		}
	}
	return false
}

func (c *CounterEquals) Describe(sim *dstsim.Simulator[int, string, int]) string {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return fmt.Sprintf("counter=%d, expected=%d", counter.count, c.value)
		}
	}
	return "node not found"
}

// Condition: counter greater than value
type CounterGreaterThan struct {
	nodeID int
	value  int
}

func (c *CounterGreaterThan) Name() string {
	return fmt.Sprintf("counter_greater_than_%d", c.value)
}

func (c *CounterGreaterThan) Eval(sim *dstsim.Simulator[int, string, int]) bool {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return counter.count > c.value
		}
	}
	return false
}

func (c *CounterGreaterThan) Describe(sim *dstsim.Simulator[int, string, int]) string {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return fmt.Sprintf("counter=%d, threshold=%d", counter.count, c.value)
		}
	}
	return "node not found"
}

// Condition: counter less than or equal to value
type CounterLessOrEqual struct {
	nodeID int
	value  int
}

func (c *CounterLessOrEqual) Name() string {
	return fmt.Sprintf("counter_less_or_equal_%d", c.value)
}

func (c *CounterLessOrEqual) Eval(sim *dstsim.Simulator[int, string, int]) bool {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return counter.count <= c.value
		}
	}
	return false
}

func (c *CounterLessOrEqual) Describe(sim *dstsim.Simulator[int, string, int]) string {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return fmt.Sprintf("counter=%d, max=%d", counter.count, c.value)
		}
	}
	return "node not found"
}

// multiIndicatorHandler sends multiple indicators per request
type multiIndicatorHandler struct {
	indicatorsPerRequest int
}

func (h *multiIndicatorHandler) ProcessRequests(sim *dstsim.Simulator[int, string, int], fromNode int, requests []string) map[int][]int {
	result := make(map[int][]int)
	for range requests {
		// Send multiple indicators for each request
		for i := 0; i < h.indicatorsPerRequest; i++ {
			result[fromNode] = append(result[fromNode], i)
		}
	}
	return result
}

// testTrackingPolicy tracks whether it was used
type testTrackingPolicy[I any, ID comparable] struct {
	usedPtr *bool
	delay   int64
}

func (p *testTrackingPolicy[I, ID]) ScheduleDelivery(args dstsim.DeliveryArgs[I, ID]) (bool, int64, []string) {
	*p.usedPtr = true
	return true, p.delay, nil
}

// testInvalidDelayPolicy returns invalid delay (0) to test panic
type testInvalidDelayPolicy[I any, ID comparable] struct{}

func (p *testInvalidDelayPolicy[I, ID]) ScheduleDelivery(args dstsim.DeliveryArgs[I, ID]) (bool, int64, []string) {
	return true, 0, nil // Invalid: delay must be >= 1
}
