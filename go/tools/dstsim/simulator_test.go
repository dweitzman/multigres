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

// TestRunUntil tests the RunUntil method with various conditions
func TestRunUntil(t *testing.T) {
	tests := []struct {
		name           string
		stopCondition  func(*dstsim.Simulator[int, string, int]) dstsim.Condition[int, string, int]
		maxTicks       int64
		expectError    bool
		expectErrorMsg string
	}{
		{
			name: "condition becomes true before maxTicks",
			stopCondition: func(sim *dstsim.Simulator[int, string, int]) dstsim.Condition[int, string, int] {
				return &CounterGreaterThan{nodeID: 1, value: 5}
			},
			maxTicks:    100,
			expectError: false,
		},
		{
			name: "maxTicks reached without condition true",
			stopCondition: func(sim *dstsim.Simulator[int, string, int]) dstsim.Condition[int, string, int] {
				return &CounterGreaterThan{nodeID: 1, value: 1000}
			},
			maxTicks:       20,
			expectError:    true,
			expectErrorMsg: "simulation reached max ticks",
		},
		{
			name: "condition true immediately",
			stopCondition: func(sim *dstsim.Simulator[int, string, int]) dstsim.Condition[int, string, int] {
				// Counter starts at 0, so <= 0 is immediately true
				return &CounterLessOrEqual{nodeID: 1, value: 0}
			},
			maxTicks:    100,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

			node := &CounterNode{id: 1}
			sim.RegisterNode(node)

			stopCond := tt.stopCondition(sim)
			err := sim.RunUntil(stopCond, tt.maxTicks)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectErrorMsg != "" {
					require.Contains(t, err.Error(), tt.expectErrorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestNodesMethod tests the Nodes() method returns registered nodes
func TestNodesMethod(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	// Initially no nodes
	require.Empty(t, sim.Nodes(), "should have no nodes initially")

	// Register nodes
	node1 := &CounterNode{id: 1}
	node2 := &CounterNode{id: 2}
	node3 := &CounterNode{id: 3}

	sim.RegisterNode(node1)
	sim.RegisterNode(node2)
	sim.RegisterNode(node3)

	// Verify all nodes returned
	nodes := sim.Nodes()
	require.Len(t, nodes, 3, "should have 3 nodes")

	// Verify node IDs (order doesn't matter)
	ids := make(map[int]bool)
	for _, node := range nodes {
		ids[node.ID()] = true
	}
	require.True(t, ids[1], "should contain node 1")
	require.True(t, ids[2], "should contain node 2")
	require.True(t, ids[3], "should contain node 3")
}
