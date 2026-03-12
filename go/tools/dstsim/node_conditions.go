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

package dstsim

import "fmt"

// NodeCondition is an optionally-stateful predicate evaluated against a single
// node of concrete type N. A separate instance is created per node by
// PerNodeCondition, so implementations may safely carry per-node state across
// ticks (e.g., tracking whether a node was stopped last tick).
type NodeCondition[N any, I any, R any, ID comparable] interface {
	EvalNode(node N, sim *Simulator[I, R, ID]) bool
}

// NodeConditionFunc adapts a plain function to NodeCondition for stateless predicates.
type NodeConditionFunc[N any, I any, R any, ID comparable] func(node N, sim *Simulator[I, R, ID]) bool

func (f NodeConditionFunc[N, I, R, ID]) EvalNode(node N, sim *Simulator[I, R, ID]) bool {
	return f(node, sim)
}

// justStopped is a stateful NodeCondition that fires on the tick a node
// transitions from running to stopped (edge detection: false→true).
type justStopped[N Node[I, R, ID], I any, R any, ID comparable] struct {
	prevStopped bool
}

func (j *justStopped[N, I, R, ID]) EvalNode(node N, sim *Simulator[I, R, ID]) bool {
	currStopped := sim.IsNodeStopped(node.ID())
	fired := currStopped && !j.prevStopped
	j.prevStopped = currStopped
	return fired
}

// JustStopped returns a new NodeCondition that fires on the single tick a node
// transitions from running to stopped. A fresh instance must be created per node
// (the factory pattern in PerNodeCondition guarantees this).
func JustStopped[N Node[I, R, ID], I any, R any, ID comparable]() NodeCondition[N, I, R, ID] {
	return &justStopped[N, I, R, ID]{}
}

// nodeAnd is a NodeCondition that is true when all sub-conditions are true.
type nodeAnd[N any, I any, R any, ID comparable] struct {
	conds []NodeCondition[N, I, R, ID]
}

func (c *nodeAnd[N, I, R, ID]) EvalNode(node N, sim *Simulator[I, R, ID]) bool {
	for _, cond := range c.conds {
		if !cond.EvalNode(node, sim) {
			return false
		}
	}
	return true
}

// NodeAnd creates a NodeCondition that is true when all given conditions are true.
func NodeAnd[N any, I any, R any, ID comparable](conds ...NodeCondition[N, I, R, ID]) NodeCondition[N, I, R, ID] {
	return &nodeAnd[N, I, R, ID]{conds: conds}
}

// nodeNot is a NodeCondition that negates another.
type nodeNot[N any, I any, R any, ID comparable] struct {
	cond NodeCondition[N, I, R, ID]
}

func (c *nodeNot[N, I, R, ID]) EvalNode(node N, sim *Simulator[I, R, ID]) bool {
	return !c.cond.EvalNode(node, sim)
}

// NodeNot creates a NodeCondition that negates the given condition.
func NodeNot[N any, I any, R any, ID comparable](cond NodeCondition[N, I, R, ID]) NodeCondition[N, I, R, ID] {
	return &nodeNot[N, I, R, ID]{cond: cond}
}

// perNodeCondition is a top-level Condition that iterates over all simulator
// nodes of type N, maintaining one NodeCondition instance per node. It returns
// true if any node's condition fires on the current tick. All nodes are always
// evaluated (not short-circuited) so that stateful sub-conditions such as
// JustStopped can update their state regardless of whether another node fired.
type perNodeCondition[N Node[I, R, ID], I any, R any, ID comparable] struct {
	newCond func() NodeCondition[N, I, R, ID]
	perNode map[ID]NodeCondition[N, I, R, ID]
}

func (c *perNodeCondition[N, I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	fired := false
	for _, node := range sim.Nodes() {
		n, ok := any(node).(N)
		if !ok {
			continue
		}
		id := node.ID()
		cond, exists := c.perNode[id]
		if !exists {
			cond = c.newCond()
			c.perNode[id] = cond
		}
		if cond.EvalNode(n, sim) {
			fired = true
		}
	}
	return fired
}

func (c *perNodeCondition[N, I, R, ID]) Name() string {
	return "per_node"
}

func (c *perNodeCondition[N, I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	matching := 0
	for _, node := range sim.Nodes() {
		if _, ok := any(node).(N); ok {
			matching++
		}
	}
	return fmt.Sprintf("per-node condition (%d/%d matching nodes tracked)", len(c.perNode), matching)
}

// nodeIsRunning is a Condition that is true when a specific node (identified by
// ID) is not stopped in the simulator.
type nodeIsRunning[I any, R any, ID comparable] struct {
	id ID
}

func (c *nodeIsRunning[I, R, ID]) Name() string {
	return fmt.Sprintf("node_is_running(%v)", c.id)
}

func (c *nodeIsRunning[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	return !sim.IsNodeStopped(c.id)
}

func (c *nodeIsRunning[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	return fmt.Sprintf("node %v running=%v", c.id, !sim.IsNodeStopped(c.id))
}

// NodeIsRunning returns a Condition that is true when the node with the given ID
// is not stopped in the simulator. Combine with AtLeastNTicks to check whether
// a node has been continuously running for a minimum number of ticks:
//
//	dstsim.AtLeastNTicks(300, dstsim.NodeIsRunning[...](nodeID))
func NodeIsRunning[I any, R any, ID comparable](id ID) Condition[I, R, ID] {
	return &nodeIsRunning[I, R, ID]{id: id}
}

// PerNodeCondition returns a Condition that evaluates a per-node condition against
// every node of type N in the simulator. It returns true if any node's condition fires.
//
// newCond is called once when each node is first encountered; the returned instance
// is reused on every subsequent tick so that stateful sub-conditions (e.g., JustStopped)
// accumulate state correctly per node.
//
// Example — count orch crashes that occur while mid-appointment:
//
//	NewAtLeastNTimes(1000, PerNodeCondition(func() NodeCondition[*OrchNode, ...] {
//	    return NodeAnd(
//	        JustStopped[*OrchNode, ...](),
//	        NodeConditionFunc[*OrchNode, ...](func(o *OrchNode, _ *Simulator[...]) bool {
//	            return o.AppointmentStageComplete(PhaseBegin)
//	        }),
//	    )
//	}))
func PerNodeCondition[N Node[I, R, ID], I any, R any, ID comparable](
	newCond func() NodeCondition[N, I, R, ID],
) Condition[I, R, ID] {
	return &perNodeCondition[N, I, R, ID]{
		newCond: newCond,
		perNode: make(map[ID]NodeCondition[N, I, R, ID]),
	}
}
