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

// This file contains test-only simulation infrastructure.
// Production code should only depend on node.go for the Node interface.
//
// Crash simulation is supported via StopNode/ResumeNode/RestartNode primitives.
// Nodes are never removed from the simulator; stopping is a state change.
// Stopped nodes do not receive Step() calls and indicators addressed to them
// are dropped. If a node implements the Restartable interface, RestartNode
// calls Restart() before resuming it, allowing it to clear ephemeral state.
//
// TODO: Review API for footguns and add factory methods where needed:
// - UntilPolicy requires manual Sim reference (easy to forget, nil panic)
// - PolicySequence construction is verbose
// - Consider adding sim.NewUntilPolicy(), sim.NewPolicySequence() factory methods

import (
	"fmt"
	"math/rand/v2"
)

// IndicatorDelivery represents an indicator to be delivered to a target node
// RequestHandler processes requests emitted by nodes
// It typically delivers requests to target nodes (with optional delays/transformations)
type RequestHandler[I any, R any, ID comparable] interface {
	// ProcessRequests is called after a node emits requests
	// Returns a map of target nodes to indicators that should be delivered
	ProcessRequests(sim *Simulator[I, R, ID], fromNode ID, requests []R) map[ID][]I
}

// Simulator executes a distributed protocol deterministically
type Simulator[I any, R any, ID comparable] struct {
	nodes       map[ID]Node[I, R, ID]
	currentTick int64
	rng         *rand.Rand
	assertions  []Assertion[I, R, ID]

	// Delivery policy for network simulation
	indicatorDeliveryPolicy IndicatorDeliveryPolicy[I, ID]

	// Pluggable handler for converting node requests to indicators
	requestHandler RequestHandler[I, R, ID]

	// Track which "Sometimes" conditions have been satisfied
	sometimesSatisfied map[string]bool

	// Track EventuallyAlways: when condition became true and if it stayed true
	eventuallyAlwaysBecameTrue map[string]int64 // tick when condition first became true
	eventuallyAlwaysViolated   map[string]bool  // whether condition became false after being true

	// Scheduled indicator deliveries (tick -> nodeID -> indicators)
	scheduledDeliveries map[int64]map[ID][]I

	// Trace buffer for storing tick traces
	traceBuffer *TraceBuffer[I, R, ID]

	// Current tick's trace (accumulated during tick processing)
	// Used for both trace storage and debug logging
	currentTickTrace *TickTrace[I, R, ID]

	// stoppedIDs tracks which registered nodes are currently stopped.
	// Stopped nodes remain in the nodes map but do not receive Step() calls,
	// and indicators addressed to them are dropped and recorded in the trace.
	// Use StopNode/ResumeNode/RestartNode to change a node's stopped state.
	stoppedIDs map[ID]bool

	// inTick is true while the step loop is running.
	// Lifecycle changes requested during a tick are collected in pendingChanges
	// and applied at the end of the tick to avoid mutating state mid-loop.
	inTick         bool
	pendingChanges []func()
}

// SimulatorOptions configures the simulator
type SimulatorOptions struct {
	Seed           int64 // For reproducible randomness
	TraceRetention int   // Maximum number of ticks to keep in trace (0 = unlimited, default = 1000)
}

// NewSimulator creates a new simulator
func NewSimulator[I, R any, ID comparable](opts SimulatorOptions) *Simulator[I, R, ID] {
	rng := rand.New(rand.NewPCG(uint64(opts.Seed), uint64(opts.Seed)))

	// Start at a random positive tick number to catch bugs that assume tick starts at 0
	initialTick := rng.Int64N(1000000)

	return &Simulator[I, R, ID]{
		nodes:                      make(map[ID]Node[I, R, ID]),
		currentTick:                initialTick,
		rng:                        rng,
		indicatorDeliveryPolicy:    &FastNetwork[I, ID]{}, // Default: fast network with 1-tick latency
		assertions:                 make([]Assertion[I, R, ID], 0),
		sometimesSatisfied:         make(map[string]bool),
		eventuallyAlwaysBecameTrue: make(map[string]int64),
		eventuallyAlwaysViolated:   make(map[string]bool),
		scheduledDeliveries:        make(map[int64]map[ID][]I),
		traceBuffer:                NewTraceBuffer[I, R, ID](opts.TraceRetention),
		stoppedIDs:                 make(map[ID]bool),
	}
}

// RegisterNode adds a node to the simulation
func (s *Simulator[I, R, ID]) RegisterNode(n Node[I, R, ID]) {
	s.nodes[n.ID()] = n
}

// SetDeliveryPolicy sets the indicator delivery policy for network simulation
func (s *Simulator[I, R, ID]) SetDeliveryPolicy(policy IndicatorDeliveryPolicy[I, ID]) {
	s.indicatorDeliveryPolicy = policy
}

// CurrentTick returns the current simulation tick
func (s *Simulator[I, R, ID]) CurrentTick() int64 {
	return s.currentTick
}

// SetRequestHandler sets the handler that will process node requests
func (s *Simulator[I, R, ID]) SetRequestHandler(handler RequestHandler[I, R, ID]) {
	s.requestHandler = handler
}

// scheduleIndicator schedules an indicator for delivery to a target node via the delivery policy
// The policy determines when/if the indicator is delivered (enforces minimum 1-tick delay)
// This is an internal method - external callers should use RequestHandler.ProcessRequests
func (s *Simulator[I, R, ID]) scheduleIndicator(fromNode ID, targetNode ID, ind I) error {
	// Use delivery policy to determine when/if to deliver
	delivered, delayTicks := s.indicatorDeliveryPolicy.ScheduleDelivery(s.currentTick, fromNode, targetNode, ind)

	// Message dropped by policy
	if !delivered {
		if s.currentTickTrace != nil {
			s.currentTickTrace.DroppedIndicators = append(s.currentTickTrace.DroppedIndicators, DroppedIndicator[I, ID]{
				TargetNode: targetNode,
				Indicator:  ind,
			})
		}
		return nil
	}

	// Enforce realistic latency: delay must be at least 1 tick
	// No exceptions - messages cannot be delivered in the same tick they're generated
	if delayTicks < 1 {
		return fmt.Errorf("IndicatorDeliveryPolicy returned invalid delay %d (must be >= 1) at tick %d", delayTicks, s.currentTick)
	}

	// Drop indicators addressed to stopped nodes — they can't receive messages.
	if s.stoppedIDs[targetNode] {
		if s.currentTickTrace != nil {
			s.currentTickTrace.DroppedIndicators = append(s.currentTickTrace.DroppedIndicators, DroppedIndicator[I, ID]{
				TargetNode: targetNode,
				Indicator:  ind,
			})
		}
		return nil
	}

	// Schedule indicator for delivery at future tick
	if _, exists := s.nodes[targetNode]; !exists {
		panic(fmt.Sprintf("cannot schedule indicator for non-existent node %v (check RequestHandler output)", targetNode))
	}

	deliverAt := s.currentTick + delayTicks
	if s.scheduledDeliveries[deliverAt] == nil {
		s.scheduledDeliveries[deliverAt] = make(map[ID][]I)
	}
	s.scheduledDeliveries[deliverAt][targetNode] = append(s.scheduledDeliveries[deliverAt][targetNode], ind)
	return nil
}

// RunUntil runs the simulation until the condition becomes true, or maxTicks have elapsed
// Returns an error if maxTicks is reached without the condition becoming true
// Set maxTicks to 0 for unlimited ticks
func (s *Simulator[I, R, ID]) RunUntil(stopCondition Condition[I, R, ID], maxTicks int64) error {
	// Compose the stop condition: user condition OR max ticks reached
	var finalCondition Condition[I, R, ID]
	if maxTicks > 0 {
		finalCondition = Or(stopCondition, TickCondition[I, R, ID](maxTicks))
	} else {
		finalCondition = stopCondition
	}

	for {
		// Check if we should stop before processing this tick
		if finalCondition.Eval(s) {
			// Determine if we stopped due to condition or timeout
			if maxTicks > 0 && !stopCondition.Eval(s) {
				// Timeout: max ticks reached but user condition not satisfied
				return fmt.Errorf("simulation reached max ticks (%d) without condition '%s' becoming true", maxTicks, stopCondition.Name())
			}
			return s.checkDeferredProperties()
		}
		// Initialize trace for this tick (used for both trace storage and debug logging)
		s.currentTickTrace = &TickTrace[I, R, ID]{
			Tick:                s.currentTick,
			NodeSteps:           make(map[ID]NodeStepTrace[I, R]),
			DroppedIndicators:   make([]DroppedIndicator[I, ID], 0),
			AssertionViolations: make([]string, 0),
		}

		// Get scheduled indicator deliveries for this tick (local variable - no simulator state)
		tickIndicators := s.scheduledDeliveries[s.currentTick]
		delete(s.scheduledDeliveries, s.currentTick)

		// Process all nodes for this tick by calling Step() once per non-stopped node.
		// All active nodes are called every tick (even with empty indicators) so they can process time-based logic.
		// An anonymous function with defer ensures inTick is always cleared, even on early return.
		// Snapshot the active node IDs before iterating so StopNode/ResumeNode/RestartNode calls from
		// within a RequestHandler (which are deferred) don't interfere with the current loop.
		if err := func() error {
			s.inTick = true
			defer func() { s.inTick = false }()

			nodeIDs := make([]ID, 0, len(s.nodes))
			for id := range s.nodes {
				nodeIDs = append(nodeIDs, id)
			}
			for _, nodeID := range nodeIDs {
				node, stillActive := s.nodes[nodeID]
				if !stillActive {
					continue
				}
				// Skip stopped nodes — they don't receive Step() calls.
				if s.stoppedIDs[nodeID] {
					continue
				}
				indicators := tickIndicators[nodeID] // nil slice if no indicators
				requests := node.Step(s.currentTick, indicators)

				// Record in trace - accumulate this node's activity
				if len(indicators) > 0 || len(requests) > 0 {
					s.currentTickTrace.NodeSteps[nodeID] = NodeStepTrace[I, R]{
						Indicators: indicators,
						Requests:   requests,
					}
				}

				// Process requests from the node - convert to indicators
				if s.requestHandler != nil && len(requests) > 0 {
					indicatorsToDeliver := s.requestHandler.ProcessRequests(s, nodeID, requests)
					for targetID, inds := range indicatorsToDeliver {
						for _, ind := range inds {
							if err := s.scheduleIndicator(nodeID, targetID, ind); err != nil {
								return err
							}
						}
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}

		// Apply any deferred node additions/removals before checking assertions,
		// so assertions see the updated node set.
		s.applyPendingNodeChanges()

		// Check assertions
		var firstViolation *AssertionViolation
		for _, assertion := range s.assertions {
			conditionHolds := assertion.Condition.Eval(s)

			switch assertion.Quantifier {
			case Always:
				// Condition must be true at every tick
				if !conditionHolds {
					violation := &AssertionViolation{
						ConditionName: assertion.Condition.Name(),
						Quantifier:    Always,
						Tick:          s.currentTick,
						Description:   assertion.Condition.Describe(s),
					}
					if s.currentTickTrace != nil && firstViolation == nil {
						s.currentTickTrace.AssertionViolations = append(s.currentTickTrace.AssertionViolations,
							fmt.Sprintf("Always(%s): %s", assertion.Condition.Name(), assertion.Condition.Describe(s)))
						firstViolation = violation
					} else if firstViolation == nil {
						return violation
					}
				}

			case Never:
				// Condition must never be true
				if conditionHolds {
					violation := &AssertionViolation{
						ConditionName: assertion.Condition.Name(),
						Quantifier:    Never,
						Tick:          s.currentTick,
						Description:   assertion.Condition.Describe(s),
					}
					if s.currentTickTrace != nil && firstViolation == nil {
						s.currentTickTrace.AssertionViolations = append(s.currentTickTrace.AssertionViolations,
							fmt.Sprintf("Never(%s): %s", assertion.Condition.Name(), assertion.Condition.Describe(s)))
						firstViolation = violation
					} else if firstViolation == nil {
						return violation
					}
				}

			case Sometimes:
				// Track if condition has been true at least once
				if conditionHolds {
					s.sometimesSatisfied[assertion.Condition.Name()] = true
				}

			case EventuallyAlways:
				// Track when condition becomes true and if it stays true
				condName := assertion.Condition.Name()
				if conditionHolds {
					// Mark when it first became true
					if _, exists := s.eventuallyAlwaysBecameTrue[condName]; !exists {
						s.eventuallyAlwaysBecameTrue[condName] = s.currentTick
					}
				} else {
					// Check if it was previously true (violation!)
					if _, wasTrue := s.eventuallyAlwaysBecameTrue[condName]; wasTrue {
						return &AssertionViolation{
							ConditionName: condName,
							Quantifier:    EventuallyAlways,
							Tick:          s.currentTick,
							Description:   fmt.Sprintf("condition became true at tick %d but is now false: %s", s.eventuallyAlwaysBecameTrue[condName], assertion.Condition.Describe(s)),
						}
					}
				}
			}
		}

		// Save tick trace to history
		if s.currentTickTrace != nil {
			// Only save non-empty ticks to trace
			if len(s.currentTickTrace.NodeSteps) > 0 || len(s.currentTickTrace.DroppedIndicators) > 0 || len(s.currentTickTrace.AssertionViolations) > 0 || len(s.currentTickTrace.NodeChanges) > 0 {
				s.traceBuffer.Append(*s.currentTickTrace)
			}

			s.currentTickTrace = nil
		}

		if firstViolation != nil {
			return firstViolation
		}

		s.currentTick++
	}
}

// RunFor is a convenience helper that runs the simulation for exactly N more ticks
func (s *Simulator[I, R, ID]) RunFor(ticks int64) error {
	return s.RunUntil(TickCondition[I, R, ID](ticks), ticks)
}

// Nodes returns all registered nodes, including any that are currently stopped.
func (s *Simulator[I, R, ID]) Nodes() []Node[I, R, ID] {
	nodes := make([]Node[I, R, ID], 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// afterTick runs fn immediately if called outside a tick, or defers it to
// the end of the current tick if called from within the step loop.
func (s *Simulator[I, R, ID]) afterTick(fn func()) {
	if s.inTick {
		s.pendingChanges = append(s.pendingChanges, fn)
	} else {
		fn()
	}
}

// StopNode stops a node: it will no longer receive Step() calls and any indicators
// addressed to it are dropped (recorded in the trace). The node remains registered.
//
// If called during a tick (e.g., from inside a RequestHandler), the change is deferred
// to the end of the current tick. If called outside a tick, it is immediate.
func (s *Simulator[I, R, ID]) StopNode(id ID) {
	if _, exists := s.nodes[id]; !exists {
		panic(fmt.Sprintf("cannot stop unknown node %v", id))
	}
	s.afterTick(func() {
		s.stoppedIDs[id] = true
		if s.currentTickTrace != nil {
			s.currentTickTrace.NodeChanges = append(s.currentTickTrace.NodeChanges,
				fmt.Sprintf("stopped: %v", id))
		}
	})
}

// ResumeNode resumes a stopped node so it receives Step() calls again.
//
// If called during a tick, the change is deferred to end of tick.
func (s *Simulator[I, R, ID]) ResumeNode(id ID) {
	if _, exists := s.nodes[id]; !exists {
		panic(fmt.Sprintf("cannot resume unknown node %v", id))
	}
	s.afterTick(func() {
		delete(s.stoppedIDs, id)
		if s.currentTickTrace != nil {
			s.currentTickTrace.NodeChanges = append(s.currentTickTrace.NodeChanges,
				fmt.Sprintf("resumed: %v", id))
		}
	})
}

// RestartNode simulates a crash-restart of a stopped node.
// If the node implements the Restartable interface, Restart() is called so the
// node can clear ephemeral state while retaining durable state (e.g., disk data).
// The node is then resumed and will receive Step() calls starting next tick.
//
// Panics if the node does not exist. If called during a tick, the restart is
// deferred to end of tick.
func (s *Simulator[I, R, ID]) RestartNode(id ID) {
	if _, exists := s.nodes[id]; !exists {
		panic(fmt.Sprintf("cannot restart unknown node %v", id))
	}
	s.afterTick(func() {
		if r, ok := s.nodes[id].(Restartable); ok {
			r.Restart()
		}
		delete(s.stoppedIDs, id)
		if s.currentTickTrace != nil {
			s.currentTickTrace.NodeChanges = append(s.currentTickTrace.NodeChanges,
				fmt.Sprintf("restarted: %v", id))
		}
	})
}

// IsNodeStopped reports whether the node with the given ID is currently stopped.
func (s *Simulator[I, R, ID]) IsNodeStopped(id ID) bool {
	return s.stoppedIDs[id]
}

// applyPendingNodeChanges runs any node lifecycle changes deferred during the tick.
// Called at the end of each tick, before assertion checking.
func (s *Simulator[I, R, ID]) applyPendingNodeChanges() {
	for _, fn := range s.pendingChanges {
		fn()
	}
	s.pendingChanges = s.pendingChanges[:0]
}
