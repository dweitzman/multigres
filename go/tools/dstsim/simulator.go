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
// TODO: Add crash/restart simulation capabilities for testing fault tolerance.
// Many distributed systems bugs involve crashes - nodes need to be able to:
// - Crash at arbitrary ticks (lose in-memory state)
// - Restart at arbitrary ticks (reload persistent state)
// - Have separate persistent vs ephemeral state
// This would enable testing crash recovery, leader election during crashes, etc.
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

		// Process all nodes for this tick by calling Step() once per node
		// All nodes must be called every tick (even with empty indicators) so they can process time-based logic
		for nodeID, node := range s.nodes {
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
				// Schedule all indicators through the delivery policy
				for targetID, indicators := range indicatorsToDeliver {
					for _, ind := range indicators {
						if err := s.scheduleIndicator(nodeID, targetID, ind); err != nil {
							return err
						}
					}
				}
			}
		}

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
			if len(s.currentTickTrace.NodeSteps) > 0 || len(s.currentTickTrace.DroppedIndicators) > 0 || len(s.currentTickTrace.AssertionViolations) > 0 {
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

// Nodes returns all registered nodes
func (s *Simulator[I, R, ID]) Nodes() []Node[I, R, ID] {
	nodes := make([]Node[I, R, ID], 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}
