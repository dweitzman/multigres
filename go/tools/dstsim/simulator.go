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

import (
	"fmt"
	"io"
	"math/rand/v2"
)

// TickHandler is called at each tick to drive the simulation
// It typically delivers tick indicators to nodes and processes their responses
type TickHandler[I any, R any, ID comparable] interface {
	// OnTick is called at the start of each tick during RunUntil
	OnTick(sim *Simulator[I, R, ID], tick int64)
}

// RequestHandler processes requests emitted by nodes
// It typically delivers requests to target nodes (with optional delays/transformations)
type RequestHandler[I any, R any, ID comparable] interface {
	// ProcessRequests is called after a node emits requests
	ProcessRequests(sim *Simulator[I, R, ID], fromNode ID, requests []R)
}

// MessageInterceptor can delay or drop messages to simulate network chaos
// This is test-only - production event loops should not use interceptors
type MessageInterceptor[I any, R any, ID comparable] interface {
	// InterceptIndicator is called before delivering an indicator to a node
	// Returns the delay in ticks (0 = immediate, positive = delayed, -1 = dropped)
	InterceptIndicator(currentTick int64, target ID, indicator I) int64

	// InterceptRequest is called before scheduling a request's delivery
	// Returns the delay in ticks (0 = immediate, positive = delayed, -1 = dropped)
	InterceptRequest(currentTick int64, from ID, request R) int64
}

// TraceEvent records a simulation event
type TraceEvent[I any, R any, ID comparable] struct {
	Tick      int64
	NodeID    ID
	Indicator *I
	Requests  []R
}

// StateProvider is an optional interface that nodes can implement to expose internal state for debugging
// The returned value should be a simple map or struct that can be easily logged
type StateProvider interface {
	GetDebugState() any
}

// Simulator executes a distributed protocol deterministically
type Simulator[I any, R any, ID comparable] struct {
	nodes       map[ID]Node[I, R, ID]
	currentTick int64
	rng         *rand.Rand
	assertions  []Assertion[I, R, ID]
	interceptor MessageInterceptor[I, R, ID]

	// Debug logging
	debugLogWriter io.Writer

	// Pluggable handlers for driving simulation
	tickHandler    TickHandler[I, R, ID]
	requestHandler RequestHandler[I, R, ID]

	// Track which "Sometimes" conditions have been satisfied
	sometimesSatisfied map[string]bool

	// Track EventuallyAlways: when condition became true and if it stayed true
	eventuallyAlwaysBecameTrue map[string]int64 // tick when condition first became true
	eventuallyAlwaysViolated   map[string]bool  // whether condition became false after being true

	// Scheduled actions (ticks when to inject indicators)
	scheduledActions map[int64][]func()

	// Trace of all events
	trace []TraceEvent[I, R, ID]

	// Current tick log (for debug logging)
	currentTickLog *tickLog[I, R, ID]
}

type tickIndicatorDelivery[I any, R any, ID comparable] struct {
	nodeID    ID
	indicator I
	requests  []R
}

// tickLog collects everything that happens during a tick for logging
type tickLog[I any, R any, ID comparable] struct {
	tick                int64
	indicators          []tickIndicatorDelivery[I, R, ID]
	assertionViolations []string
}

// SimulatorOptions configures the simulator
type SimulatorOptions struct {
	Seed           int64     // For reproducible randomness
	DebugLogWriter io.Writer // Optional writer for human-readable debug logs
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
		debugLogWriter:             opts.DebugLogWriter,
		assertions:                 make([]Assertion[I, R, ID], 0),
		sometimesSatisfied:         make(map[string]bool),
		eventuallyAlwaysBecameTrue: make(map[string]int64),
		eventuallyAlwaysViolated:   make(map[string]bool),
		scheduledActions:           make(map[int64][]func()),
		trace:                      make([]TraceEvent[I, R, ID], 0),
	}
}

// RegisterNode adds a node to the simulation
func (s *Simulator[I, R, ID]) RegisterNode(n Node[I, R, ID]) {
	s.nodes[n.ID()] = n
}

// AddAssertion registers an assertion with a temporal quantifier
func (s *Simulator[I, R, ID]) AddAssertion(assertion Assertion[I, R, ID]) {
	s.assertions = append(s.assertions, assertion)
}

// Always adds an assertion that must be true at every tick (safety/invariant)
func (s *Simulator[I, R, ID]) Always(cond Condition[I, R, ID]) {
	s.AddAssertion(Assertion[I, R, ID]{Condition: cond, Quantifier: Always})
}

// Never adds an assertion that must never be true (forbidden state)
func (s *Simulator[I, R, ID]) Never(cond Condition[I, R, ID]) {
	s.AddAssertion(Assertion[I, R, ID]{Condition: cond, Quantifier: Never})
}

// Sometimes adds an assertion that must be true at least once (reachability)
func (s *Simulator[I, R, ID]) Sometimes(cond Condition[I, R, ID]) {
	s.AddAssertion(Assertion[I, R, ID]{Condition: cond, Quantifier: Sometimes})
}

// Finally adds an assertion that must be true at the end of simulation
func (s *Simulator[I, R, ID]) Finally(cond Condition[I, R, ID]) {
	s.AddAssertion(Assertion[I, R, ID]{Condition: cond, Quantifier: Finally})
}

// EventuallyAlways adds an assertion that must become true and stay true (◇□P)
func (s *Simulator[I, R, ID]) EventuallyAlways(cond Condition[I, R, ID]) {
	s.AddAssertion(Assertion[I, R, ID]{Condition: cond, Quantifier: EventuallyAlways})
}

// SetInterceptor sets the message interceptor for chaos testing
func (s *Simulator[I, R, ID]) SetInterceptor(interceptor MessageInterceptor[I, R, ID]) {
	s.interceptor = interceptor
}

// SetTickHandler sets the handler that will be called at each tick
func (s *Simulator[I, R, ID]) SetTickHandler(handler TickHandler[I, R, ID]) {
	s.tickHandler = handler
}

// SetRequestHandler sets the handler that will process node requests
func (s *Simulator[I, R, ID]) SetRequestHandler(handler RequestHandler[I, R, ID]) {
	s.requestHandler = handler
}

// ScheduleIn schedules an action to run after a delay (in ticks) from now
// delay must be >= 0 (use 0 for immediate execution on next tick processing)
func (s *Simulator[I, R, ID]) ScheduleIn(delay int64, fn func()) {
	deliverAt := s.currentTick + delay
	s.scheduledActions[deliverAt] = append(s.scheduledActions[deliverAt], fn)
}

// DeliverIndicator delivers an indicator to a specific node
// If an interceptor is set, it may delay or drop the indicator
func (s *Simulator[I, R, ID]) DeliverIndicator(nodeID ID, ind I) []R {
	// Check interceptor
	if s.interceptor != nil {
		delay := s.interceptor.InterceptIndicator(s.currentTick, nodeID, ind)
		if delay == -1 {
			// Dropped
			return nil
		}
		if delay > 0 {
			// Delayed - schedule for later, bypass interceptor on delivery
			s.ScheduleIn(delay, func() {
				s.deliverIndicatorInternal(nodeID, ind)
			})
			return nil
		}
		// delay == 0, deliver immediately (fall through)
	}

	return s.deliverIndicatorInternal(nodeID, ind)
}

// deliverIndicatorInternal delivers without going through interceptor
func (s *Simulator[I, R, ID]) deliverIndicatorInternal(nodeID ID, ind I) []R {
	node, exists := s.nodes[nodeID]
	if !exists {
		return nil
	}

	requests := node.Step(ind)

	// Record in trace
	s.trace = append(s.trace, TraceEvent[I, R, ID]{
		Tick:      s.currentTick,
		NodeID:    nodeID,
		Indicator: &ind,
		Requests:  requests,
	})

	// Collect for debug logging (will be logged at end of tick)
	if s.currentTickLog != nil {
		s.currentTickLog.indicators = append(s.currentTickLog.indicators, tickIndicatorDelivery[I, R, ID]{
			nodeID:    nodeID,
			indicator: ind,
			requests:  requests,
		})
	}

	return requests
}

// Trace returns the complete event trace
func (s *Simulator[I, R, ID]) Trace() []TraceEvent[I, R, ID] {
	return s.trace
}

// logTickSummary logs the collected tick information
func (s *Simulator[I, R, ID]) logTickSummary(log *tickLog[I, R, ID]) {
	if len(log.indicators) == 0 && len(log.assertionViolations) == 0 {
		// Skip logging ticks with no activity
		return
	}

	fmt.Fprintf(s.debugLogWriter, "\n=== Tick %d ===\n", log.tick)

	// Log assertion violations first (if any)
	if len(log.assertionViolations) > 0 {
		fmt.Fprintf(s.debugLogWriter, "⚠️  ASSERTION VIOLATIONS:\n")
		for _, violation := range log.assertionViolations {
			fmt.Fprintf(s.debugLogWriter, "    %s\n", violation)
		}
		fmt.Fprintf(s.debugLogWriter, "\n")
	}

	// Log node states
	if len(log.indicators) > 0 {
		fmt.Fprintf(s.debugLogWriter, "Node states:\n")
		for nodeID, node := range s.nodes {
			if stateProvider, ok := node.(StateProvider); ok {
				fmt.Fprintf(s.debugLogWriter, "  %v: %+v\n", nodeID, stateProvider.GetDebugState())
			}
		}
		fmt.Fprintf(s.debugLogWriter, "\n")
	}

	// Log all indicator deliveries
	if len(log.indicators) > 0 {
		fmt.Fprintf(s.debugLogWriter, "Indicators delivered:\n")
		for _, delivery := range log.indicators {
			fmt.Fprintf(s.debugLogWriter, "  %v <- %T %+v\n", delivery.nodeID, delivery.indicator, delivery.indicator)
			if len(delivery.requests) > 0 {
				fmt.Fprintf(s.debugLogWriter, "    -> %d requests: %+v\n", len(delivery.requests), delivery.requests)
			}
		}
	}
}

// RunUntil runs the simulation until the specified tick
func (s *Simulator[I, R, ID]) RunUntil(maxTick int64) error {
	for s.currentTick <= maxTick {
		// Initialize tick log if debug logging is enabled
		if s.debugLogWriter != nil {
			s.currentTickLog = &tickLog[I, R, ID]{
				tick:                s.currentTick,
				indicators:          make([]tickIndicatorDelivery[I, R, ID], 0),
				assertionViolations: make([]string, 0),
			}
		}

		// Call tick handler if set (drives the simulation)
		if s.tickHandler != nil {
			s.tickHandler.OnTick(s, s.currentTick)
		}

		// Execute scheduled actions for this tick
		if actions, exists := s.scheduledActions[s.currentTick]; exists {
			for _, action := range actions {
				action()
			}
			delete(s.scheduledActions, s.currentTick)
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
					if s.currentTickLog != nil && firstViolation == nil {
						s.currentTickLog.assertionViolations = append(s.currentTickLog.assertionViolations,
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
					if s.currentTickLog != nil && firstViolation == nil {
						s.currentTickLog.assertionViolations = append(s.currentTickLog.assertionViolations,
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

		// Log tick summary and return first violation if any
		if s.currentTickLog != nil {
			s.logTickSummary(s.currentTickLog)
			s.currentTickLog = nil
		}

		if firstViolation != nil {
			return firstViolation
		}

		s.currentTick++
	}

	// After simulation ends, check "Sometimes" and "Eventually" properties
	return s.checkDeferredProperties()
}

// checkDeferredProperties checks assertions that are evaluated at the end of simulation
func (s *Simulator[I, R, ID]) checkDeferredProperties() error {
	for _, assertion := range s.assertions {
		switch assertion.Quantifier {
		case Sometimes:
			// Check if condition was ever satisfied
			if !s.sometimesSatisfied[assertion.Condition.Name()] {
				return &AssertionViolation{
					ConditionName: assertion.Condition.Name(),
					Quantifier:    Sometimes,
					Tick:          s.currentTick,
					Description:   "condition was never true during simulation",
				}
			}

		case Finally:
			// Check if condition is true at the end
			if !assertion.Condition.Eval(s) {
				return &AssertionViolation{
					ConditionName: assertion.Condition.Name(),
					Quantifier:    Finally,
					Tick:          s.currentTick,
					Description:   assertion.Condition.Describe(s),
				}
			}

		case EventuallyAlways:
			// Check if condition ever became true
			condName := assertion.Condition.Name()
			if _, becameTrue := s.eventuallyAlwaysBecameTrue[condName]; !becameTrue {
				return &AssertionViolation{
					ConditionName: condName,
					Quantifier:    EventuallyAlways,
					Tick:          s.currentTick,
					Description:   "condition never became true during simulation",
				}
			}
			// Also verify it's still true at the end
			if !assertion.Condition.Eval(s) {
				return &AssertionViolation{
					ConditionName: condName,
					Quantifier:    EventuallyAlways,
					Tick:          s.currentTick,
					Description:   fmt.Sprintf("condition became true at tick %d but is false at end: %s", s.eventuallyAlwaysBecameTrue[condName], assertion.Condition.Describe(s)),
				}
			}
		}
	}

	return nil
}

// CurrentTick returns the current simulation tick
func (s *Simulator[I, R, ID]) CurrentTick() int64 {
	return s.currentTick
}

// Nodes returns all registered nodes
func (s *Simulator[I, R, ID]) Nodes() []Node[I, R, ID] {
	nodes := make([]Node[I, R, ID], 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// Condition is a named predicate that can be evaluated at any point in the simulation
// Conditions are pure predicates - they describe "what is true" without judging if that's good or bad
type Condition[I any, R any, ID comparable] interface {
	// Eval returns true if the condition currently holds
	Eval(sim *Simulator[I, R, ID]) bool

	// Name returns a unique identifier for this condition
	Name() string

	// Describe returns a human-readable description of the current state
	// (useful for debugging/logging, especially when condition is true)
	Describe(sim *Simulator[I, R, ID]) string
}

// TemporalQuantifier specifies when/how often a condition must hold
type TemporalQuantifier int

const (
	Always           TemporalQuantifier = iota // Condition must be true at every tick (safety/invariant)
	Sometimes                                  // Condition must be true at least once (reachability)
	Finally                                    // Condition must be true at the end of simulation
	EventuallyAlways                           // Condition must become true and stay true (◇□P - liveness)
	Never                                      // Condition must never be true (unreachable/forbidden)
)

// Assertion combines a condition with a temporal quantifier
// This separates "what to check" from "when/how often we want it to be true"
type Assertion[I any, R any, ID comparable] struct {
	Condition  Condition[I, R, ID]
	Quantifier TemporalQuantifier
}

// AssertionViolation is returned when an assertion is violated
type AssertionViolation struct {
	AssertionName string
	ConditionName string
	Quantifier    TemporalQuantifier
	Tick          int64
	Description   string
}

func (e *AssertionViolation) Error() string {
	switch e.Quantifier {
	case Never:
		return fmt.Sprintf("assertion violated at tick %d: condition '%s' was true but should never be (state: %s)",
			e.Tick, e.ConditionName, e.Description)
	case Always:
		return fmt.Sprintf("assertion violated at tick %d: condition '%s' was false but should always be true (state: %s)",
			e.Tick, e.ConditionName, e.Description)
	case Sometimes:
		return fmt.Sprintf("assertion violated: condition '%s' was never true during simulation",
			e.ConditionName)
	case Finally:
		return fmt.Sprintf("assertion violated: condition '%s' was not true at end of simulation (final state: %s)",
			e.ConditionName, e.Description)
	case EventuallyAlways:
		return "assertion violated: " + e.Description
	default:
		return fmt.Sprintf("assertion '%s' violated", e.AssertionName)
	}
}
