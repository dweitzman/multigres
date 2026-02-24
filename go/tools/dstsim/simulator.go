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
	"math/rand/v2"
)

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

// Simulator executes a distributed protocol deterministically
type Simulator[I any, R any, ID comparable] struct {
	nodes       map[ID]Node[I, R, ID]
	currentTick int64
	rng         *rand.Rand
	invariants  []Invariant[I, R, ID]
	interceptor MessageInterceptor[I, R, ID]

	// Scheduled actions (ticks when to inject indicators)
	scheduledActions map[int64][]func()

	// Trace of all events
	trace []TraceEvent[I, R, ID]
}

// SimulatorOptions configures the simulator
type SimulatorOptions struct {
	Seed int64 // For reproducible randomness
}

// NewSimulator creates a new simulator
func NewSimulator[I, R any, ID comparable](opts SimulatorOptions) *Simulator[I, R, ID] {
	rng := rand.New(rand.NewPCG(uint64(opts.Seed), uint64(opts.Seed)))

	// Start at a random positive tick number to catch bugs that assume tick starts at 0
	initialTick := rng.Int64N(1000000)

	return &Simulator[I, R, ID]{
		nodes:            make(map[ID]Node[I, R, ID]),
		currentTick:      initialTick,
		rng:              rng,
		invariants:       make([]Invariant[I, R, ID], 0),
		scheduledActions: make(map[int64][]func()),
		trace:            make([]TraceEvent[I, R, ID], 0),
	}
}

// RegisterNode adds a node to the simulation
func (s *Simulator[I, R, ID]) RegisterNode(n Node[I, R, ID]) {
	s.nodes[n.ID()] = n
}

// AddInvariant registers an invariant to check after each tick
func (s *Simulator[I, R, ID]) AddInvariant(inv Invariant[I, R, ID]) {
	s.invariants = append(s.invariants, inv)
}

// SetInterceptor sets the message interceptor for chaos testing
func (s *Simulator[I, R, ID]) SetInterceptor(interceptor MessageInterceptor[I, R, ID]) {
	s.interceptor = interceptor
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

	return requests
}

// Trace returns the complete event trace
func (s *Simulator[I, R, ID]) Trace() []TraceEvent[I, R, ID] {
	return s.trace
}

// RunUntil runs the simulation until the specified tick
func (s *Simulator[I, R, ID]) RunUntil(maxTick int64) error {
	for s.currentTick <= maxTick {
		// Execute scheduled actions for this tick
		if actions, exists := s.scheduledActions[s.currentTick]; exists {
			for _, action := range actions {
				action()
			}
			delete(s.scheduledActions, s.currentTick)
		}

		// Check invariants
		for _, inv := range s.invariants {
			if violations := inv.Check(s); len(violations) > 0 {
				return &InvariantViolation{
					Tick:       s.currentTick,
					Violations: violations,
				}
			}
		}

		s.currentTick++
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

// Invariant is a safety property that should always hold
type Invariant[I any, R any, ID comparable] interface {
	Check(sim *Simulator[I, R, ID]) []string
}

// InvariantViolation is returned when an invariant is violated
type InvariantViolation struct {
	Tick       int64
	Violations []string
}

func (e *InvariantViolation) Error() string {
	return fmt.Sprintf("invariant violation at tick %d: %v", e.Tick, e.Violations)
}
