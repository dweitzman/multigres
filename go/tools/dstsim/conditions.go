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

import (
	"fmt"
	"strings"
)

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

// And combines multiple conditions - true only if all are true
type AndCombinator[I any, R any, ID comparable] struct {
	Conditions []Condition[I, R, ID]
}

func (c *AndCombinator[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	for _, cond := range c.Conditions {
		if !cond.Eval(sim) {
			return false
		}
	}
	return true
}

func (c *AndCombinator[I, R, ID]) Name() string {
	names := make([]string, len(c.Conditions))
	for i, cond := range c.Conditions {
		names[i] = cond.Name()
	}
	return fmt.Sprintf("and(%s)", strings.Join(names, ", "))
}

func (c *AndCombinator[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	descriptions := make([]string, len(c.Conditions))
	for i, cond := range c.Conditions {
		descriptions[i] = cond.Describe(sim)
	}
	return fmt.Sprintf("all of: [%s]", strings.Join(descriptions, ", "))
}

// And creates a new And condition that evaluates to true when all sub-conditions are true
func And[I any, R any, ID comparable](conditions ...Condition[I, R, ID]) *AndCombinator[I, R, ID] {
	return &AndCombinator[I, R, ID]{Conditions: conditions}
}

// OrCombinator combines multiple conditions - true if any are true
type OrCombinator[I any, R any, ID comparable] struct {
	Conditions []Condition[I, R, ID]
}

func (c *OrCombinator[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	for _, cond := range c.Conditions {
		if cond.Eval(sim) {
			return true
		}
	}
	return false
}

func (c *OrCombinator[I, R, ID]) Name() string {
	names := make([]string, len(c.Conditions))
	for i, cond := range c.Conditions {
		names[i] = cond.Name()
	}
	return fmt.Sprintf("or(%s)", strings.Join(names, ", "))
}

func (c *OrCombinator[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	descriptions := make([]string, len(c.Conditions))
	for i, cond := range c.Conditions {
		descriptions[i] = cond.Describe(sim)
	}
	return fmt.Sprintf("any of: [%s]", strings.Join(descriptions, ", "))
}

// Or creates a new Or condition that evaluates to true when any sub-condition is true
func Or[I any, R any, ID comparable](conditions ...Condition[I, R, ID]) *OrCombinator[I, R, ID] {
	return &OrCombinator[I, R, ID]{Conditions: conditions}
}

// NotCombinator negates a condition
type NotCombinator[I any, R any, ID comparable] struct {
	Condition Condition[I, R, ID]
}

func (c *NotCombinator[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	return !c.Condition.Eval(sim)
}

func (c *NotCombinator[I, R, ID]) Name() string {
	return fmt.Sprintf("not(%s)", c.Condition.Name())
}

func (c *NotCombinator[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	return "not: " + c.Condition.Describe(sim)
}

// Not creates a new Not condition that negates the given condition
func Not[I any, R any, ID comparable](condition Condition[I, R, ID]) *NotCombinator[I, R, ID] {
	return &NotCombinator[I, R, ID]{Condition: condition}
}

// RelativeTickCondition evaluates to true after N ticks have elapsed since first evaluation
// This is useful for stage transitions: "advance to next stage after 100 ticks in current stage"
type RelativeTickCondition[I any, R any, ID comparable] struct {
	ticksToWait   int64
	firstEvalTick int64
	hasBeenCalled bool
}

func (c *RelativeTickCondition[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	if !c.hasBeenCalled {
		c.firstEvalTick = sim.CurrentTick()
		c.hasBeenCalled = true
		return false // First call always returns false (0 ticks elapsed)
	}
	return sim.CurrentTick() >= c.firstEvalTick+c.ticksToWait
}

func (c *RelativeTickCondition[I, R, ID]) Name() string {
	if !c.hasBeenCalled {
		return fmt.Sprintf("after_%d_ticks", c.ticksToWait)
	}
	return fmt.Sprintf("after_%d_ticks_from_%d", c.ticksToWait, c.firstEvalTick)
}

func (c *RelativeTickCondition[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	if !c.hasBeenCalled {
		return fmt.Sprintf("after %d ticks (not yet started)", c.ticksToWait)
	}
	elapsed := sim.CurrentTick() - c.firstEvalTick
	return fmt.Sprintf("after %d ticks (elapsed: %d)", c.ticksToWait, elapsed)
}

// TickCondition creates a condition that becomes true N ticks after first evaluation
func TickCondition[I any, R any, ID comparable](ticksToWait int64) *RelativeTickCondition[I, R, ID] {
	return &RelativeTickCondition[I, R, ID]{ticksToWait: ticksToWait}
}

// AbsoluteTickReached is a condition that becomes true when the current tick reaches a specific value
type AbsoluteTickReached[I any, R any, ID comparable] struct {
	targetTick int64
}

func (c *AbsoluteTickReached[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	return sim.CurrentTick() >= c.targetTick
}

func (c *AbsoluteTickReached[I, R, ID]) Name() string {
	return fmt.Sprintf("tick_reached_%d", c.targetTick)
}

func (c *AbsoluteTickReached[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	return fmt.Sprintf("current tick: %d, target: %d", sim.CurrentTick(), c.targetTick)
}

// AbsoluteTick creates a condition that becomes true when the simulation reaches a specific tick
func AbsoluteTick[I any, R any, ID comparable](tick int64) *AbsoluteTickReached[I, R, ID] {
	return &AbsoluteTickReached[I, R, ID]{targetTick: tick}
}
