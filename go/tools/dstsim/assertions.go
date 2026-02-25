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
)

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
