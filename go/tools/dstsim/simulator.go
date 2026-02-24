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
	"strings"
)

// IndicatorDelivery represents an indicator to be delivered to a target node
// RequestHandler processes requests emitted by nodes
// It typically delivers requests to target nodes (with optional delays/transformations)
type RequestHandler[I any, R any, ID comparable] interface {
	// ProcessRequests is called after a node emits requests
	// Returns a map of target nodes to indicators that should be delivered
	ProcessRequests(sim *Simulator[I, R, ID], fromNode ID, requests []R) map[ID][]I
}

// IndicatorDeliveryPolicy determines when and if indicators are delivered
// This models network behavior: latency, packet loss, and (future) retries/duplicates
type IndicatorDeliveryPolicy[I any, ID comparable] interface {
	// ScheduleDelivery is called when an indicator is enqueued for delivery
	// Parameters:
	//   - currentTick: the current simulator tick
	//   - fromNode: the node sending the indicator (may be zero value if unknown)
	//   - target: the node receiving the indicator
	//   - indicator: the message being delivered
	//
	// Returns:
	//   - delivered: false if message is dropped, true if it should be delivered
	//   - delayTicks: must be >= 1 if delivered=true (enforced by simulator)
	//
	// Future: Could return []int64 to support multiple deliveries (retries/duplicates)
	ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (delivered bool, delayTicks int64)
}

// FastNetwork simulates a fast, reliable network (e.g., local datacenter, loopback)
// with minimal latency (1 tick) and no packet loss
type FastNetwork[I any, ID comparable] struct{}

func (p *FastNetwork[I, ID]) ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (bool, int64) {
	return true, 1 // Always deliver at next tick
}

// UnreliableNetwork simulates a chaotic network with random delays and packet loss
type UnreliableNetwork[I any, ID comparable] struct {
	MaxDelay int64      // Maximum delay in ticks (>= 1)
	DropRate float64    // Probability of dropping message (0.0 - 1.0)
	Rng      *rand.Rand // Random number generator (must be provided)
}

func (p *UnreliableNetwork[I, ID]) ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (bool, int64) {
	// Check if message should be dropped
	if p.Rng.Float64() < p.DropRate {
		return false, 0 // Message dropped
	}

	// Random delay between 1 and MaxDelay (inclusive)
	delay := int64(1)
	if p.MaxDelay > 1 {
		delay = 1 + p.Rng.Int64N(p.MaxDelay)
	}
	return true, delay
}

// UntilPolicy uses InitialPolicy until a condition becomes true, then permanently switches to AfterPolicy
// This is a "latching" policy - once switched, it never switches back
type UntilPolicy[I any, R any, ID comparable] struct {
	UntilCondition Condition[I, R, ID]            // When this becomes true, switch to AfterPolicy
	InitialPolicy  IndicatorDeliveryPolicy[I, ID] // Policy to use before condition is true
	AfterPolicy    IndicatorDeliveryPolicy[I, ID] // Policy to use after condition becomes true (permanent)
	Sim            *Simulator[I, R, ID]           // Reference to simulator for condition evaluation
	hasSwitched    bool                           // Track whether we've switched (latching)
}

func (p *UntilPolicy[I, R, ID]) ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (bool, int64) {
	// Check if we should switch (only check if we haven't switched yet)
	if !p.hasSwitched && p.UntilCondition.Eval(p.Sim) {
		p.hasSwitched = true
	}

	// Use appropriate policy
	if p.hasSwitched {
		return p.AfterPolicy.ScheduleDelivery(currentTick, fromNode, target, indicator)
	}
	return p.InitialPolicy.ScheduleDelivery(currentTick, fromNode, target, indicator)
}

// PolicySequence manages a sequence of delivery policies with observable transitions
// Each stage has a policy and a condition for when to advance to the next stage
// Stages can be queried to check if they're active, enabling assertions about policy state
type PolicySequence[I any, R any, ID comparable] struct {
	stages            []policyStage[I, R, ID]
	currentStageIndex int
	sim               *Simulator[I, R, ID]
}

type policyStage[I any, R any, ID comparable] struct {
	policy         IndicatorDeliveryPolicy[I, ID]
	advanceWhen    Condition[I, R, ID] // When to advance to next stage (nil for final stage)
	stageCondition *StageActiveCondition[I, R, ID]
}

// StageActiveCondition is a Condition that's true when a specific stage is active
type StageActiveCondition[I any, R any, ID comparable] struct {
	seq        *PolicySequence[I, R, ID]
	stageIndex int
	stageName  string
}

func (c *StageActiveCondition[I, R, ID]) Eval(sim *Simulator[I, R, ID]) bool {
	return c.seq.currentStageIndex == c.stageIndex
}

func (c *StageActiveCondition[I, R, ID]) Name() string {
	return c.stageName
}

func (c *StageActiveCondition[I, R, ID]) Describe(sim *Simulator[I, R, ID]) string {
	return fmt.Sprintf("policy stage '%s' is active (stage %d of %d)", c.stageName, c.stageIndex+1, len(c.seq.stages))
}

// NewPolicySequence creates a new policy sequence starting with the given initial policy
func NewPolicySequence[I any, R any, ID comparable](sim *Simulator[I, R, ID], initialPolicy IndicatorDeliveryPolicy[I, ID], stageName string) *PolicySequence[I, R, ID] {
	seq := &PolicySequence[I, R, ID]{
		stages:            make([]policyStage[I, R, ID], 0),
		currentStageIndex: 0,
		sim:               sim,
	}

	// Create condition for initial stage
	stageCondition := &StageActiveCondition[I, R, ID]{
		seq:        seq,
		stageIndex: 0,
		stageName:  stageName,
	}

	// Add initial stage
	seq.stages = append(seq.stages, policyStage[I, R, ID]{
		policy:         initialPolicy,
		advanceWhen:    nil, // Will be set when next stage is added
		stageCondition: stageCondition,
	})

	return seq
}

// AppendPolicy adds a new stage to the sequence
// The sequence will advance from the current last stage to this new stage when advanceWhen becomes true
// Returns a Condition that's true when this stage is active (can be used in assertions)
func (seq *PolicySequence[I, R, ID]) AppendPolicy(policy IndicatorDeliveryPolicy[I, ID], advanceWhen Condition[I, R, ID], stageName string) Condition[I, R, ID] {
	// Set the advance condition for the previous stage
	if len(seq.stages) > 0 {
		seq.stages[len(seq.stages)-1].advanceWhen = advanceWhen
	}

	// Create condition for this stage
	stageIndex := len(seq.stages)
	stageCondition := &StageActiveCondition[I, R, ID]{
		seq:        seq,
		stageIndex: stageIndex,
		stageName:  stageName,
	}

	// Add new stage
	seq.stages = append(seq.stages, policyStage[I, R, ID]{
		policy:         policy,
		advanceWhen:    nil, // Will be set when next stage is added (or remain nil if final)
		stageCondition: stageCondition,
	})

	return stageCondition
}

// ScheduleDelivery implements IndicatorDeliveryPolicy
func (seq *PolicySequence[I, R, ID]) ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (bool, int64) {
	// Check if we should advance to next stage
	if seq.currentStageIndex < len(seq.stages)-1 {
		currentStage := seq.stages[seq.currentStageIndex]
		if currentStage.advanceWhen != nil && currentStage.advanceWhen.Eval(seq.sim) {
			oldIndex := seq.currentStageIndex
			seq.currentStageIndex++
			// Log stage transition if debug logging is enabled
			if seq.sim.debugLogWriter != nil {
				fmt.Fprintf(seq.sim.debugLogWriter, "[PolicySequence] Tick %d: Advanced from stage %d (%s) to stage %d (%s)\n",
					currentTick, oldIndex, seq.stages[oldIndex].stageCondition.stageName,
					seq.currentStageIndex, seq.stages[seq.currentStageIndex].stageCondition.stageName)
			}
		}
	}

	// Use current stage's policy
	return seq.stages[seq.currentStageIndex].policy.ScheduleDelivery(currentTick, fromNode, target, indicator)
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

	// Debug logging
	debugLogWriter io.Writer

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
		indicatorDeliveryPolicy:    &FastNetwork[I, ID]{}, // Default: fast network with 1-tick latency
		assertions:                 make([]Assertion[I, R, ID], 0),
		sometimesSatisfied:         make(map[string]bool),
		eventuallyAlwaysBecameTrue: make(map[string]int64),
		eventuallyAlwaysViolated:   make(map[string]bool),
		scheduledDeliveries:        make(map[int64]map[ID][]I),
		trace:                      make([]TraceEvent[I, R, ID], 0),
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

// SetTickHandler sets the handler that will be called at each tick
// SetRequestHandler sets the handler that will process node requests
func (s *Simulator[I, R, ID]) SetRequestHandler(handler RequestHandler[I, R, ID]) {
	s.requestHandler = handler
}

// ScheduleIn schedules an action to run after a delay (in ticks) from now
// delay must be >= 0 (use 0 for immediate execution on next tick processing)
// scheduleIndicator schedules an indicator for delivery to a target node via the delivery policy
// The policy determines when/if the indicator is delivered (enforces minimum 1-tick delay)
// This is an internal method - external callers should use RequestHandler.ProcessRequests
func (s *Simulator[I, R, ID]) scheduleIndicator(fromNode ID, targetNode ID, ind I) {
	// Use delivery policy to determine when/if to deliver
	delivered, delayTicks := s.indicatorDeliveryPolicy.ScheduleDelivery(s.currentTick, fromNode, targetNode, ind)

	// Message dropped by policy
	if !delivered {
		if s.currentTickLog != nil {
			s.currentTickLog.indicators = append(s.currentTickLog.indicators, tickIndicatorDelivery[I, R, ID]{
				nodeID:    targetNode,
				indicator: ind,
				requests:  nil, // No requests since it was dropped
			})
		}
		return
	}

	// Enforce realistic latency: delay must be at least 1 tick
	// No exceptions - messages cannot be delivered in the same tick they're generated
	if delayTicks < 1 {
		panic(fmt.Sprintf("IndicatorDeliveryPolicy returned invalid delay: %d (must be >= 1, no exceptions)", delayTicks))
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
		fmt.Fprintf(s.debugLogWriter, "Indicators:\n")
		for _, delivery := range log.indicators {
			if delivery.requests == nil {
				// Dropped by delivery policy
				fmt.Fprintf(s.debugLogWriter, "  %v <- %T %+v [DROPPED]\n", delivery.nodeID, delivery.indicator, delivery.indicator)
			} else {
				fmt.Fprintf(s.debugLogWriter, "  %v <- %T %+v\n", delivery.nodeID, delivery.indicator, delivery.indicator)
				if len(delivery.requests) > 0 {
					fmt.Fprintf(s.debugLogWriter, "    -> %d requests: %+v\n", len(delivery.requests), delivery.requests)
				}
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

		// Get scheduled indicator deliveries for this tick (local variable - no simulator state)
		tickIndicators := s.scheduledDeliveries[s.currentTick]
		delete(s.scheduledDeliveries, s.currentTick)

		// Process all nodes for this tick by calling Step() once per node
		// All nodes must be called every tick (even with empty indicators) so they can process time-based logic
		for nodeID, node := range s.nodes {
			indicators := tickIndicators[nodeID] // nil slice if no indicators
			requests := node.Step(s.currentTick, indicators)

			// Record in trace
			for _, ind := range indicators {
				s.trace = append(s.trace, TraceEvent[I, R, ID]{
					Tick:      s.currentTick,
					NodeID:    nodeID,
					Indicator: &ind,
					Requests:  requests,
				})
			}

			// Collect for debug logging (will be logged at end of tick)
			if s.currentTickLog != nil {
				for _, ind := range indicators {
					s.currentTickLog.indicators = append(s.currentTickLog.indicators, tickIndicatorDelivery[I, R, ID]{
						nodeID:    nodeID,
						indicator: ind,
						requests:  requests,
					})
				}
			}

			// Process requests from the node - convert to indicators
			if s.requestHandler != nil && len(requests) > 0 {
				indicatorsToDeliver := s.requestHandler.ProcessRequests(s, nodeID, requests)
				// Schedule all indicators through the delivery policy
				for targetID, indicators := range indicatorsToDeliver {
					for _, ind := range indicators {
						s.scheduleIndicator(nodeID, targetID, ind)
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
