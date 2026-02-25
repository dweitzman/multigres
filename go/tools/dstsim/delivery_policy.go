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
	"math/rand/v2"
)

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

// PerSourcePolicy dispatches to different delivery policies based on which node sent
// the indicator. This lets callers give some sources (e.g. a discovery/etcd node)
// a reliable ordered delivery while applying chaos to all other traffic.
//
// Example:
//
//	sim.SetDeliveryPolicy(&dstsim.PerSourcePolicy[Ind, NodeID]{
//	    Default:   &dstsim.UnreliableNetwork[Ind, NodeID]{MaxDelay: 5, DropRate: 0.1, Rng: rng},
//	    Overrides: map[NodeID]dstsim.IndicatorDeliveryPolicy[Ind, NodeID]{
//	        "discovery": dstsim.NewOrderedReliableNetwork[Ind, NodeID](3, rng),
//	    },
//	})
type PerSourcePolicy[I any, ID comparable] struct {
	Default   IndicatorDeliveryPolicy[I, ID]
	Overrides map[ID]IndicatorDeliveryPolicy[I, ID]
}

func (p *PerSourcePolicy[I, ID]) ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (bool, int64) {
	if override, ok := p.Overrides[fromNode]; ok {
		return override.ScheduleDelivery(currentTick, fromNode, target, indicator)
	}
	return p.Default.ScheduleDelivery(currentTick, fromNode, target, indicator)
}

// orderedChannel is the key for tracking per-channel delivery state in OrderedReliableNetwork.
type orderedChannel[ID comparable] struct{ from, to ID }

// OrderedReliableNetwork delivers all messages reliably (no drops) with variable delay,
// but guarantees that messages from the same source to the same target arrive in the
// order they were sent. This models a reliable ordered transport such as a TCP connection
// or an etcd watch stream.
//
// Use this policy (via PerSourcePolicy) for sources like a discovery node whose messages
// must never be lost or reordered, while normal orch↔pooler traffic uses chaos.
type OrderedReliableNetwork[I any, ID comparable] struct {
	MaxDelay     int64                        // Maximum additional delay beyond the mandatory 1-tick minimum
	Rng          *rand.Rand                   // Must be provided
	lastDelivery map[orderedChannel[ID]]int64 // last scheduled delivery tick per (from, to) channel
}

// NewOrderedReliableNetwork creates an OrderedReliableNetwork with the given max delay.
func NewOrderedReliableNetwork[I any, ID comparable](maxDelay int64, rng *rand.Rand) *OrderedReliableNetwork[I, ID] {
	return &OrderedReliableNetwork[I, ID]{
		MaxDelay:     maxDelay,
		Rng:          rng,
		lastDelivery: make(map[orderedChannel[ID]]int64),
	}
}

func (p *OrderedReliableNetwork[I, ID]) ScheduleDelivery(currentTick int64, fromNode ID, target ID, indicator I) (bool, int64) {
	delay := int64(1)
	if p.MaxDelay > 0 {
		delay = 1 + p.Rng.Int64N(p.MaxDelay)
	}
	deliverAt := currentTick + delay

	// Enforce ordering: this message must arrive strictly after the previous one
	// on the same (from, to) channel.
	ch := orderedChannel[ID]{from: fromNode, to: target}
	if last, ok := p.lastDelivery[ch]; ok && deliverAt <= last {
		deliverAt = last + 1
	}
	p.lastDelivery[ch] = deliverAt

	return true, deliverAt - currentTick
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
	return "stage_active_" + c.stageName
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
			seq.currentStageIndex++
		}
	}

	// Use current stage's policy
	return seq.stages[seq.currentStageIndex].policy.ScheduleDelivery(currentTick, fromNode, target, indicator)
}
