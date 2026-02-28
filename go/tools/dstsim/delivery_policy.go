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

// DeliveryArgs holds the parameters for a single delivery decision.
// Using a struct makes adding new fields non-breaking for all implementations.
type DeliveryArgs[I any, ID comparable] struct {
	CurrentTick int64
	FromNode    ID
	Target      ID
	Indicator   I
	// AllNodes lists all node IDs currently registered in the simulator, in
	// registration order. Partition policies use this to eagerly assign all
	// nodes to groups when a partition starts, enabling a single consolidated
	// trace event instead of per-node events spread across ticks. May be nil
	// in tests that call ScheduleDelivery directly; assignment falls back to
	// lazy per-node assignment in that case.
	AllNodes []ID
}

// IndicatorDeliveryPolicy determines when and if indicators are delivered.
// This models network behavior: latency, packet loss, partitions, etc.
type IndicatorDeliveryPolicy[I any, ID comparable] interface {
	// ScheduleDelivery is called when an indicator is enqueued for delivery.
	//
	// Returns:
	//   - delivered: false if message is dropped, true if it should be delivered
	//   - delayTicks: must be >= 1 if delivered=true (enforced by simulator)
	//   - events: optional trace events for this delivery decision (e.g. "partition
	//     started"). May be nil. The simulator appends these to the current tick's
	//     PolicyTransitions trace field.
	//
	// Future: Could return []int64 to support multiple deliveries (retries/duplicates)
	ScheduleDelivery(args DeliveryArgs[I, ID]) (delivered bool, delayTicks int64, events []string)
}

// FastNetwork simulates a fast, reliable network (e.g., local datacenter, loopback)
// with minimal latency (1 tick) and no packet loss
type FastNetwork[I any, ID comparable] struct{}

func (p *FastNetwork[I, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	return true, 1, nil // Always deliver at next tick
}

// UnreliableNetwork simulates a chaotic network with random delays, packet loss,
// and optional network partitions.
//
// When PartitionRate > 0, the network periodically splits into two groups: messages
// between nodes in different groups are dropped for the partition's duration.
//
// When args.AllNodes is provided (as it is when called from the simulator), all
// nodes are eagerly assigned to groups when a partition starts and a single
// "partition started: group A=[...], group B=[...]" trace event is emitted.
// When args.AllNodes is nil (e.g. in direct test calls), assignment falls back to
// lazy per-node assignment the first time each node is seen, with a per-node event.
//
// In the future, partition semantics could be extended to model zone/cell topology,
// where the split always follows datacenter region boundaries.
type UnreliableNetwork[I any, ID comparable] struct {
	MaxDelay             int64      // Maximum delay in ticks (>= 1)
	DropRate             float64    // Probability of dropping message (0.0 - 1.0)
	PartitionRate        float64    // Probability per tick that a network partition starts (0 = disabled)
	MaxPartitionDuration int64      // Maximum partition length in ticks (used when PartitionRate > 0)
	Rng                  *rand.Rand // Must be provided

	// partition state (used when PartitionRate > 0)
	//
	// groupAssignment: key present + true → group A; key present + false → group B;
	// key absent → not yet seen this partition (assigned lazily on first encounter).
	// nil between partitions.
	groupAssignment map[ID]bool
	partitionEnd    int64 // tick when current partition ends; 0 = no partition active
	lastTick        int64 // last tick for which partition state was advanced
	initialized     bool  // true after first ScheduleDelivery call; needed to handle tick 0
}

// IsPartitioned reports whether a network partition is currently active.
func (p *UnreliableNetwork[I, ID]) IsPartitioned() bool {
	return p.partitionEnd > 0
}

func (p *UnreliableNetwork[I, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	var events []string

	// Advance partition state exactly once per tick (when PartitionRate is configured).
	// The `initialized` flag handles the edge case where the simulator starts at tick 0:
	// on the very first call, `!initialized` is true regardless of currentTick value.
	if p.PartitionRate > 0 && (!p.initialized || args.CurrentTick > p.lastTick) {
		p.initialized = true
		events = append(events, p.advancePartition(args.CurrentTick, args.AllNodes)...)
		p.lastTick = args.CurrentTick
	}

	// During a partition, drop messages between the two groups.
	if p.partitionEnd > 0 {
		fromGroup, fromEvents := p.groupOf(args.FromNode)
		events = append(events, fromEvents...)
		targetGroup, targetEvents := p.groupOf(args.Target)
		events = append(events, targetEvents...)
		if fromGroup != targetGroup {
			return false, 0, events
		}
	}

	// Check if message should be dropped by normal packet loss.
	if p.Rng.Float64() < p.DropRate {
		return false, 0, events
	}

	// Random delay between 1 and MaxDelay (inclusive)
	delay := int64(1)
	if p.MaxDelay > 1 {
		delay = 1 + p.Rng.Int64N(p.MaxDelay)
	}
	return true, delay, events
}

// groupOf returns which group (true=A, false=B) the node belongs to in the current
// partition, lazily assigning it on first encounter. Returns the group and a trace
// event if a new assignment was made (used for nodes that join mid-partition or when
// args.AllNodes was nil at partition start).
func (p *UnreliableNetwork[I, ID]) groupOf(id ID) (bool, []string) {
	if inA, assigned := p.groupAssignment[id]; assigned {
		return inA, nil
	}
	inA := p.Rng.Float64() < 0.5
	p.groupAssignment[id] = inA
	group := "B"
	if inA {
		group = "A"
	}
	return inA, []string{fmt.Sprintf("node %v assigned to partition group %s", id, group)}
}

// advancePartition ends any expired partition and possibly starts a new one.
// If a new partition starts and allNodes is provided, all nodes are eagerly assigned
// to groups and a single consolidated event is returned. Otherwise only the
// "partition started" event is returned (nodes are assigned lazily via groupOf).
func (p *UnreliableNetwork[I, ID]) advancePartition(currentTick int64, allNodes []ID) []string {
	var events []string
	if p.partitionEnd > 0 && currentTick >= p.partitionEnd {
		events = append(events, "partition ended")
		p.partitionEnd = 0
		p.groupAssignment = nil
	}
	if p.partitionEnd == 0 && p.Rng.Float64() < p.PartitionRate {
		duration := int64(1)
		if p.MaxPartitionDuration > 1 {
			duration = 1 + p.Rng.Int64N(p.MaxPartitionDuration)
		}
		p.partitionEnd = currentTick + duration
		p.groupAssignment = make(map[ID]bool)
		if len(allNodes) > 0 {
			// Eagerly assign all known nodes to groups and emit a single consolidated event.
			var groupA, groupB []ID
			for _, id := range allNodes {
				if p.Rng.Float64() < 0.5 {
					p.groupAssignment[id] = true
					groupA = append(groupA, id)
				} else {
					p.groupAssignment[id] = false
					groupB = append(groupB, id)
				}
			}
			events = append(events, fmt.Sprintf("partition started (ends at tick %d): group A=%v, group B=%v", p.partitionEnd, groupA, groupB))
		} else {
			events = append(events, fmt.Sprintf("partition started (ends at tick %d)", p.partitionEnd))
		}
	}
	return events
}

// PartitionedNetwork wraps any base delivery policy and adds random network partitions
// on top of it. Use this when you want partition behaviour combined with a policy that
// does not natively support it (e.g. FastNetwork). When using UnreliableNetwork,
// prefer its built-in PartitionRate/MaxPartitionDuration fields instead.
//
// During a partition, nodes are split into two groups and messages between them are
// dropped. Node group assignment follows the same eager/lazy logic as UnreliableNetwork
// (see that type's documentation).
//
// In the future this could be extended to model zone/cell topology, where the split
// always separates known datacenter regions rather than choosing randomly.
type PartitionedNetwork[I any, ID comparable] struct {
	Base          IndicatorDeliveryPolicy[I, ID]
	PartitionRate float64    // probability per tick that a new partition starts (0–1)
	MaxDuration   int64      // maximum partition length in ticks
	Rng           *rand.Rand // must be provided

	// groupAssignment holds each node's group for the current partition:
	//   key present, value true  → group A
	//   key present, value false → group B
	//   key absent               → not yet seen this partition (assigned lazily)
	// nil between partitions.
	groupAssignment map[ID]bool
	partitionEnd    int64 // tick when the current partition ends; 0 = no partition
	lastTick        int64 // last tick for which partition state was advanced
}

// NewPartitionedNetwork creates a PartitionedNetwork wrapping the given base policy.
func NewPartitionedNetwork[I any, ID comparable](
	base IndicatorDeliveryPolicy[I, ID],
	partitionRate float64,
	maxDuration int64,
	rng *rand.Rand,
) *PartitionedNetwork[I, ID] {
	return &PartitionedNetwork[I, ID]{
		Base:          base,
		PartitionRate: partitionRate,
		MaxDuration:   maxDuration,
		Rng:           rng,
		lastTick:      -1, // ensure advancePartition runs on tick 0
	}
}

// IsPartitioned reports whether a network partition is currently active.
func (p *PartitionedNetwork[I, ID]) IsPartitioned() bool {
	return p.partitionEnd > 0
}

func (p *PartitionedNetwork[I, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	var events []string

	// Advance partition state exactly once per tick.
	if args.CurrentTick > p.lastTick {
		events = append(events, p.advancePartition(args.CurrentTick, args.AllNodes)...)
		p.lastTick = args.CurrentTick
	}

	// During a partition, drop messages between the two groups.
	if p.partitionEnd > 0 {
		fromGroup, fromEvents := p.groupOf(args.FromNode)
		events = append(events, fromEvents...)
		targetGroup, targetEvents := p.groupOf(args.Target)
		events = append(events, targetEvents...)
		if fromGroup != targetGroup {
			return false, 0, events
		}
	}

	delivered, delay, baseEvents := p.Base.ScheduleDelivery(args)
	events = append(events, baseEvents...)
	return delivered, delay, events
}

// groupOf returns which group (true=A, false=B) the given node belongs to during
// the current partition, assigning it randomly on first encounter.
func (p *PartitionedNetwork[I, ID]) groupOf(id ID) (bool, []string) {
	if inA, assigned := p.groupAssignment[id]; assigned {
		return inA, nil
	}
	inA := p.Rng.Float64() < 0.5
	p.groupAssignment[id] = inA
	group := "B"
	if inA {
		group = "A"
	}
	return inA, []string{fmt.Sprintf("node %v assigned to partition group %s", id, group)}
}

// advancePartition ends any expired partition and possibly starts a new one.
func (p *PartitionedNetwork[I, ID]) advancePartition(currentTick int64, allNodes []ID) []string {
	var events []string
	if p.partitionEnd > 0 && currentTick >= p.partitionEnd {
		events = append(events, "partition ended")
		p.partitionEnd = 0
		p.groupAssignment = nil
	}
	if p.partitionEnd == 0 && p.Rng.Float64() < p.PartitionRate {
		duration := int64(1)
		if p.MaxDuration > 1 {
			duration = 1 + p.Rng.Int64N(p.MaxDuration)
		}
		p.partitionEnd = currentTick + duration
		p.groupAssignment = make(map[ID]bool)
		if len(allNodes) > 0 {
			var groupA, groupB []ID
			for _, id := range allNodes {
				if p.Rng.Float64() < 0.5 {
					p.groupAssignment[id] = true
					groupA = append(groupA, id)
				} else {
					p.groupAssignment[id] = false
					groupB = append(groupB, id)
				}
			}
			events = append(events, fmt.Sprintf("partition started (ends at tick %d): group A=%v, group B=%v", p.partitionEnd, groupA, groupB))
		} else {
			events = append(events, fmt.Sprintf("partition started (ends at tick %d)", p.partitionEnd))
		}
	}
	return events
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

func (p *PerSourcePolicy[I, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	if override, ok := p.Overrides[args.FromNode]; ok {
		return override.ScheduleDelivery(args)
	}
	return p.Default.ScheduleDelivery(args)
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

func (p *OrderedReliableNetwork[I, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	delay := int64(1)
	if p.MaxDelay > 0 {
		delay = 1 + p.Rng.Int64N(p.MaxDelay)
	}
	deliverAt := args.CurrentTick + delay

	// Enforce ordering: this message must arrive strictly after the previous one
	// on the same (from, to) channel.
	ch := orderedChannel[ID]{from: args.FromNode, to: args.Target}
	if last, ok := p.lastDelivery[ch]; ok && deliverAt <= last {
		deliverAt = last + 1
	}
	p.lastDelivery[ch] = deliverAt

	return true, deliverAt - args.CurrentTick, nil
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

func (p *UntilPolicy[I, R, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	// Check if we should switch (only check if we haven't switched yet)
	if !p.hasSwitched && p.UntilCondition.Eval(p.Sim) {
		p.hasSwitched = true
	}

	// Use appropriate policy
	if p.hasSwitched {
		return p.AfterPolicy.ScheduleDelivery(args)
	}
	return p.InitialPolicy.ScheduleDelivery(args)
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
func (seq *PolicySequence[I, R, ID]) ScheduleDelivery(args DeliveryArgs[I, ID]) (bool, int64, []string) {
	// Check if we should advance to next stage
	if seq.currentStageIndex < len(seq.stages)-1 {
		currentStage := seq.stages[seq.currentStageIndex]
		if currentStage.advanceWhen != nil && currentStage.advanceWhen.Eval(seq.sim) {
			seq.currentStageIndex++
		}
	}

	// Use current stage's policy
	return seq.stages[seq.currentStageIndex].policy.ScheduleDelivery(args)
}
