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

	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// PendingDelivery is a buffered indicator with its routing metadata.
type PendingDelivery[I any, ID comparable] struct {
	From ID
	To   ID
	Ind  I
}

// IndicatorDeliveryManager manages indicator delivery between simulation ticks.
// Unlike a per-message policy, a delivery manager owns its own internal queue:
// new indicators are submitted via Enqueue, and ready ones are returned by
// Deliver on each tick.
//
// Drain removes all buffered indicators regardless of their scheduled delivery
// time. SequenceDeliveryManager uses this to transfer in-flight messages to
// the next stage when the advance condition becomes true.
type IndicatorDeliveryManager[I any, ID comparable] interface {
	// Enqueue submits an indicator for future delivery. The manager decides
	// whether to drop, delay, or duplicate it. Returns dropped=true if the
	// indicator was discarded, plus any trace events (e.g. a first-seen drop).
	Enqueue(tick int64, from, to ID, ind I) (dropped bool, events []string)

	// Deliver returns all indicators that are ready at this tick, plus any that
	// were dropped at delivery time (e.g. due to a network partition).
	// allNodes is the current set of registered node IDs, used by partition
	// implementations for eager group assignment.
	Deliver(tick int64, allNodes []ID) (delivered []PendingDelivery[I, ID], droppedAtDelivery []PendingDelivery[I, ID], events []string)

	// Drain removes and returns all buffered (not-yet-delivered) indicators.
	Drain() []PendingDelivery[I, ID]
}

// chanKey identifies a directed channel between two nodes.
type chanKey[ID comparable] struct{ from, to ID }

// ChaosDeliveryManager implements IndicatorDeliveryManager with configurable
// chaos (drops, delays, duplicates) and optional network partitions.
//
// Indicators are enqueued into per-channel queues with chaos applied at Enqueue
// time. Network partitions are evaluated at Deliver time, so an in-flight
// indicator may be dropped if a partition starts before it is delivered.
//
// The zero value is a reliable delivery manager: no drops, no extra delay
// beyond 1 tick, no duplicates, no partitions (all ChaosParams fields are
// zero/nil, so Decide returns the reliable defaults).
type ChaosDeliveryManager[I any, ID comparable] struct {
	Chaos                ChaosParams // Drop/delay/dup parameters; Rng must be non-nil for chaos
	PartitionRate        float64     // Probability per Deliver call that a new partition starts (0 = disabled)
	MaxPartitionDuration int64       // Max partition duration in ticks (used when PartitionRate > 0)

	channels        map[chanKey[ID]]*chaosQueue[PendingDelivery[I, ID]]
	groupAssignment map[ID]bool // nil between partitions
	partitionEnd    int64       // tick when current partition ends; 0 = no partition
}

// IsPartitioned reports whether a network partition is currently active.
func (m *ChaosDeliveryManager[I, ID]) IsPartitioned() bool {
	return m.partitionEnd > 0
}

func (m *ChaosDeliveryManager[I, ID]) channel(from, to ID) *chaosQueue[PendingDelivery[I, ID]] {
	if m.channels == nil {
		m.channels = make(map[chanKey[ID]]*chaosQueue[PendingDelivery[I, ID]])
	}
	key := chanKey[ID]{from, to}
	q, ok := m.channels[key]
	if !ok {
		q = &chaosQueue[PendingDelivery[I, ID]]{chaos: m.Chaos}
		m.channels[key] = q
	}
	return q
}

func (m *ChaosDeliveryManager[I, ID]) Enqueue(tick int64, from, to ID, ind I) (dropped bool, events []string) {
	pd := PendingDelivery[I, ID]{From: from, To: to, Ind: ind}
	if !m.channel(from, to).push(pd, tick) {
		return true, nil
	}
	return false, nil
}

func (m *ChaosDeliveryManager[I, ID]) Deliver(tick int64, allNodes []ID) (delivered []PendingDelivery[I, ID], droppedAtDelivery []PendingDelivery[I, ID], events []string) {
	if m.PartitionRate > 0 {
		events = append(events, m.advancePartition(tick, allNodes)...)
	}
	for _, q := range sortedmaps.ByStr(m.channels) {
		for _, pd := range q.pull(tick) {
			if m.partitionEnd > 0 {
				fromGroup, ev := m.groupOf(pd.From)
				events = append(events, ev...)
				toGroup, ev2 := m.groupOf(pd.To)
				events = append(events, ev2...)
				if fromGroup != toGroup {
					droppedAtDelivery = append(droppedAtDelivery, pd)
					continue
				}
			}
			delivered = append(delivered, pd)
		}
	}
	return delivered, droppedAtDelivery, events
}

func (m *ChaosDeliveryManager[I, ID]) Drain() []PendingDelivery[I, ID] {
	var all []PendingDelivery[I, ID]
	for _, q := range sortedmaps.ByStr(m.channels) {
		all = append(all, q.drain()...)
	}
	return all
}

func (m *ChaosDeliveryManager[I, ID]) groupOf(id ID) (bool, []string) {
	if inA, ok := m.groupAssignment[id]; ok {
		return inA, nil
	}
	inA := m.Chaos.Rng.Float64() < 0.5
	m.groupAssignment[id] = inA
	group := "B"
	if inA {
		group = "A"
	}
	return inA, []string{fmt.Sprintf("node %v assigned to partition group %s", id, group)}
}

func (m *ChaosDeliveryManager[I, ID]) advancePartition(tick int64, allNodes []ID) []string {
	var events []string
	if m.partitionEnd > 0 && tick >= m.partitionEnd {
		events = append(events, "partition ended")
		m.partitionEnd = 0
		m.groupAssignment = nil
	}
	if m.partitionEnd == 0 && m.Chaos.Rng != nil && m.Chaos.Rng.Float64() < m.PartitionRate {
		duration := int64(1)
		if m.MaxPartitionDuration > 1 {
			duration = 1 + m.Chaos.Rng.Int64N(m.MaxPartitionDuration)
		}
		m.partitionEnd = tick + duration
		m.groupAssignment = make(map[ID]bool)
		if len(allNodes) > 0 {
			var groupA, groupB []ID
			for _, id := range allNodes {
				if m.Chaos.Rng.Float64() < 0.5 {
					m.groupAssignment[id] = true
					groupA = append(groupA, id)
				} else {
					m.groupAssignment[id] = false
					groupB = append(groupB, id)
				}
			}
			events = append(events, fmt.Sprintf("partition started (ends at tick %d): group A=%v, group B=%v", m.partitionEnd, groupA, groupB))
		} else {
			events = append(events, fmt.Sprintf("partition started (ends at tick %d)", m.partitionEnd))
		}
	}
	return events
}

// PerSourceDeliveryManager dispatches to different delivery managers based on
// which node sent the indicator. This lets callers give some sources (e.g. a
// discovery node) reliable ordered delivery while applying chaos to all other
// traffic.
//
// Deliver is called on all sub-managers every tick so each manager can advance
// its own internal state (e.g. partition state) regardless of whether it has
// pending indicators.
type PerSourceDeliveryManager[I any, ID comparable] struct {
	Default   IndicatorDeliveryManager[I, ID]
	Overrides map[ID]IndicatorDeliveryManager[I, ID]
}

func (p *PerSourceDeliveryManager[I, ID]) Enqueue(tick int64, from, to ID, ind I) (dropped bool, events []string) {
	if override, ok := p.Overrides[from]; ok {
		return override.Enqueue(tick, from, to, ind)
	}
	return p.Default.Enqueue(tick, from, to, ind)
}

func (p *PerSourceDeliveryManager[I, ID]) Deliver(tick int64, allNodes []ID) (delivered []PendingDelivery[I, ID], droppedAtDelivery []PendingDelivery[I, ID], events []string) {
	delivered, droppedAtDelivery, events = p.Default.Deliver(tick, allNodes)
	for _, m := range sortedmaps.ByStr(p.Overrides) {
		d, dr, ev := m.Deliver(tick, allNodes)
		delivered = append(delivered, d...)
		droppedAtDelivery = append(droppedAtDelivery, dr...)
		events = append(events, ev...)
	}
	return delivered, droppedAtDelivery, events
}

func (p *PerSourceDeliveryManager[I, ID]) Drain() []PendingDelivery[I, ID] {
	all := p.Default.Drain()
	for _, m := range sortedmaps.ByStr(p.Overrides) {
		all = append(all, m.Drain()...)
	}
	return all
}

// UntilDeliveryManager uses InitialManager until a condition becomes true,
// then permanently switches to AfterManager. On switch, all in-flight indicators
// are drained from InitialManager and re-enqueued in AfterManager so they are
// not lost.
type UntilDeliveryManager[I any, R any, ID comparable] struct {
	UntilCondition Condition[I, R, ID]
	InitialManager IndicatorDeliveryManager[I, ID]
	AfterManager   IndicatorDeliveryManager[I, ID]
	Sim            *Simulator[I, R, ID]
	hasSwitched    bool
}

func (u *UntilDeliveryManager[I, R, ID]) Enqueue(tick int64, from, to ID, ind I) (dropped bool, events []string) {
	if u.hasSwitched {
		return u.AfterManager.Enqueue(tick, from, to, ind)
	}
	return u.InitialManager.Enqueue(tick, from, to, ind)
}

func (u *UntilDeliveryManager[I, R, ID]) Deliver(tick int64, allNodes []ID) (delivered []PendingDelivery[I, ID], droppedAtDelivery []PendingDelivery[I, ID], events []string) {
	if !u.hasSwitched && u.UntilCondition.Eval(u.Sim) {
		drained := u.InitialManager.Drain()
		u.hasSwitched = true
		for _, pd := range drained {
			u.AfterManager.Enqueue(tick, pd.From, pd.To, pd.Ind)
		}
	}
	if u.hasSwitched {
		return u.AfterManager.Deliver(tick, allNodes)
	}
	return u.InitialManager.Deliver(tick, allNodes)
}

func (u *UntilDeliveryManager[I, R, ID]) Drain() []PendingDelivery[I, ID] {
	if u.hasSwitched {
		return u.AfterManager.Drain()
	}
	return u.InitialManager.Drain()
}

// SequenceDeliveryManager manages a sequence of delivery managers with
// observable stage transitions. Each stage has a manager and a condition for
// when to advance to the next stage.
//
// When the advance condition becomes true at the start of a Deliver call,
// all in-flight indicators are drained from the current stage's manager and
// re-enqueued in the next stage's manager so they are not lost.
//
// Use AppendStage to add stages after the initial one. The returned Condition
// is true while that stage is active, and can be used in sim.Sometimes or
// other assertions.
type SequenceDeliveryManager[I any, R any, ID comparable] struct {
	stages            []managerStage[I, R, ID]
	currentStageIndex int
	sim               *Simulator[I, R, ID]
}

type managerStage[I any, R any, ID comparable] struct {
	manager        IndicatorDeliveryManager[I, ID]
	advanceWhen    Condition[I, R, ID] // nil for the final stage
	stageCondition *StageActiveCondition[I, R, ID]
}

// StageActiveCondition is true while a specific stage in a SequenceDeliveryManager
// is active.
type StageActiveCondition[I any, R any, ID comparable] struct {
	seq        *SequenceDeliveryManager[I, R, ID]
	stageIndex int
	stageName  string
}

func (c *StageActiveCondition[I, R, ID]) Eval(_ *Simulator[I, R, ID]) bool {
	return c.seq.currentStageIndex == c.stageIndex
}

func (c *StageActiveCondition[I, R, ID]) Name() string {
	return "stage_active_" + c.stageName
}

func (c *StageActiveCondition[I, R, ID]) Describe(_ *Simulator[I, R, ID]) string {
	return fmt.Sprintf("delivery stage '%s' is active (stage %d of %d)", c.stageName, c.stageIndex+1, len(c.seq.stages))
}

// NewSequenceDeliveryManager creates a sequence starting with the given initial manager.
func NewSequenceDeliveryManager[I any, R any, ID comparable](sim *Simulator[I, R, ID], initialManager IndicatorDeliveryManager[I, ID], stageName string) *SequenceDeliveryManager[I, R, ID] {
	seq := &SequenceDeliveryManager[I, R, ID]{sim: sim}
	stageCondition := &StageActiveCondition[I, R, ID]{
		seq:        seq,
		stageIndex: 0,
		stageName:  stageName,
	}
	seq.stages = append(seq.stages, managerStage[I, R, ID]{
		manager:        initialManager,
		stageCondition: stageCondition,
	})
	return seq
}

// AppendStage adds a new stage. The sequence advances from the previous last
// stage to this one when advanceWhen becomes true at Deliver time. Returns a
// Condition that is true while this stage is active.
func (seq *SequenceDeliveryManager[I, R, ID]) AppendStage(manager IndicatorDeliveryManager[I, ID], advanceWhen Condition[I, R, ID], stageName string) Condition[I, R, ID] {
	if len(seq.stages) > 0 {
		seq.stages[len(seq.stages)-1].advanceWhen = advanceWhen
	}
	stageIndex := len(seq.stages)
	stageCondition := &StageActiveCondition[I, R, ID]{
		seq:        seq,
		stageIndex: stageIndex,
		stageName:  stageName,
	}
	seq.stages = append(seq.stages, managerStage[I, R, ID]{
		manager:        manager,
		stageCondition: stageCondition,
	})
	return stageCondition
}

func (seq *SequenceDeliveryManager[I, R, ID]) Enqueue(tick int64, from, to ID, ind I) (dropped bool, events []string) {
	return seq.stages[seq.currentStageIndex].manager.Enqueue(tick, from, to, ind)
}

func (seq *SequenceDeliveryManager[I, R, ID]) Deliver(tick int64, allNodes []ID) (delivered []PendingDelivery[I, ID], droppedAtDelivery []PendingDelivery[I, ID], events []string) {
	// Advance through stages whose conditions are now true (may skip multiple stages
	// in one tick if conditions fire simultaneously).
	for seq.currentStageIndex < len(seq.stages)-1 {
		current := seq.stages[seq.currentStageIndex]
		if current.advanceWhen == nil || !current.advanceWhen.Eval(seq.sim) {
			break
		}
		drained := current.manager.Drain()
		seq.currentStageIndex++
		next := seq.stages[seq.currentStageIndex].manager
		for _, pd := range drained {
			next.Enqueue(tick, pd.From, pd.To, pd.Ind)
		}
	}
	return seq.stages[seq.currentStageIndex].manager.Deliver(tick, allNodes)
}

func (seq *SequenceDeliveryManager[I, R, ID]) Drain() []PendingDelivery[I, ID] {
	return seq.stages[seq.currentStageIndex].manager.Drain()
}
