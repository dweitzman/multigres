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

package consensus

import (
	"slices"

	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// CoordNode is the coordinator state machine.
//
// # Normal path — cohort expansion
//
// The coordinator monitors pooler statuses, identifies observers (poolers that
// are replicating but not yet in the cohort), and expands the cohort by writing
// DurabilityRules updates to the primary via compare-and-swap.
//
// Adds and removes are always separate writes. This is required because the
// correct ordering of synchronous_standby_names changes relative to the rules
// write differs between the two cases:
//   - Adding cohort members: update sync settings AFTER the write (new replicas
//     don't need to ack the write that adds them).
//   - Removing cohort members: update sync settings BEFORE the write (must not
//     relax the quorum before the removal is durable).
//
// # Manual mode
//
// When targetPolicy is nil the coordinator operates in manual mode: it never
// autonomously adds observed replicas to the cohort.
//
// # Emergency path (not yet implemented — Stage 3)
//
// When the primary becomes unreachable the coordinator will run a two-phase
// election (Revoke → Establish) to appoint a new primary.
//
// # State
//
// All state is ephemeral — a restarted coordinator re-learns the cluster by
// processing PoolerStatusIndicator and PoolerDiscoveredIndicator updates.
type CoordNode struct {
	id           NodeID
	targetPolicy DurabilityPolicy // nil = manual mode

	// known tracks each pooler the coord has been told about. Values are
	// updated each time a PoolerStatusIndicator arrives.
	known map[NodeID]*knownPooler

	// pendingWrite is set when a WritePolicyRequest has been emitted and we
	// are waiting for a WritePolicyResponseIndicator. Only one write is in
	// flight at a time.
	pendingWrite *pendingPolicyWrite

	// healthTimeoutTicks is the number of ticks without a PoolerStatusIndicator
	// before a pooler is considered unreachable. Zero disables timeout-based
	// staleness checking (suitable for simulation tests where poolers only
	// broadcast on state change).
	healthTimeoutTicks int64
}

type knownPooler struct {
	state          PoolerPersistentState
	pgStatus       PostgresStatus
	properties     NodeProperties
	lastStatusTick int64 // tick at which the last PoolerStatusIndicator was received
}

type pendingPolicyWrite struct {
	target NodeID
	rules  DurabilityRules
}

// NewCoordNode creates a coordinator node.
// targetPolicy is the desired DurabilityPolicy to work toward as the cohort
// grows (e.g. AtLeastPolicy(3) for a 3-node HA cluster).
func NewCoordNode(id NodeID, targetPolicy DurabilityPolicy) *CoordNode {
	return &CoordNode{
		id:           id,
		targetPolicy: targetPolicy,
		known:        make(map[NodeID]*knownPooler),
	}
}

// SetHealthTimeout configures a staleness threshold: if a pooler has not sent
// a status update within ticks ticks, it is considered unreachable.
// Zero disables timeout-based staleness (the default).
func (c *CoordNode) SetHealthTimeout(ticks int64) {
	c.healthTimeoutTicks = ticks
}

// ClusterView is the coordinator's current best-known state of the cluster,
// computed from accumulated PoolerStatusIndicators.
type ClusterView struct {
	// HighestQuorumRules is the highest-Seq DurabilityRules for which the
	// coordinator has confirmed a write quorum: enough cohort members have
	// reported applying this version (or a later one) to satisfy the rules'
	// DurabilityPolicy. This is the last known-good state of the cluster.
	// Nil if no version has confirmed quorum.
	HighestQuorumRules *DurabilityRules

	// HighestSeenRules is the highest-Seq DurabilityRules reported by any
	// known pooler, regardless of quorum. Nil if no rules have been seen.
	// If HighestSeenRules.Seq > HighestQuorumRules.Seq, a partial leader-driven
	// rule change exists and must be propagated before establishing a new primary.
	HighestSeenRules *DurabilityRules

	// PrimaryHealthy is true when the best-known primary is currently reachable.
	// The "best-known" primary is HighestQuorumRules.Primary when a quorum-confirmed
	// version exists, falling back to HighestSeenRules.Primary otherwise. The
	// primary is considered reachable when postgres is running and its last status
	// is within the configured health timeout.
	//
	// This is the key flag for deciding between the normal path (PrimaryHealthy=true)
	// and the emergency failover path (PrimaryHealthy=false).
	PrimaryHealthy bool
}

// ClusterView returns the coordinator's current cluster state assessment.
// tick is used for health-staleness checks when a health timeout is configured.
func (c *CoordNode) ClusterView(tick int64) ClusterView {
	highestSeen, highestQuorum := c.computeRulesVersions()

	// Identify the best-known primary: prefer the quorum-confirmed version,
	// fall back to the highest seen when quorum is not yet confirmed.
	var primaryID NodeID
	if highestQuorum != nil {
		primaryID = highestQuorum.Primary
	} else if highestSeen != nil {
		primaryID = highestSeen.Primary
	}

	var primaryHealthy bool
	if primaryID != "" {
		if p, ok := c.known[primaryID]; ok {
			primaryHealthy = c.isHealthy(p, tick)
		}
	}
	return ClusterView{
		HighestSeenRules:   highestSeen,
		HighestQuorumRules: highestQuorum,
		PrimaryHealthy:     primaryHealthy,
	}
}

// computeRulesVersions scans all known poolers and computes the two key
// DurabilityRules views: the highest-Seq version seen from any pooler, and the
// highest-Seq version for which write quorum is confirmed.
func (c *CoordNode) computeRulesVersions() (highestSeen, highestQuorum *DurabilityRules) {
	// Build a map from Seq → rules record using the rules reported by each pooler.
	// All poolers with the same Seq hold the same immutable record.
	rulesBySeq := make(map[int64]*DurabilityRules)
	for _, p := range sortedmaps.Values(c.known) {
		r := p.state.Rules
		if r == nil {
			continue
		}
		if _, exists := rulesBySeq[r.Seq]; !exists {
			rulesBySeq[r.Seq] = r
		}
		if highestSeen == nil || r.Seq > highestSeen.Seq {
			highestSeen = r
		}
	}
	if highestSeen == nil {
		return nil, nil
	}

	// Walk versions from highest to lowest, returning the first with confirmed quorum.
	for seq := highestSeen.Seq; seq >= 1; seq-- {
		r, ok := rulesBySeq[seq]
		if !ok {
			continue // coordinator has not seen this version
		}
		if c.isDurable(r) {
			highestQuorum = r
			break
		}
	}
	return highestSeen, highestQuorum
}

// isDurable returns true if enough cohort members have reported applying
// rules.Seq (or a later version) to satisfy rules.Policy.IsDurable. The primary
// is included in the acking set since it commits locally before propagating via
// WAL — this correctly handles AtLeast(1) where the primary alone is sufficient.
func (c *CoordNode) isDurable(rules *DurabilityRules) bool {
	if rules.Policy == nil {
		return true // nil policy: no acks needed
	}
	var acking []CohortMember
	for _, m := range rules.Members {
		p, ok := c.known[m.ID]
		if !ok || p.state.Rules == nil || p.state.Rules.Seq < rules.Seq {
			continue
		}
		acking = append(acking, m)
	}
	return rules.Policy.IsDurable(rules.Members, acking)
}

// isHealthy returns true if a pooler is currently considered reachable.
// A pooler is unhealthy when its postgres is stopped or (if a health timeout
// is configured) when it has not sent a status update within the timeout window.
func (c *CoordNode) isHealthy(p *knownPooler, tick int64) bool {
	if p.pgStatus == PostgresStopped {
		return false
	}
	if c.healthTimeoutTicks > 0 && p.lastStatusTick > 0 && tick-p.lastStatusTick > c.healthTimeoutTicks {
		return false
	}
	return true
}

// ID returns the coordinator node's unique identifier.
func (c *CoordNode) ID() NodeID {
	return c.id
}

// KnownPoolerIDs returns the IDs of all poolers the coordinator currently
// tracks. Used by simulation infrastructure to reconcile discovery state.
func (c *CoordNode) KnownPoolerIDs() []NodeID {
	return sortedmaps.Keys(c.known)
}

// Step processes all indicators that arrived this tick and returns requests.
func (c *CoordNode) Step(tick int64, indicators []Indicator) []Request {
	for _, ind := range indicators {
		switch v := ind.(type) {
		case PoolerDiscoveredIndicator:
			if _, ok := c.known[v.PoolerID]; !ok {
				c.known[v.PoolerID] = &knownPooler{}
			}
		case PoolerRemovedIndicator:
			delete(c.known, v.PoolerID)
		case PoolerStatusIndicator:
			if p, ok := c.known[v.PoolerID]; ok {
				p.state = v.State
				p.pgStatus = v.PostgresStatus
				p.properties = v.Properties
				p.lastStatusTick = tick
			}
		case WritePolicyResponseIndicator:
			c.handleWriteResponse(v)
		}
	}

	return c.advance()
}

// handleWriteResponse processes the primary's response to a WritePolicyRequest.
func (c *CoordNode) handleWriteResponse(ind WritePolicyResponseIndicator) {
	if c.pendingWrite == nil || ind.FromPooler != c.pendingWrite.target {
		return // stale or unexpected response
	}

	if ind.Accepted {
		// The write succeeded. Update our cached view of the primary's rules
		// so the next write uses the correct Seq without waiting for a status
		// broadcast.
		if p, ok := c.known[ind.FromPooler]; ok {
			rules := c.pendingWrite.rules
			p.state.Rules = &rules
		}
	} else {
		// CAS mismatch: the primary's current seq is ind.CurrentSeq, not what
		// we thought. Only update our cache if we don't already have a fresher
		// view from an out-of-band status update (which would have a matching seq).
		if p, ok := c.known[ind.FromPooler]; ok {
			if p.state.Rules == nil || p.state.Rules.Seq != ind.CurrentSeq {
				// We don't have the full rules for ind.CurrentSeq yet. Clear our
				// cached rules and wait for a PoolerStatusIndicator that carries
				// the full record before retrying.
				p.state.Rules = nil
			}
			// If p.state.Rules.Seq == ind.CurrentSeq we already have up-to-date
			// information and can retry on the next advance() call.
		}
	}
	c.pendingWrite = nil
}

// advance decides whether to emit a WritePolicyRequest. Called at the end of
// every Step() after all indicators have been processed.
func (c *CoordNode) advance() []Request {
	if c.pendingWrite != nil {
		return nil // write in flight; wait for response
	}

	primaryID, primaryKnown := c.findPrimary()
	if primaryKnown == nil {
		return nil
	}

	currentRules := primaryKnown.state.Rules
	if currentRules == nil {
		return nil // primary has no rules yet; wait for status
	}

	// Manual mode: never auto-add observers.
	if c.targetPolicy == nil {
		return nil
	}

	// Autonomous expansion: add all observed replicas not yet in the cohort.
	observers := c.observers(currentRules)
	if len(observers) == 0 {
		return nil // cohort is already up to date
	}

	// Add all observers in one write.
	newMembers := append(slices.Clone(currentRules.Members), observers...)
	newRules := DurabilityRules{
		Seq:     currentRules.Seq + 1,
		Primary: primaryID,
		Members: newMembers,
		Policy:  c.policyForMembers(newMembers),
	}

	c.pendingWrite = &pendingPolicyWrite{
		target: primaryID,
		rules:  newRules,
	}

	return []Request{WritePolicyRequest{
		TargetPooler: primaryID,
		FromCoord:    c.id,
		Rules:        newRules,
	}}
}

// findPrimary returns the NodeID and knownPooler for the active primary, or the
// zero ID and nil if none is found.
//
// TODO(stage3): strengthen this. Ideally we find a pooler whose current rules
// satisfy its own durability requirements (i.e. the write quorum of the policy is
// met by the replicas currently known to be streaming), which is strong evidence of
// a working quorum. For now, a simple role+health check suffices.
func (c *CoordNode) findPrimary() (NodeID, *knownPooler) {
	for id, p := range sortedmaps.All(c.known) {
		if p.state.Role == RolePrimary && p.pgStatus == PostgresRunning {
			return id, p
		}
	}
	return "", nil
}

// observers returns known poolers not listed in the current cohort as
// CohortMembers (with their cached properties), sorted for deterministic output.
func (c *CoordNode) observers(currentRules *DurabilityRules) []CohortMember {
	inCohort := make(map[NodeID]bool)
	for _, m := range currentRules.Members {
		inCohort[m.ID] = true
	}

	var result []CohortMember
	for _, id := range sortedmaps.Keys(c.known) {
		if !inCohort[id] {
			p := c.known[id]
			result = append(result, CohortMember{
				ID:         id,
				Properties: p.properties,
			})
		}
	}
	return result
}

// policyForMembers returns the best achievable policy for the given cohort,
// capped at the configured target policy. Uses AtLeastPolicy(len(members)) as
// the intermediate policy when the target is not yet achievable: it requires
// all current members to ack, providing the strongest guarantee possible until
// the target policy becomes achievable.
func (c *CoordNode) policyForMembers(members []CohortMember) DurabilityPolicy {
	if c.targetPolicy.IsAchievable(members) {
		return c.targetPolicy
	}
	return AtLeastPolicy(len(members))
}
