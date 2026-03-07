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
// # Emergency path (not yet implemented — Stage 3)
//
// When the primary becomes unreachable the coordinator will run a three-phase
// election (Begin → Revoke → Establish) to appoint a new primary.
//
// # State
//
// All state is ephemeral — a restarted coordinator re-learns the cluster by
// processing PoolerStatusIndicator and PoolerDiscoveredIndicator updates.
type CoordNode struct {
	id           NodeID
	targetPolicy AckPolicy

	// known tracks each pooler the coord has been told about. Values are
	// updated each time a PoolerStatusIndicator arrives.
	known map[NodeID]*knownPooler

	// pendingWrite is set when a WritePolicyRequest has been emitted and we
	// are waiting for a WritePolicyResponseIndicator. Only one write is in
	// flight at a time.
	pendingWrite *pendingPolicyWrite
}

type knownPooler struct {
	state      PoolerPersistentState
	pgStatus   PostgresStatus
	properties NodeProperties
}

type pendingPolicyWrite struct {
	target NodeID
	rules  DurabilityRules
}

// NewCoordNode creates a coordinator node.
// targetPolicy is the desired AckPolicy to work toward as the cohort
// grows (e.g. AnyNPolicy(2) for a 3-node HA cluster).
func NewCoordNode(id NodeID, targetPolicy AckPolicy) *CoordNode {
	return &CoordNode{
		id:           id,
		targetPolicy: targetPolicy,
		known:        make(map[NodeID]*knownPooler),
	}
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
func (c *CoordNode) Step(_ int64, indicators []Indicator) []Request {
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
		return
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

	// Decide on one type of change per write: adds or removes (not both).
	// For Stage 1 we only handle adds. Removes are a TODO.
	observers := c.observers(currentRules)
	if len(observers) == 0 {
		return nil // cohort is already up to date
	}

	// Add all observers in one write.
	newMembers := append(slices.Clone(currentRules.Members), observers...)
	newRules := DurabilityRules{
		Seq:     currentRules.Seq + 1,
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
// capped at the configured target policy. Uses AnyNPolicy(len(members)-1) as
// the intermediate policy when the target is not yet achievable.
func (c *CoordNode) policyForMembers(members []CohortMember) AckPolicy {
	if c.targetPolicy.IsAchievable(members) {
		return c.targetPolicy
	}
	return AnyNPolicy(len(members) - 1)
}
