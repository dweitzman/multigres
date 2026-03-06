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
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// CoordNode is the coordinator state machine.
//
// # Normal path — cohort expansion
//
// The coordinator monitors pooler statuses, identifies observers (poolers that
// are replicating but not yet in the cohort), and expands the cohort by writing
// DurabilityPolicyRecord updates to the primary via compare-and-swap.
//
// Adds and removes are always separate writes. This is required because the
// correct ordering of synchronous_standby_names changes relative to the policy
// record write differs between the two cases:
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
	targetPolicy DurabilityPolicy

	// known tracks each pooler the coord has been told about. Values are
	// updated each time a PoolerStatusIndicator arrives.
	known map[NodeID]*knownPooler

	// pendingWrite is set when a WritePolicyRequest has been emitted and we
	// are waiting for a WritePolicyResponseIndicator. Only one write is in
	// flight at a time.
	pendingWrite *pendingPolicyWrite

	// nextPolicySeq is a monotonic counter used to generate unique PolicyIDs.
	nextPolicySeq int64
}

type knownPooler struct {
	state    PoolerPersistentState
	pgStatus PostgresStatus
}

type pendingPolicyWrite struct {
	target NodeID
	record DurabilityPolicyRecord
}

// NewCoordNode creates a coordinator node.
// targetPolicy is the desired DurabilityPolicy to work toward as the cohort
// grows (e.g. AnyNPolicy(2) for a 3-node HA cluster).
func NewCoordNode(id NodeID, targetPolicy DurabilityPolicy) *CoordNode {
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
		// The write succeeded. Update our cached view of the primary's policy
		// so the next write uses the correct PreviousID without waiting for a
		// status broadcast.
		if p, ok := c.known[ind.FromPooler]; ok {
			record := c.pendingWrite.record
			p.state.Policy = &record
		}
	} else {
		// CAS mismatch: the primary's current policy is ind.CurrentID, not what
		// we thought. Only update our cache if we don't already have a fresher
		// view from an out-of-band status update (which would have a matching ID).
		if p, ok := c.known[ind.FromPooler]; ok {
			if p.state.Policy == nil || p.state.Policy.ID != ind.CurrentID {
				// We don't have the full record for ind.CurrentID yet. Clear our
				// cached policy and wait for a PoolerStatusIndicator that carries
				// the full record before retrying.
				p.state.Policy = nil
			}
			// If p.state.Policy.ID == ind.CurrentID we already have up-to-date
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

	currentPolicy := primaryKnown.state.Policy
	if currentPolicy == nil {
		return nil // primary has no policy yet; wait for status
	}

	// Decide on one type of change per write: adds or removes (not both).
	// For Stage 1 we only handle adds. Removes are a TODO.
	observers := c.observers(currentPolicy.CohortMembers)
	if len(observers) == 0 {
		return nil // cohort is already up to date
	}

	// Add all observers in one write.
	newCohort := append(slices.Clone(currentPolicy.CohortMembers), observers...)
	newPolicy := c.policyForSize(len(newCohort))

	c.nextPolicySeq++
	newRecord := DurabilityPolicyRecord{
		ID:            PolicyID(fmt.Sprintf("policy-%d", c.nextPolicySeq)),
		PreviousID:    currentPolicy.ID,
		CohortMembers: newCohort,
		Policy:        newPolicy,
	}

	c.pendingWrite = &pendingPolicyWrite{
		target: primaryID,
		record: newRecord,
	}

	return []Request{WritePolicyRequest{
		TargetPooler: primaryID,
		FromCoord:    c.id,
		Record:       newRecord,
	}}
}

// findPrimary returns the NodeID and knownPooler for the active primary, or the
// zero ID and nil if none is found.
//
// TODO(stage3): strengthen this. Ideally we find a pooler whose current policy
// record satisfies its own durability requirements (i.e. the write quorum of
// the policy is met by the replicas currently known to be streaming), which is
// strong evidence of a working quorum. If the primary is unreachable but a
// validating quorum of replicas agree on its identity and show successful
// replication, that is also good signal. For now, a simple role+health check
// suffices.
func (c *CoordNode) findPrimary() (NodeID, *knownPooler) {
	for id, p := range sortedmaps.All(c.known) {
		if p.state.Role == RolePrimary && p.pgStatus == PostgresRunning {
			return id, p
		}
	}
	return "", nil
}

// observers returns known poolers not listed in the current cohort, sorted
// for deterministic output.
func (c *CoordNode) observers(cohort []NodeID) []NodeID {
	var result []NodeID
	for _, id := range sortedmaps.Keys(c.known) {
		if !slices.Contains(cohort, id) {
			result = append(result, id)
		}
	}
	return result
}

// policyForSize returns the best achievable policy for the given cohort size,
// capped at the configured target policy. Uses AnyNPolicy(cohortSize-1) as the
// intermediate policy when the target is not yet achievable.
func (c *CoordNode) policyForSize(cohortSize int) DurabilityPolicy {
	if c.targetPolicy.IsAchievable(cohortSize) {
		return c.targetPolicy
	}
	return AnyNPolicy(cohortSize - 1)
}
