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
	"strings"
)

// QuorumCandidate carries the information the DurabilityPolicy needs to select
// a primary and sync replicas for a given election.
type QuorumCandidate struct {
	ID      NodeID
	Healthy bool   // postgres is running and reachable
	LSN     int64  // last known WAL position (0 = unknown)
	Zone    string // availability zone or cell (empty = unknown)
}

// Quorum is an opaque self-contained object created by DurabilityPolicy.ProposeQuorum
// or DurabilityPolicy.ReconstructQuorum. It captures a specific primary +
// sync-replica assignment and knows how to evaluate its own establish and
// revocation conditions without the consensus algorithm needing to understand
// quorum internals.
type Quorum interface {
	// Primary returns the node ID of the proposed primary.
	Primary() NodeID

	// SyncReplicas returns the nodes that must acknowledge writes.
	// Callers should treat this as a set (ordering undefined).
	SyncReplicas() []NodeID

	// IsEstablished reports whether the write quorum is durably established:
	// the primary and enough sync replicas have applied the Establish proposal.
	// appliers is the set of nodes that have applied the proposal.
	IsEstablished(appliers map[NodeID]bool) bool

	// IsRevoked reports whether the old write quorum is durably revoked:
	// enough nodes have applied the Revoke proposal that the old primary can no
	// longer collect the required acknowledgements for new writes.
	// appliers is the set of nodes that have applied the Revoke proposal.
	IsRevoked(appliers map[NodeID]bool) bool

	// IsWriteQuorum reports whether the given set of replicating nodes currently
	// satisfies the write-quorum requirement. Used by safety invariants to check
	// whether a primary can accept writes at a given instant.
	IsWriteQuorum(replicating []NodeID) bool

	// PostgresConfig returns the synchronous_standby_names configuration string
	// to set on the primary, e.g. "ANY 1 (pooler-2,pooler-3)".
	// An empty string means no synchronous replication is required.
	PostgresConfig() string
}

// DurabilityPolicy is invoked by OrchNode to propose and reconstruct quorums.
// The policy receives full context (health, LSN, zone, preferences) and returns
// self-contained Quorum objects that the consensus algorithm treats as opaque.
type DurabilityPolicy interface {
	// ProposeQuorum proposes a primary and sync-replica set given the current
	// cluster candidates. preferred is a hint for the desired primary; the policy
	// honours it when the preferred candidate is healthy.
	//
	// Returns the proposed Quorum and true if a valid quorum can be formed.
	// Returns nil, false if no valid quorum is possible (e.g., insufficient
	// healthy candidates).
	ProposeQuorum(candidates []QuorumCandidate, preferred NodeID) (Quorum, bool)

	// ReconstructQuorum creates a Quorum for a previously committed
	// (primary, syncReplicas) pair without re-running the election logic.
	// Used by OrchNode.learnEstablishedPrimary and safety invariants to evaluate
	// historical quorums using the original sync-replica set.
	ReconstructQuorum(primary NodeID, syncReplicas []NodeID) Quorum
}

// AnyNPolicy returns a DurabilityPolicy requiring any N synchronous
// acknowledgements from the sync-replica set before a write is durable.
// N=1 means any single sync replica suffices; N=0 disables synchronous replication.
//
// Example usage for a 3-node cluster (1 primary + 2 replicas):
//   - AnyNPolicy(1) — any one replica must ack; tolerates one replica failure.
//   - AnyNPolicy(2) — both replicas must ack; no data loss even if one replica crashes.
func AnyNPolicy(n int) DurabilityPolicy {
	return &anyNPolicy{n: n}
}

type anyNPolicy struct {
	n int
}

func (p *anyNPolicy) ProposeQuorum(candidates []QuorumCandidate, preferred NodeID) (Quorum, bool) {
	// Collect healthy candidates sorted by ID for determinism.
	healthy := make([]QuorumCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.Healthy {
			healthy = append(healthy, c)
		}
	}
	slices.SortFunc(healthy, func(a, b QuorumCandidate) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})

	if len(healthy) == 0 {
		return nil, false
	}

	// Select primary: use the preferred node if healthy, otherwise the first
	// healthy candidate in sorted order.
	primary := healthy[0].ID
	for _, c := range healthy {
		if c.ID == preferred {
			primary = c.ID
			break
		}
	}

	// Sync replicas: all healthy non-primary candidates in sorted order.
	var syncReplicas []NodeID
	for _, c := range healthy {
		if c.ID != primary {
			syncReplicas = append(syncReplicas, c.ID)
		}
	}

	return &anyNQuorum{primary: primary, syncReplicas: syncReplicas, n: p.n}, true
}

func (p *anyNPolicy) ReconstructQuorum(primary NodeID, syncReplicas []NodeID) Quorum {
	return &anyNQuorum{primary: primary, syncReplicas: syncReplicas, n: p.n}
}

type anyNQuorum struct {
	primary      NodeID
	syncReplicas []NodeID
	n            int
}

func (q *anyNQuorum) Primary() NodeID        { return q.primary }
func (q *anyNQuorum) SyncReplicas() []NodeID { return q.syncReplicas }

// IsEstablished returns true when the primary and at least n sync replicas have
// applied the Establish proposal.
func (q *anyNQuorum) IsEstablished(appliers map[NodeID]bool) bool {
	if !appliers[q.primary] {
		return false
	}
	acked := 0
	for _, id := range q.syncReplicas {
		if appliers[id] {
			acked++
		}
	}
	return acked >= q.n
}

// IsRevoked returns true when the old primary can no longer collect n acks:
// either the primary has applied the revoke itself, or fewer than n sync replicas
// remain that have NOT applied the revoke (making n acks impossible).
func (q *anyNQuorum) IsRevoked(appliers map[NodeID]bool) bool {
	if appliers[q.primary] {
		return true
	}
	remaining := 0
	for _, id := range q.syncReplicas {
		if !appliers[id] {
			remaining++
		}
	}
	return remaining < q.n
}

// IsWriteQuorum returns true when at least n of the replicating nodes are members
// of this quorum's sync-replica set.
func (q *anyNQuorum) IsWriteQuorum(replicating []NodeID) bool {
	if q.n == 0 {
		return true
	}
	replicatingSet := make(map[NodeID]bool, len(replicating))
	for _, id := range replicating {
		replicatingSet[id] = true
	}
	count := 0
	for _, id := range q.syncReplicas {
		if replicatingSet[id] {
			count++
		}
	}
	return count >= q.n
}

// PostgresConfig returns the synchronous_standby_names value to set on the primary.
// Returns an empty string when n=0 or there are no sync replicas.
func (q *anyNQuorum) PostgresConfig() string {
	if q.n == 0 || len(q.syncReplicas) == 0 {
		return ""
	}
	names := make([]string, len(q.syncReplicas))
	for i, id := range q.syncReplicas {
		names[i] = string(id)
	}
	return fmt.Sprintf("ANY %d (%s)", q.n, strings.Join(names, ","))
}
