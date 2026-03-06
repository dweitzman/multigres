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

// Package consensus implements a pure state machine library for managing durability
// configuration and primary selection in a PostgreSQL pooler cluster.
//
// Two node types participate in the simulation:
//
//   - PoolerNode: manages a single PostgreSQL instance. Persists its committed state
//     durably via PoolerStorage. The primary PoolerNode is the source of truth for the
//     cluster's DurabilityPolicyRecord (cohort membership and ack policy); replicas learn
//     about policy changes via simulated WAL replication.
//
//   - CoordNode: coordinator (orch service). Stateless across restarts. Watches pooler
//     status, discovers observers (non-cohort poolers), and drives cohort expansion and
//     durability policy upgrades by writing DurabilityPolicyRecord updates to the primary.
//     Uses emergency failover (coordinator elections) only when the primary is unreachable.
//
// Both implement dstsim.Node[Indicator, Request, NodeID] and can run in the same
// deterministic simulator.
//
// Normal path — cohort and policy changes via WAL:
//
//	CoordNode sees observer → sends WritePolicyRequest to primary → primary commits and
//	propagates WAL → replicas apply WAL and update their Policy → CoordNode observes
//	updated status.
//
// Emergency path (not yet implemented) — coordinator elections:
//
//	CoordNode detects unreachable primary → runs Begin → Revoke → Establish to appoint
//	a new primary, then returns to the normal path.
package consensus

// NodeID identifies any node in the simulation: both coordinator nodes and pooler nodes.
// Using a single ID type lets all node kinds live in one dstsim.Simulator instance.
type NodeID string

// PoolerRole is the role a pooler currently holds in the cluster.
type PoolerRole int8

const (
	RoleUnknown PoolerRole = 0
	RolePrimary PoolerRole = 1
	RoleReplica PoolerRole = 2
)

func (r PoolerRole) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	case RoleReplica:
		return "replica"
	default:
		return "unknown"
	}
}

// PostgresStatus is the operational state of a pooler's PostgreSQL instance.
// Reported in status updates alongside the committed consensus state.
type PostgresStatus int8

const (
	PostgresUnknown PostgresStatus = 0 // not yet reported
	PostgresRunning PostgresStatus = 1 // postgres is up and operational
	PostgresStopped PostgresStatus = 2 // postgres is down (e.g. received SIGTERM)
)

func (s PostgresStatus) String() string {
	switch s {
	case PostgresRunning:
		return "running"
	case PostgresStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// PolicyID uniquely identifies a version of the cluster's DurabilityPolicyRecord.
// The zero value means no policy has been established yet.
type PolicyID string

// DurabilityPolicy determines when writes are considered durable.
// It is stored in a DurabilityPolicyRecord alongside the cohort membership.
type DurabilityPolicy interface {
	// IsWriteQuorum returns true if the set of acknowledging sync replicas is
	// sufficient to consider a write durable under this policy.
	IsWriteQuorum(ackingReplicas []NodeID) bool

	// IsAchievable returns true if this policy can be satisfied with the given
	// total number of cohort members (including the primary).
	IsAchievable(numCohortMembers int) bool
}

// DurabilityPolicyRecord is a versioned record of the cluster's durability configuration.
// It is written to the primary as a postgres transaction and propagated to replicas via
// WAL replication. All changes to cohort membership or the ack policy go through this
// mechanism rather than coordinator elections.
//
// Writes are compare-and-swap: the writer must supply the ID of the policy it believes is
// currently active (PreviousID). If PreviousID does not match the primary's current policy,
// the write is rejected and the writer must refresh its view before retrying.
type DurabilityPolicyRecord struct {
	// ID uniquely identifies this version of the policy.
	ID PolicyID

	// PreviousID is the ID of the policy this record supersedes. The zero value
	// means this is the initial policy (the cluster's first policy write).
	PreviousID PolicyID

	// CohortMembers is the ordered list of pooler IDs that may participate in
	// coordinator votes. Only poolers listed here are required to participate in
	// an emergency failover vote.
	CohortMembers []NodeID

	// Policy determines how many sync-replica ACKs are required for durability.
	Policy DurabilityPolicy
}

// PoolerPersistentState is the durable state a PoolerNode writes to storage.
// It survives process restarts and is loaded on startup via PoolerStorage.Load.
//
// Note: the WAL replay position (LSN) is not persisted here — it is always
// recoverable from postgres itself (e.g. SELECT pg_last_wal_replay_lsn()). It
// is reported in status indicators but is not part of the consensus persistent state.
type PoolerPersistentState struct {
	// Role is the pooler's current role (primary or replica).
	Role PoolerRole

	// Primary is the current primary as known to this pooler. For a primary node,
	// this is itself. For replicas, this is the node they stream WAL from.
	Primary NodeID

	// Policy is the most recent DurabilityPolicyRecord this pooler has committed
	// (primary) or applied from WAL replication (replica). Nil means no policy has
	// been seen yet.
	Policy *DurabilityPolicyRecord
}

// PolicyVersion returns the ID of the current policy record, or the zero value
// if no policy has been applied yet.
func (s PoolerPersistentState) PolicyVersion() PolicyID {
	if s.Policy == nil {
		return ""
	}
	return s.Policy.ID
}

// PoolerStorage is implemented by anything that durably persists PoolerPersistentState.
// In simulation it reads and writes an in-memory struct; in production it uses atomic
// write-rename+fsync to ensure committed state survives crashes.
//
// PoolerNode calls Save synchronously before emitting any response.
// Load is called by NewPoolerNode on startup and Restart() after crash recovery.
type PoolerStorage interface {
	Save(state PoolerPersistentState) error
	Load() (PoolerPersistentState, error)
}
