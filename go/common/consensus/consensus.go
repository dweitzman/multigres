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
//     cluster's DurabilityRules (cohort membership and ack policy); replicas learn
//     about rule changes via simulated WAL replication.
//
//   - CoordNode: coordinator (orch service). Stateless across restarts. Watches pooler
//     status, discovers observers (non-cohort poolers), and drives cohort expansion and
//     durability policy upgrades by writing DurabilityRules updates to the primary.
//     Uses emergency failover (coordinator elections) only when the primary is unreachable.
//
// Both implement dstsim.Node[Indicator, Request, NodeID] and can run in the same
// deterministic simulator.
//
// Normal path — cohort and policy changes via WAL:
//
//	CoordNode sees observer → sends WritePolicyRequest to primary → primary commits and
//	propagates WAL → replicas apply WAL and update their Rules → CoordNode observes
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

// NodeProperties holds static attributes of a pooler node that remain fixed for the
// lifetime of that node ID. Provided at startup (e.g. command-line flags) and used
// by AckPolicy implementations for zone-aware or topology-aware quorum decisions.
type NodeProperties struct {
	Zone string
}

// CohortMember pairs a pooler's identity with its static properties. Storing both
// together in DurabilityRules.Members means AckPolicy implementations always have
// all the information they need without a separate property lookup.
type CohortMember struct {
	ID         NodeID
	Properties NodeProperties
}

// AckPolicy determines when writes are considered durable.
// It is stored in DurabilityRules alongside the cohort membership.
type AckPolicy interface {
	// IsWriteQuorum returns true if the set of acknowledging sync replicas is
	// sufficient to consider a write durable under this policy.
	IsWriteQuorum(ackingReplicas []CohortMember) bool

	// IsAchievable returns true if this policy can be satisfied with the given
	// cohort. Implementations may use member properties (e.g. zone distribution)
	// as needed.
	IsAchievable(cohort []CohortMember) bool

	// IsRevoked returns true when every leader in leaders has had its
	// leadership revoked by the recruited set.
	//
	// A leader's leadership is considered revoked when either:
	//   - the leader is in recruited (it has committed to the coordinator
	//     and will not write unilaterally), or
	//   - no subset of non-recruited replicas (allMembers \ {leader} \
	//     recruited) can satisfy this policy (so no durable write is
	//     possible without the coordinator's knowledge).
	//
	// Pass a single known primary to check one specific leadership.
	// Pass all cohort members as leaders to check whether all possible
	// leaderships are revoked (used during emergency failover when the
	// true primary is unknown).
	IsRevoked(allMembers, recruited, leaders []CohortMember) bool
}

// DurabilityRules is a versioned record of the cluster's durability configuration.
// It is written to the primary as a postgres transaction and propagated to replicas
// via WAL replication. All changes to cohort membership or the ack policy go through
// this mechanism rather than coordinator elections.
//
// Writes are compare-and-swap: the primary accepts a write only if the incoming Seq
// is exactly its current Seq + 1. This prevents stale coordinators from overwriting
// a more recent configuration. Seq 0 is reserved for the "no rules yet" zero value;
// the first real rules record always has Seq 1.
//
// A higher Seq always means more recent rules, enabling any node to determine which
// of two rule sets is current without external context.
type DurabilityRules struct {
	// Seq is a monotonically increasing sequence number.
	Seq int64

	// Primary is the NodeID of the postgres primary for this shard at this
	// rule version. Every write to the WAL must flow through this node.
	Primary NodeID

	// Members is the full list of pooler IDs and their static properties that
	// may participate in coordinator votes. Only members listed here are required
	// to participate in an emergency failover vote.
	Members []CohortMember

	// Policy determines how many sync-replica ACKs are required for durability.
	Policy AckPolicy
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

	// Rules is the most recent DurabilityRules this pooler has committed (primary)
	// or applied from WAL replication (replica). Nil means no rules have been
	// seen yet.
	Rules *DurabilityRules
}

// PolicySeq returns the sequence number of the current rules, or 0 if no rules
// have been applied yet.
func (s PoolerPersistentState) PolicySeq() int64 {
	if s.Rules == nil {
		return 0
	}
	return s.Rules.Seq
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
