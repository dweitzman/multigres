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
//     cluster's Term (cohort membership and ack policy); replicas learn
//     about term changes via simulated WAL replication.
//
//   - CoordNode: coordinator (orch service). Stateless across restarts. Watches pooler
//     status, discovers observers (non-cohort poolers), and drives cohort expansion and
//     durability policy upgrades by writing Term updates to the primary.
//     Uses coordinator-led term changes when the primary is unreachable.
//
// Both implement dstsim.Node[Indicator, Request, NodeID] and can run in the same
// deterministic simulator.
//
// Normal path — cohort and policy changes via WAL:
//
//	CoordNode sees observer → sends WritePolicyRequest to primary → primary commits and
//	propagates WAL → replicas apply WAL and update their Term → CoordNode observes
//	updated status.
//
// Coordinator-led path — term change without a running primary:
//
//	CoordNode detects unreachable primary → recruits cohort members → writes new term
//	to shadow WAL → nodes apply term and resume replication.
package consensus

import "fmt"

// NodeID identifies any node in the simulation: both coordinator nodes and pooler nodes.
// Using a single ID type lets all node kinds live in one dstsim.Simulator instance.
type NodeID string

// LSN is a Log Sequence Number: a monotonically increasing position in the
// PostgreSQL WAL. In production this maps to the uint64 value underlying
// postgres's pg_lsn type (e.g. the result of pg_lsn_to_uint64(pg_current_wal_lsn())).
// In simulation it is a simple counter. The zero value means "no LSN known".
type LSN int64

// NodePosition describes a node's current position in the WAL timeline,
// combining its real WAL progress with the most recent durability term it has
// accepted (which may come from either real WAL or shadow WAL). The Term and
// LSN together give a coordinator everything it needs to rank candidates during
// coordinator-led term change: pick the node with the highest real WAL position, breaking
// ties by term Seq (a higher Seq means the node has already committed to a later
// rule change).
//
// NOTE: each coordinator proposes exactly one shadow WAL term per ProposedSeq,
// but racing coordinators can leave shadow WAL entries at non-consecutive Seqs.
// After uses only the highest accepted Term.Seq as a tiebreaker, which is
// sufficient today. If the protocol later allows multiple shadow WAL entries at
// the same Seq (e.g. incremental membership changes), After must also account
// for the count of shadow WAL entries to rank candidates correctly.
type NodePosition struct {
	// Term is the most recently accepted term — applied from real WAL or
	// durably written to shadow WAL by a coordinator. Nil if no term known.
	Term *Term
	// LSN is the node's real WAL position at the time this snapshot was taken.
	// The coordinator uses the maximum LSN across all recruited nodes as
	// BaseLSN for shadow WAL writes, ensuring a safe promotion point.
	LSN LSN
}

// After reports whether p is strictly more advanced than other.
// A node at a higher real WAL position is always preferred; at equal WAL
// positions, the node that has accepted a higher term Seq wins (having
// committed to a further rule change already).
func (p NodePosition) After(other NodePosition) bool {
	if p.LSN != other.LSN {
		return p.LSN > other.LSN
	}
	var ps, os int64
	if p.Term != nil {
		ps = p.Term.Seq
	}
	if other.Term != nil {
		os = other.Term.Seq
	}
	return ps > os
}

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
// by DurabilityPolicy implementations for zone-aware or topology-aware quorum decisions.
type NodeProperties struct {
	Zone string
}

// CohortMember pairs a pooler's identity with its static properties. Storing both
// together in Term.Members means DurabilityPolicy implementations always
// have all the information they need without a separate property lookup.
type CohortMember struct {
	ID         NodeID
	Properties NodeProperties
}

// DurabilityPolicy determines when transactions are considered durable, when a policy
// change is achievable, and when a coordinator's recruitment of nodes is sufficient
// to both revoke leadership and guarantee overlap with any competing coordinator.
//
// The three methods each serve a distinct purpose in the consensus protocol:
//
//   - IsDurable: used by the primary (or coordinator) to evaluate whether a transaction
//     has received enough acknowledgements from the cohort.
//   - IsAchievable: used by the coordinator before issuing a rule change to verify
//     the proposed cohort can satisfy the policy, preventing an impossible rule change
//     from hanging forever.
//   - RevokesAndSamplesAllRevocationSets: used during coordinator-led rule changes to determine
//     whether enough nodes have been recruited to both revoke the current primary's write
//     ability and guarantee that any two coordinators satisfying this property share at
//     least one recruited node (enabling sequencing of competing coordinator elections).
type DurabilityPolicy interface {
	// IsDurable returns true if the set of acknowledging cohort members is
	// sufficient to consider a write durable under this policy.
	//
	// ackingMembers should include every cohort member that has confirmed the
	// write: both the primary (which always commits locally before acking) and
	// any replicas that have streamed and applied the WAL entry. Including the
	// primary enables policies that count the primary as part of the quorum
	// (e.g. AtLeast(1): any single node having the write is sufficient).
	//
	// Examples for a 3-node cohort {primary, R1, R2}:
	//   AtLeast(1): IsDurable([P,R1,R2], [P]) = true  (primary alone is enough)
	//   AtLeast(2): IsDurable([P,R1,R2], [P]) = false (need primary + one replica)
	//   AtLeast(2): IsDurable([P,R1,R2], [R1,R2]) = true (stale primary + two replicas is durable -- this could happen after a primary crash or during coordinator-led rule propagation if the primary is unreachable)
	//   AtLeast(2): IsDurable([P,R1,R2], [P,R1]) = true
	//   AtLeast(3): IsDurable([P,R1,R2], [P,R1]) = false (all three required)
	IsDurable(cohortMembers, ackingMembers []CohortMember) bool

	// IsAchievable returns true if this policy can ever be satisfied with the
	// proposed cohort. Implementations may use member properties (e.g. zone
	// distribution) as needed.
	//
	// The coordinator calls IsAchievable before writing a new Term to avoid
	// committing a configuration the cluster can never satisfy. For example,
	// AtLeast(3) with only 2 proposed members would never reach quorum —
	// writing such a term would leave the cluster permanently stuck waiting for an
	// ack that can never arrive.
	//
	// Example:
	//   AtLeast(3).IsAchievable([P, R1]) = false  (only 2 members; need 3)
	//   AtLeast(3).IsAchievable([P, R1, R2]) = true
	IsAchievable(proposedCohort []CohortMember) bool

	// RevokesAndSamplesAllRevocationSets returns true when the recruited set
	// simultaneously satisfies two properties required for safe coordinator-led term change:
	//
	//  1. Revocation: the primary can no longer form a durable write, because even
	//     if every non-recruited cohort member acks, IsDurable is not satisfied.
	//     Formally: !IsDurable(cohort, cohort \ recruited).
	//
	//  2. Coverage: the recruited set samples at least one node from every minimal
	//     revocation set — the smallest subsets of the cohort that, when recruited,
	//     achieve revocation — excluding the primary-only minimal revocation set
	//     (see convention below). This guarantees that any two coordinators that
	//     both satisfy RevokesAndSamplesAllRevocationSets must have recruited at
	//     least one replica in common, enabling the cluster to sequence competing
	//     coordinators (the one whose recruitment the shared replica accepted first
	//     wins).
	//
	// Why both properties are needed:
	//
	// Revocation alone is insufficient: two coordinators could each recruit a
	// disjoint set of nodes that individually revokes the primary, but the
	// coordinators would have no shared node to establish ordering. Example for
	// AtLeast(3) with cohort {P, R1, R2}: recruiting either {R1} or {R2} revokes P
	// (leaving only 2 acking nodes, fewer than 3). Two coordinators could revoke P
	// independently without ever visiting the same node, making it impossible to
	// determine which coordinator's proposed rules should win.
	//
	// Coverage alone is insufficient: it does not guarantee the primary is actually
	// unable to write.
	//
	// Convention — primary excluded from coverage:
	//
	// Emergency failover is triggered precisely because the primary is unreachable.
	// Technically, recruiting only the primary would revoke its write ability
	// (it stops accepting writes), but if the primary alone constitutes a valid
	// coverage set, two coordinators would be required to contact the primary
	// to touch overlapping node during revocation — yet neither could reach it in
	// the first place.
	// By convention, only replicas (non-primary cohort members) are counted when
	// evaluating the coverage criterion. This ensures two valid recruited sets must
	// share at least one reachable replica, providing a meaningful ordering anchor.
	//
	// The primary parameter identifies which cohort member is the current primary
	// so that it can be excluded from the coverage check.
	//
	// Example for AtLeast(3), cohort {P, R1, R2} (3 nodes, need all 3 to ack):
	//   Minimal revocation sets: {P}, {R1}, {R2} (each singleton revokes).
	//   {P} is excluded from coverage (primary-only set, per convention above).
	//   RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1], P) = false
	//     (R1 alone revokes P, but does not cover {R2}).
	//   RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1,R2], P) = true
	//     (both replicas recruited — revoked and all non-primary sets covered).
	//
	// Example for AtLeast(2), cohort {P, R1, R2} (3 nodes, need any 2 to ack):
	//   Minimal revocation sets: {P,R1}, {P,R2}, {R1,R2} (size-2 subsets).
	//   None are primary-only, so all three must be covered.
	//   RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1], P) = false
	//     (P+R2 can still ack — not revoked).
	//   RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1,R2], P) = true
	//     (both replicas recruited — revoked and all minimal sets covered via replicas).
	RevokesAndSamplesAllRevocationSets(cohortMembers, recruitedMembers []CohortMember, primary CohortMember) bool
}

// Term is a versioned record of the cluster's durability configuration.
// It is written to the primary as a postgres transaction and propagated to replicas
// via WAL replication. All changes to cohort membership or the durability policy go
// through this mechanism rather than coordinator elections.
//
// Writes are compare-and-swap: the primary accepts a write only if the incoming Seq
// is exactly its current Seq + 1. This prevents stale coordinators from overwriting
// a more recent configuration. Seq 0 is reserved for the "no term yet" zero value;
// the first real term record always has Seq 1.
//
// A higher Seq always means a more recent term, enabling any node to determine which
// of two terms is current without external context.
type Term struct {
	// Seq is a monotonically increasing sequence number.
	Seq int64

	// Primary is the NodeID of the postgres primary for this shard at this
	// rule version. Every write to the WAL must flow through this node.
	Primary NodeID

	// Members is the full list of pooler IDs and their static properties that
	// may participate in coordinator votes. Only members listed here are required
	// to participate in an coordinator-led term change vote.
	Members []CohortMember

	// Policy determines the durability requirements for writes and failover.
	Policy DurabilityPolicy
}

// String returns a compact representation suitable for trace output.
func (t Term) String() string {
	return fmt.Sprintf("seq=%d/prim=%v", t.Seq, t.Primary)
}

// RecruitmentCommitment records that this pooler has granted a coordinator
// exclusive authority to append term changes to the shadow WAL in the range
// (AtTermSeq, ProposedSeq]. It is purely an authorization token — it does not
// carry the content of any term change.
//
// While a commitment is held, the pooler withdraws from write quorum for the
// active term so that the coordinator can safely reorganise the cluster. Only
// one coordinator may hold authority at a time; a new commitment with a higher
// ProposedSeq supersedes any previous one.
//
// The commitment survives crashes: a restarted pooler loads it from storage
// and honours it before accepting any new instructions.
type RecruitmentCommitment struct {
	// CoordID is the coordinator that holds authority.
	CoordID NodeID

	// AtTermSeq is the base term seq the coordinator is building from (the last
	// term this pooler had applied when the commitment was granted).
	AtTermSeq int64

	// ProposedSeq is the highest term seq the coordinator is authorised to write
	// to the shadow WAL.
	ProposedSeq int64
}

// String returns a compact representation suitable for trace output.
func (r *RecruitmentCommitment) String() string {
	if r == nil {
		return "nil"
	}
	return fmt.Sprintf("%v(at=%d→%d)", r.CoordID, r.AtTermSeq, r.ProposedSeq)
}

// IsRevokedBy reports whether proposed should replace r as the active
// commitment. The ordering is lexicographic on (AtTermSeq, ProposedSeq):
//   - A higher AtTermSeq always wins: the proposing coordinator has more
//     up-to-date base knowledge, so its authority takes precedence.
//   - When AtTermSeq values are equal, a higher ProposedSeq wins.
//   - A lower AtTermSeq never revokes regardless of ProposedSeq: a coordinator
//     with stale base knowledge cannot displace one that knows more.
//   - Equal (AtTermSeq, ProposedSeq) does NOT revoke — first-write-wins
//     regardless of coordinator identity.
//
// The idempotent case (same coordinator, same parameters) is handled by Go
// struct equality (r == proposed).
func (r RecruitmentCommitment) IsRevokedBy(proposed RecruitmentCommitment) bool {
	if proposed.AtTermSeq != r.AtTermSeq {
		return proposed.AtTermSeq > r.AtTermSeq
	}
	return proposed.ProposedSeq > r.ProposedSeq
}

// AllowsTermChange reports whether this commitment authorises the holder to
// write a term at termSeq. The write is authorised when it advances the term
// (termSeq > AtTermSeq) and falls within the authorised range (termSeq <= ProposedSeq).
func (r RecruitmentCommitment) AllowsTermChange(termSeq int64) bool {
	return termSeq > r.AtTermSeq && termSeq <= r.ProposedSeq
}

// PoolerPersistentState is the durable state a PoolerNode writes to storage.
// It survives process restarts and is loaded on startup via PoolerStorage.Load.
//
// Note: the WAL replay position (LSN) is not persisted here — it is always
// recoverable from postgres itself (e.g. SELECT pg_last_wal_replay_lsn()). It
// is reported in status indicators but is not part of the consensus persistent state.
type PoolerPersistentState struct {
	// TODO: Role and Primary are derivable from the node's own ID and CachedTerm
	// (Role = primary iff nodeID == CachedTerm.Primary; Primary = CachedTerm.Primary).
	// They are kept here as a bootstrap convenience for nodes that have not yet
	// received their first term. Remove them once startup always provides an
	// initial term.

	// Role is the pooler's current role (primary or replica).
	Role PoolerRole

	// Primary is the current primary as known to this pooler. For a primary node,
	// this is itself. For replicas, this is the node they stream WAL from.
	Primary NodeID

	// CachedTerm is a cached copy of the most recently applied Term — the term
	// this pooler last committed to postgres WAL (primary) or received via WAL
	// replication (replica). Nil means no term has been seen yet.
	//
	// This is a cache: in principle it can be re-read from postgres on startup
	// (e.g. by querying the consensus state table). It is persisted here to
	// avoid a postgres round-trip on every coordinator status check.
	// TODO: evaluate whether this needs to be persisted separately, or whether
	// it can always be refreshed from postgres when postgres is available.
	CachedTerm *Term

	// Commitment grants one coordinator exclusive authority to append term
	// changes to the shadow WAL in the range (AtTermSeq, ProposedSeq]. A new
	// commitment with a higher ProposedSeq supersedes any previous one.
	// Nil means no coordinator currently holds authority.
	Commitment *RecruitmentCommitment

	// ShadowWAL is the append-only log of Term changes written by the
	// authorised coordinator while postgres is stopped. Like real WAL, entries
	// are ordered by Seq and must satisfy the same invariants as a normal term
	// write (sequential seqs, achievable policy, valid cohort). Unlike
	// CachedTerm, this cannot be recovered from postgres — it is the source of
	// truth for term transitions that occurred while postgres was stopped.
	//
	// Entries are pruned once real postgres WAL confirms the corresponding
	// transitions.
	// TODO: add shadow WAL truncation for coordinator-directed pg_rewind
	// (non-durable divergent timeline recovery).
	ShadowWAL []*Term
}

// PolicySeq returns the sequence number of the current rules, or 0 if no rules
// have been applied yet.
func (s PoolerPersistentState) PolicySeq() int64 {
	if s.CachedTerm == nil {
		return 0
	}
	return s.CachedTerm.Seq
}

// CommitmentEndSeq returns the ProposedSeq of the current commitment (the
// highest term seq the authorised coordinator may write), or 0 if no
// commitment exists.
func (s PoolerPersistentState) CommitmentEndSeq() int64 {
	if s.Commitment == nil {
		return 0
	}
	return s.Commitment.ProposedSeq
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
