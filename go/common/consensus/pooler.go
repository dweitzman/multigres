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

import clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

// ConsensusRole is a pooler's role relative to the highest rule it knows: the
// leader, a follower (a cohort member that is not the leader), or an observer
// (not a member of that rule's cohort — including the cold-start case where no
// rule is known yet).
type ConsensusRole int

const (
	ConsensusRoleObserver ConsensusRole = iota
	ConsensusRoleFollower
	ConsensusRoleLeader
)

func (r ConsensusRole) String() string {
	switch r {
	case ConsensusRoleLeader:
		return "leader"
	case ConsensusRoleFollower:
		return "follower"
	case ConsensusRoleObserver:
		return "observer"
	default:
		return "unknown"
	}
}

// SelfConsensusRole derives a pooler's role from its own consensus status, using
// the highest rule it knows (across its current position and the replication
// primary it follows). It returns:
//   - ConsensusRoleLeader when that rule names this pooler as leader,
//   - ConsensusRoleFollower when this pooler is in that rule's cohort but not the leader,
//   - ConsensusRoleObserver otherwise (not a cohort member, or no rule known yet).
func SelfConsensusRole(cs *clustermetadatapb.ConsensusStatus) ConsensusRole {
	rule := HighestKnownRule([]*clustermetadatapb.ConsensusStatus{cs})
	self := cs.GetId()
	if RuleNamesLeader(rule, self) {
		return ConsensusRoleLeader
	}
	for _, member := range rule.GetCohortMembers() {
		if idsEqual(member, self) {
			return ConsensusRoleFollower
		}
	}
	return ConsensusRoleObserver
}

// NamesSelfAsLeader reports whether cs names its own pooler as the leader of the
// highest rule it knows — across both its current position and the replication
// primary it follows (HighestKnownRule over this single status). It is the
// leader-only projection of SelfConsensusRole, kept as a convenience for the
// many callers that only care whether this pooler is the leader; use
// SelfConsensusRole when the follower/observer distinction matters.
//
// Returns false when cs, its ID, or any known rule is absent.
//
// TODO: deprecate in favor of SelfConsensusRole once all callers have migrated
// (notably the multiorch analysis package threads a NamesSelfAsLeader bool).
// Not marked Deprecated yet because staticcheck SA1019 fails the build at every
// call site, so the tag must land together with the call-site migration.
func NamesSelfAsLeader(cs *clustermetadatapb.ConsensusStatus) bool {
	return SelfConsensusRole(cs) == ConsensusRoleLeader
}

// IsNonRevokedCommittedLeader reports whether cs names its own pooler as the
// leader of its highest *committed* rule — its current position only, excluding
// the replication primary it may have been told to follow — and that committed
// rule has not been revoked by cs's term revocation.
//
// This is the write-safety leadership input: durable (never ahead of what's
// committed, so it stays false in the pg_promote()→commit window) and
// revocation-aware (a deposed leader at a now-revoked term is excluded). Contrast
// NamesSelfAsLeader, which is the highest-known (routing) check.
func IsNonRevokedCommittedLeader(cs *clustermetadatapb.ConsensusStatus) bool {
	committed := cs.GetCurrentPosition().GetRule()
	if !RuleNamesLeader(committed, cs.GetId()) {
		return false
	}
	return !IsRuleRevoked(committed, cs.GetTermRevocation())
}

// LeaderTerm returns the coordinator term of the pooler's current recorded
// rule if the pooler names itself as leader (per NamesSelfAsLeader). Returns 0
// when it does not, when the consensus status is nil/empty, or when the rule has
// no coordinator term.
func LeaderTerm(cs *clustermetadatapb.ConsensusStatus) int64 {
	if !NamesSelfAsLeader(cs) {
		return 0
	}
	return cs.GetCurrentPosition().GetRule().GetRuleNumber().GetCoordinatorTerm()
}
