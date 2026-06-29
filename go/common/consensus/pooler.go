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

// NamesSelfAsLeader reports whether cs names its own pooler as the leader of the
// highest rule it knows — across both its current position and the replication
// primary it follows (HighestKnownRule over this single status).
//
// Returns false when cs, its ID, or any known rule is absent.
func NamesSelfAsLeader(cs *clustermetadatapb.ConsensusStatus) bool {
	self := cs.GetId()
	if self == nil {
		return false
	}
	leader := HighestKnownRule([]*clustermetadatapb.ConsensusStatus{cs}).GetLeaderId()
	if leader == nil {
		return false
	}
	return idsEqual(self, leader)
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
