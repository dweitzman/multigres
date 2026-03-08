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

// AnyNPolicy returns an AckPolicy that considers a write durable when at least
// n sync replicas have acknowledged it. AnyNPolicy(0) means no replicas are
// required — suitable for a 1-node bootstrap cluster with no HA guarantees.
func AnyNPolicy(n int) AckPolicy {
	return &anyNPolicy{n: n}
}

type anyNPolicy struct {
	n int
}

// IsWriteQuorum returns true if the number of acknowledging replicas meets the threshold.
func (p *anyNPolicy) IsWriteQuorum(ackingReplicas []CohortMember) bool {
	return len(ackingReplicas) >= p.n
}

// IsAchievable returns true if this policy can be satisfied with the given cohort.
// AnyNPolicy(n) requires at least n+1 members (one primary plus n sync replicas).
func (p *anyNPolicy) IsAchievable(cohort []CohortMember) bool {
	return len(cohort) >= p.n+1
}

// IsRevoked returns true when every leader in leaders has had its leadership
// revoked by the recruited set. See AckPolicy.IsRevoked for the full contract.
//
// For AnyN(n) with a cohort of size C, a leader is revoked when either the
// leader itself is recruited, or at least C-n of its replicas are recruited
// (leaving fewer than n non-recruited replicas, so no quorum is achievable).
func (p *anyNPolicy) IsRevoked(allMembers, recruited, leaders []CohortMember) bool {
	recruitedIDs := make(map[NodeID]bool, len(recruited))
	for _, m := range recruited {
		recruitedIDs[m.ID] = true
	}
	nonRecruitedCount := len(allMembers) - len(recruited)
	for _, leader := range leaders {
		if recruitedIDs[leader.ID] {
			continue // leader is recruited, it can block all writes
		}
		if nonRecruitedCount >= p.n {
			return false // enough non-recruited replicas remain to form a quorum
		}
	}
	return true
}

// AckThreshold returns the minimum number of replica ACKs required.
func (p *anyNPolicy) AckThreshold() int {
	return p.n
}
