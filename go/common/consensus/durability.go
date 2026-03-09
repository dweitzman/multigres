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

// AtLeastPolicy returns a DurabilityPolicy that requires at least n cohort
// members — including the primary — to have acknowledged a write before it is
// considered durable.
//
// The relationship to PostgreSQL's synchronous_standby_names syntax:
//
//	AtLeast(1) ↔ ANY 0  (primary alone is sufficient; no replica ACK required)
//	AtLeast(2) ↔ ANY 1  (primary + 1 replica)
//	AtLeast(3) ↔ ANY 2  (primary + 2 replicas)
//
// AtLeast(1) is appropriate for a bootstrapping single-node cluster or when HA
// guarantees are temporarily relaxed. Production HA clusters should target at
// least AtLeast(3) (primary + 2 replicas).
//
// TODO: add a dedicated test suite that exhaustively demonstrates AtLeastPolicy
// behaviour across a range of cohort sizes and transitions: IsDurable,
// IsAchievable, and RevokesAndSamplesAllRevocationSets at each cohort size,
// plus examples showing how the three methods together ensure durability,
// progress, leadership revocation, and coordinator overlap.
func AtLeastPolicy(n int) DurabilityPolicy {
	return &atLeastPolicy{n: n}
}

type atLeastPolicy struct {
	n int
}

// IsDurable returns true if at least n cohort members have confirmed the write.
// The primary should always appear in ackingMembers (it commits locally first);
// this allows AtLeast(1) to be satisfied by the primary alone with no replicas.
func (p *atLeastPolicy) IsDurable(_ []CohortMember, ackingMembers []CohortMember) bool {
	return len(ackingMembers) >= p.n
}

// IsAchievable returns true if the proposed cohort is large enough to ever
// satisfy this policy. AtLeast(n) requires at least n members in the cohort
// because a write needs n acknowledgements (including the primary) and a node
// cannot ack a write it never receives.
func (p *atLeastPolicy) IsAchievable(proposedCohort []CohortMember) bool {
	return len(proposedCohort) >= p.n
}

// RevokesAndSamplesAllRevocationSets returns true when the recruited set both:
//
//  1. Revokes the primary: the non-recruited cohort members alone cannot satisfy
//     IsDurable, i.e. len(cohort)-len(recruited) < n.
//
//  2. Samples every minimal revocation set — excluding the primary-only set (if
//     it exists). Emergency failover is initiated precisely because the primary
//     is unreachable; by convention only replicas contribute to the coverage
//     criterion. The primary is identified by the primary parameter and excluded
//     when counting recruited members for the coverage check.
//
//     The coverage threshold for replicas is min(n, C-1), where C is the cohort
//     size (including the primary):
//     - When C == n (all members must ack), minimal revocation sets are
//     singletons. {P} is excluded by convention; all n-1 replicas must be
//     recruited (threshold = n-1 = C-1 = min(n, C-1)).
//     - When C > n, minimal revocation sets have size > 1 and none consist
//     only of the primary. n replica members suffice to intersect every such
//     set (threshold = n = min(n, C-1)).
//     - When C == 1 and n == 1 (single-node bootstrap), no replicas exist so
//     the threshold is 0 = min(1, 0). Revocation is achieved by recruiting
//     the primary alone: C-r < n → 0 < 1 (r=1). This allows AtLeast(1)
//     revocation even with no replicas.
//
// The combined condition is:
//
//	len(recruitedReplicas) >= min(n, C-1)  AND  len(cohort)-len(recruited) < n
//
// where recruitedReplicas = recruited members excluding the primary.
//
// Example: AtLeast(3) with cohort {P, R1, R2}, primary=P (need all 3 to ack):
//
//	Minimal revocation sets: {P}, {R1}, {R2}. {P} is excluded by convention.
//	RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1], P) = false
//	  (only 1 replica; threshold = min(3,2) = 2).
//	RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1,R2], P) = true
//	  (2 replicas >= 2; revoked: 3-2=1 < 3).
//
// Example: AtLeast(2) with cohort {P, R1, R2}, primary=P (any 2 must ack):
//
//	Minimal revocation sets: {P,R1}, {P,R2}, {R1,R2}. None are primary-only.
//	RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1], P) = false
//	  (P+R2=2 remain — not revoked).
//	RevokesAndSamplesAllRevocationSets([P,R1,R2], [R1,R2], P) = true
//	  (2 replicas >= min(2,2)=2; revoked: 3-2=1 < 2).
//
// Example: AtLeast(1) with cohort {P}, primary=P (single-node bootstrap):
//
//	RevokesAndSamplesAllRevocationSets([P], [P], P) = true
//	  (0 replicas >= min(1,0)=0; revoked: 1-1=0 < 1).
func (p *atLeastPolicy) RevokesAndSamplesAllRevocationSets(cohortMembers, recruitedMembers []CohortMember, primary CohortMember) bool {
	C := len(cohortMembers)
	r := len(recruitedMembers)

	// Count recruited members excluding the primary (replicas only).
	recruitedReplicas := 0
	for _, m := range recruitedMembers {
		if m.ID != primary.ID {
			recruitedReplicas++
		}
	}

	// Coverage threshold: min(n, C-1).
	// When C == n, need all n-1 replicas. When C > n, n replicas suffice.
	// When C == 1 (single-node), threshold is 0 and the primary alone revokes.
	sampleThreshold := min(p.n, C-1)
	return recruitedReplicas >= sampleThreshold && C-r < p.n
}

// AtLeastThreshold returns the minimum number of cohort member ACKs (including
// primary) required for durability. Used for serialisation and for computing
// the corresponding PostgreSQL synchronous_standby_names value: the postgres
// ANY N value equals AtLeastThreshold()-1 (since postgres counts replica ACKs,
// not total node ACKs).
func (p *atLeastPolicy) AtLeastThreshold() int {
	return p.n
}
