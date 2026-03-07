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

// AckThreshold returns the minimum number of replica ACKs required.
func (p *anyNPolicy) AckThreshold() int {
	return p.n
}
