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

package consensus_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/consensus"
)

// members builds a []CohortMember slice from IDs for use in policy test cases.
func members(ids ...consensus.NodeID) []consensus.CohortMember {
	out := make([]consensus.CohortMember, len(ids))
	for i, id := range ids {
		out[i] = consensus.CohortMember{ID: id}
	}
	return out
}

func TestAtLeastPolicyIsDurable(t *testing.T) {
	const (
		p  consensus.NodeID = "primary"
		r1 consensus.NodeID = "r1"
		r2 consensus.NodeID = "r2"
	)

	cases := []struct {
		name   string
		policy consensus.DurabilityPolicy
		cohort []consensus.CohortMember
		acking []consensus.CohortMember
		want   bool
	}{
		// AtLeast(1): primary alone is sufficient.
		{
			name:   "AtLeast1/no_acks",
			policy: consensus.AtLeastPolicy(1),
			cohort: members(p),
			acking: nil,
			want:   false,
		},
		{
			name:   "AtLeast1/primary_only",
			policy: consensus.AtLeastPolicy(1),
			cohort: members(p),
			acking: members(p),
			want:   true,
		},
		{
			name:   "AtLeast1/primary_and_replica",
			policy: consensus.AtLeastPolicy(1),
			cohort: members(p, r1),
			acking: members(p, r1),
			want:   true,
		},
		{
			name:   "AtLeast1/replica_only",
			policy: consensus.AtLeastPolicy(1),
			cohort: members(p, r1),
			// This can happen during emergency failover
			acking: members(r1),
			want:   true,
		},

		// AtLeast(2): primary + 1 replica required.
		{
			name:   "AtLeast2/primary_only",
			policy: consensus.AtLeastPolicy(2),
			cohort: members(p, r1),
			acking: members(p),
			want:   false,
		},
		{
			name:   "AtLeast2/primary_and_one_replica",
			policy: consensus.AtLeastPolicy(2),
			cohort: members(p, r1, r2),
			acking: members(p, r1),
			want:   true,
		},
		{
			name:   "AtLeast2/two_replicas_no_primary",
			policy: consensus.AtLeastPolicy(2),
			cohort: members(p, r1, r2),
			acking: members(r1, r2),
			want:   true,
		},

		// AtLeast(3): primary + 2 replicas required.
		{
			name:   "AtLeast3/primary_and_one_replica",
			policy: consensus.AtLeastPolicy(3),
			cohort: members(p, r1, r2),
			acking: members(p, r1),
			want:   false,
		},
		{
			name:   "AtLeast3/all_three",
			policy: consensus.AtLeastPolicy(3),
			cohort: members(p, r1, r2),
			acking: members(p, r1, r2),
			want:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.policy.IsDurable(tc.cohort, tc.acking))
		})
	}
}

func TestAtLeastPolicyIsAchievable(t *testing.T) {
	const (
		p  consensus.NodeID = "primary"
		r1 consensus.NodeID = "r1"
		r2 consensus.NodeID = "r2"
		r3 consensus.NodeID = "r3"
	)

	cases := []struct {
		name   string
		policy consensus.DurabilityPolicy
		cohort []consensus.CohortMember
		want   bool
	}{
		{
			name:   "AtLeast1/empty_cohort",
			policy: consensus.AtLeastPolicy(1),
			cohort: nil,
			want:   false,
		},
		{
			name:   "AtLeast1/single_node",
			policy: consensus.AtLeastPolicy(1),
			cohort: members(p),
			want:   true,
		},
		{
			name:   "AtLeast3/two_nodes",
			policy: consensus.AtLeastPolicy(3),
			cohort: members(p, r1),
			want:   false,
		},
		{
			name:   "AtLeast3/exactly_three_nodes",
			policy: consensus.AtLeastPolicy(3),
			cohort: members(p, r1, r2),
			want:   true,
		},
		{
			name:   "AtLeast3/four_nodes",
			policy: consensus.AtLeastPolicy(3),
			cohort: members(p, r1, r2, r3),
			want:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.policy.IsAchievable(tc.cohort))
		})
	}
}

func TestAtLeastPolicyRevokesAndSamplesAllRevocationSets(t *testing.T) {
	const (
		p  consensus.NodeID = "primary"
		r1 consensus.NodeID = "r1"
		r2 consensus.NodeID = "r2"
		r3 consensus.NodeID = "r3"
		r4 consensus.NodeID = "r4"
	)
	primary := consensus.CohortMember{ID: p}

	cases := []struct {
		name      string
		policy    consensus.DurabilityPolicy
		cohort    []consensus.CohortMember
		recruited []consensus.CohortMember
		want      bool
	}{
		// ── AtLeast(1), 1 node: single-node bootstrap ────────────────────────────
		// sampleThreshold = min(1,0) = 0; only the primary exists.
		{
			name:      "AtLeast1/1node/primary_only",
			policy:    consensus.AtLeastPolicy(1),
			cohort:    members(p),
			recruited: members(p),
			want:      true, // 0 replicas >= 0; revoked: 1-1=0 < 1
		},
		{
			name:      "AtLeast1/1node/nobody_recruited",
			policy:    consensus.AtLeastPolicy(1),
			cohort:    members(p),
			recruited: nil,
			want:      false, // 1-0=1 not < 1
		},

		// ── AtLeast(3), 3 nodes: C==n, every member must ack ────────────────────
		// sampleThreshold = min(3,2) = 2; all replicas must be recruited.
		{
			name:      "AtLeast3/3nodes/one_replica",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2),
			recruited: members(r1),
			want:      false, // 1 replica < 2
		},
		{
			name:      "AtLeast3/3nodes/both_replicas",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2),
			recruited: members(r1, r2),
			want:      true, // 2 replicas >= 2; revoked: 3-2=1 < 3
		},
		{
			name:      "AtLeast3/3nodes/primary_and_one_replica",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2),
			recruited: members(p, r1),
			want:      false, // only 1 recruited replica < 2; recruiting the primary doesn't help coverage
		},
		{
			name:      "AtLeast3/3nodes/all_three",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2),
			recruited: members(p, r1, r2),
			want:      true, // 2 replicas >= 2; revoked: 3-3=0 < 3
		},

		// ── AtLeast(2), 3 nodes: C>n, minimal revocation sets have size 2 ───────
		// sampleThreshold = min(2,2) = 2.
		{
			name:      "AtLeast2/3nodes/one_replica",
			policy:    consensus.AtLeastPolicy(2),
			cohort:    members(p, r1, r2),
			recruited: members(r1),
			want:      false, // 3-1=2 not < 2 (not revoked)
		},
		{
			name:      "AtLeast2/3nodes/both_replicas",
			policy:    consensus.AtLeastPolicy(2),
			cohort:    members(p, r1, r2),
			recruited: members(r1, r2),
			want:      true, // 2 replicas >= 2; revoked: 3-2=1 < 2
		},

		// ── AtLeast(3), 5 nodes: the key production scenario ────────────────────
		// sampleThreshold = min(3,4) = 3 replicas.
		// A coordinator only needs to reach 3 of 4 replicas to complete recruitment.
		// Any two such recruited sets share at least 2 members — no divergence.
		{
			name:      "AtLeast3/5nodes/three_replicas",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2, r3, r4),
			recruited: members(r1, r2, r3),
			want:      true, // 3 replicas >= 3; revoked: 5-3=2 < 3
		},
		{
			name:      "AtLeast3/5nodes/two_replicas_insufficient",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2, r3, r4),
			recruited: members(r1, r2),
			want:      false, // 2 replicas < 3; also 5-2=3 not < 3
		},
		{
			name:      "AtLeast3/5nodes/primary_plus_two_replicas",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2, r3, r4),
			recruited: members(p, r1, r2),
			want:      false, // only 2 recruited replicas < 3; primary doesn't count for coverage
		},
		{
			name:      "AtLeast3/5nodes/primary_plus_three_replicas",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2, r3, r4),
			recruited: members(p, r1, r2, r3),
			want:      true, // 3 replicas >= 3; revoked: 5-4=1 < 3
		},
		{
			name:      "AtLeast3/5nodes/all_four_replicas",
			policy:    consensus.AtLeastPolicy(3),
			cohort:    members(p, r1, r2, r3, r4),
			recruited: members(r1, r2, r3, r4),
			want:      true, // 4 replicas >= 3; revoked: 5-4=1 < 3
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.policy.RevokesAndSamplesAllRevocationSets(tc.cohort, tc.recruited, primary)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAtLeastPolicyThreshold(t *testing.T) {
	type atLeastThresholder interface {
		AtLeastThreshold() int
	}
	for n := 1; n <= 5; n++ {
		p := consensus.AtLeastPolicy(n).(atLeastThresholder)
		assert.Equal(t, n, p.AtLeastThreshold())
	}
}
