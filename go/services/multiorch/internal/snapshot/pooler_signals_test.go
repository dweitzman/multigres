// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestPoolerHealthState_CohortIneligible(t *testing.T) {
	tests := []struct {
		name string
		av   *clustermetadatapb.AvailabilityStatus
		want bool
	}{
		{
			name: "nil availability status (older pooler) treated as eligible",
			av:   nil,
			want: false,
		},
		{
			name: "availability status with no cohort eligibility field treated as eligible",
			av:   &clustermetadatapb.AvailabilityStatus{},
			want: false,
		},
		{
			name: "UNKNOWN signal (default) treated as eligible",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_UNKNOWN,
				},
			},
			want: false,
		},
		{
			name: "ELIGIBLE returns false",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			want: false,
		},
		{
			name: "INELIGIBLE returns true",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, (&PoolerHealthState{AvailabilityStatus: tc.av}).CohortIneligible())
		})
	}
}

// poolerWithLeaderTerm builds a PoolerHealthState whose ConsensusStatus has
// the given primary term. Used to construct fixtures that exercise the
// staleness check on REQUESTING_DEMOTION.
func poolerWithLeaderTerm(t *testing.T, primaryTerm int64) *PoolerHealthState {
	t.Helper()
	id := &clustermetadatapb.ID{Name: "mp1"}
	return &PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: id},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					LeaderId: id,
					RuleNumber: &clustermetadatapb.RuleNumber{
						CoordinatorTerm: primaryTerm,
					},
				}},
			},
		},
	}
}

// poolerWithLeaderTermViaProposal builds a PoolerHealthState whose decision
// is at a different (stale) term but whose outstanding proposal is at
// primaryTerm — e.g. a self-promotion whose write reached WAL but wasn't
// marked decided yet.
func poolerWithLeaderTermViaProposal(t *testing.T, primaryTerm int64) *PoolerHealthState {
	t.Helper()
	id := &clustermetadatapb.ID{Name: "mp1"}
	return &PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: id},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{LeaderId: id, RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: primaryTerm - 1}},
					Proposal: &clustermetadatapb.ShardRule{LeaderId: id, RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: primaryTerm}},
				},
			},
		},
	}
}

func TestPoolerHealthState_RequestsLeaderReplacement(t *testing.T) {
	t.Run("nil PoolerHealthState returns false", func(t *testing.T) {
		assert.False(t, (*PoolerHealthState)(nil).RequestsLeaderReplacement())
	})

	t.Run("no AvailabilityStatus returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		assert.False(t, p.RequestsLeaderReplacement())
	})

	t.Run("AvailabilityStatus with no signals returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{}
		assert.False(t, p.RequestsLeaderReplacement())
	})

	t.Run("REQUESTING_DEMOTION with matching term returns true", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.True(t, p.RequestsLeaderReplacement())
	})

	t.Run("REQUESTING_DEMOTION with term matching an undecided proposal returns true", func(t *testing.T) {
		// The signal's term matches primaryTerm via the pooler's outstanding
		// proposal (decision is one term behind) — a self-promotion that
		// crashed before its own write was confirmed decided must still be
		// recognized.
		p := poolerWithLeaderTermViaProposal(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.True(t, p.RequestsLeaderReplacement())
	})

	t.Run("REQUESTING_DEMOTION with stale term returns false", func(t *testing.T) {
		// Signal carries term 3 but node's current primary term is 5 — stale.
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 3,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.False(t, p.RequestsLeaderReplacement())
	})

	t.Run("LeadershipSignal_ACTIVE returns false even at matching term", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE,
			},
		}
		assert.False(t, p.RequestsLeaderReplacement())
	})

	t.Run("CohortEligibility INELIGIBLE returns true even without leadership signal", func(t *testing.T) {
		// Graceful-shutdown path: the pooler advertises INELIGIBLE without
		// touching LeadershipStatus, and the analyzer must still trigger
		// replacement.
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		assert.True(t, p.RequestsLeaderReplacement())
	})

	t.Run("CohortEligibility INELIGIBLE returns true regardless of term", func(t *testing.T) {
		// Cohort eligibility is not term-gated, unlike REQUESTING_DEMOTION.
		p := poolerWithLeaderTerm(t, 0)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		assert.True(t, p.RequestsLeaderReplacement())
	})

	t.Run("CohortEligibility ELIGIBLE returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
			},
		}
		assert.False(t, p.RequestsLeaderReplacement())
	})
}

func TestPoolerHealthState_ClaimsLeadership(t *testing.T) {
	rule := func(leader string, cohort ...string) *clustermetadatapb.RulePosition {
		var members []*clustermetadatapb.ID
		for _, c := range cohort {
			members = append(members, podID(c))
		}
		return &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: ruleNumber(2), LeaderId: podID(leader), CohortMembers: members,
		}}
	}

	assert.True(t, withPosition(pooler("a"), rule("a", "a", "b")).ClaimsLeadership(), "the rule names it leader")
	assert.False(t, withPosition(pooler("b"), rule("a", "a", "b")).ClaimsLeadership(), "a cohort member that is not the leader")
	assert.False(t, withPosition(pooler("c"), rule("a", "a", "b")).ClaimsLeadership(), "not in the cohort")
	assert.False(t, pooler("a").ClaimsLeadership(), "no rule known")
	assert.False(t, (*PoolerHealthState)(nil).ClaimsLeadership())

	proposal := &clustermetadatapb.RulePosition{
		Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(1), LeaderId: podID("old")},
		Proposal: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(2), LeaderId: podID("a")},
	}
	assert.True(t, withPosition(pooler("a"), proposal).ClaimsLeadership(), "a claim can rest on an undecided proposal")
}
