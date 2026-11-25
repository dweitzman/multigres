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

package ha

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestCohortIneligible(t *testing.T) {
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
			assert.Equal(t, tc.want, CohortIneligible(tc.av))
		})
	}
}

// poolerWithLeaderTerm builds a PoolerReport whose ConsensusStatus names the
// pooler leader at the given term. Used to construct fixtures that exercise the
// staleness check on REQUESTING_DEMOTION from older poolers.
func poolerWithLeaderTerm(t *testing.T, primaryTerm int64) *PoolerReport {
	t.Helper()
	id := &clustermetadatapb.ID{Name: "mp1"}
	return &PoolerReport{
		Consensus: &clustermetadatapb.ConsensusStatus{
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

// poolerWithLeaderTermViaProposal builds a PoolerReport whose decision
// is at a different (stale) term but whose outstanding proposal is at
// primaryTerm — e.g. a self-promotion whose write reached WAL but wasn't
// marked decided yet.
func poolerWithLeaderTermViaProposal(t *testing.T, primaryTerm int64) *PoolerReport {
	t.Helper()
	id := &clustermetadatapb.ID{Name: "mp1"}
	return &PoolerReport{
		Consensus: &clustermetadatapb.ConsensusStatus{
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

func TestLeaderNeedsReplacement(t *testing.T) {
	t.Run("empty report returns false", func(t *testing.T) {
		assert.False(t, LeaderNeedsReplacement(PoolerReport{}))
	})

	t.Run("no AvailabilityStatus returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		assert.False(t, LeaderNeedsReplacement(*p))
	})

	t.Run("AvailabilityStatus with no signals returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{}
		assert.False(t, LeaderNeedsReplacement(*p))
	})

	t.Run("legacy REQUESTING_DEMOTION with matching term returns true", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.True(t, LeaderNeedsReplacement(*p))
	})

	t.Run("legacy REQUESTING_DEMOTION with term matching an undecided proposal returns true", func(t *testing.T) {
		// The signal's term matches primaryTerm via the pooler's outstanding
		// proposal (decision is one term behind) — a self-promotion that
		// crashed before its own write was confirmed decided must still be
		// recognized.
		p := poolerWithLeaderTermViaProposal(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.True(t, LeaderNeedsReplacement(*p))
	})

	t.Run("legacy REQUESTING_DEMOTION with stale term returns false", func(t *testing.T) {
		// Signal carries term 3 but node's current primary term is 5 — stale.
		p := poolerWithLeaderTerm(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 3,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.False(t, LeaderNeedsReplacement(*p))
	})

	t.Run("an unset leadership signal returns false even at matching term", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{LeaderTerm: 5},
		}
		assert.False(t, LeaderNeedsReplacement(*p))
	})

	t.Run("CohortEligibility INELIGIBLE returns true even without leadership signal", func(t *testing.T) {
		// Graceful-shutdown path: the pooler advertises INELIGIBLE without
		// touching LeadershipStatus, and the analyzer must still trigger
		// replacement.
		p := poolerWithLeaderTerm(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		assert.True(t, LeaderNeedsReplacement(*p))
	})

	t.Run("CohortEligibility INELIGIBLE returns true regardless of term", func(t *testing.T) {
		// Cohort eligibility is not term-gated, unlike REQUESTING_DEMOTION.
		p := poolerWithLeaderTerm(t, 0)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		assert.True(t, LeaderNeedsReplacement(*p))
	})

	t.Run("CohortEligibility ELIGIBLE returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
			},
		}
		assert.False(t, LeaderNeedsReplacement(*p))
	})

	t.Run("a static leader has no one to replace it", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			Facts: &clustermetadatapb.AvailabilityFacts{PostgresStatus: clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY},
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		assert.True(t, LeaderPostgresDemoted(*p), "the fact holds regardless of mode")
		assert.False(t, LeaderNeedsReplacement(*p))
	})
}

func TestLeaderPostgresDemoted(t *testing.T) {
	withPostgres := func(p *PoolerReport, st clustermetadatapb.PostgresStatus) *PoolerReport {
		p.Availability = &clustermetadatapb.AvailabilityStatus{
			Facts: &clustermetadatapb.AvailabilityFacts{PostgresStatus: st},
		}
		return p
	}
	tests := []struct {
		name string
		p    *PoolerReport
		want bool
	}{
		{name: "named leader, postgres a standby", p: withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY), want: true},
		{name: "named leader by its undecided proposal, postgres a standby", p: withPostgres(poolerWithLeaderTermViaProposal(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY), want: true},
		{name: "named leader, postgres a primary", p: withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY)},
		{name: "named leader, postgres promoting", p: withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING)},
		// Starting or unknown is not evidence of recovery; other checks handle a
		// leader whose postgres is down or restarting.
		{name: "named leader, postgres starting", p: withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STARTING)},
		{name: "named leader, postgres unknown", p: withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_UNKNOWN)},
		{name: "no status reported", p: poolerWithLeaderTerm(t, 5)},
		{name: "empty report", p: &PoolerReport{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, LeaderPostgresDemoted(*tc.p))
		})
	}

	t.Run("named leader only by an RPC-recorded rule while promote is under way", func(t *testing.T) {
		// Promote records the proposed rule before pg_promote; the rule isn't in
		// the pooler's WAL yet, so a standby postgres is expected.
		p := withPostgres(&PoolerReport{Consensus: &clustermetadatapb.ConsensusStatus{Id: &clustermetadatapb.ID{Name: "mp1"}}}, clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY)
		p.Consensus.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				LeaderId:   &clustermetadatapb.ID{Name: "mp1"},
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
			}},
		}
		assert.False(t, LeaderPostgresDemoted(*p))
	})

	t.Run("another node is the named leader", func(t *testing.T) {
		p := withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY)
		p.Consensus.CurrentPosition.Position.Decision.LeaderId = &clustermetadatapb.ID{Name: "other"}
		assert.False(t, LeaderPostgresDemoted(*p))
	})

	t.Run("a demoted leader needs replacement", func(t *testing.T) {
		p := withPostgres(poolerWithLeaderTerm(t, 5), clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY)
		assert.True(t, LeaderNeedsReplacement(*p))
	})
}

func TestPrefersNotToLead(t *testing.T) {
	assert.False(t, PrefersNotToLead(nil), "older poolers state no preference")
	assert.False(t, PrefersNotToLead(&clustermetadatapb.AvailabilityStatus{}))
	assert.True(t, PrefersNotToLead(&clustermetadatapb.AvailabilityStatus{
		Intent: &clustermetadatapb.AvailabilityIntent{
			LeadershipPreference: clustermetadatapb.LeadershipPreference_LEADERSHIP_PREFERENCE_PREFER_NOT,
		},
	}))
}
