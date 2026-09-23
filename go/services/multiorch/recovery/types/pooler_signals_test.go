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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestPoolerIsCohortIneligible(t *testing.T) {
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
				CohortEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_UNKNOWN,
			},
			want: false,
		},
		{
			name: "ELIGIBLE returns false",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_ELIGIBLE,
			},
			want: false,
		},
		{
			name: "INELIGIBLE returns true",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, PoolerIsCohortIneligible(tc.av))
		})
	}
}

func poolerHealth(t *testing.T) *multiorchdatapb.PoolerHealthState {
	t.Helper()
	id := &clustermetadatapb.ID{Name: "mp1"}
	return &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: id},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					LeaderId:   id,
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				}},
			},
		},
	}
}

func TestLeaderNeedsReplacement(t *testing.T) {
	t.Run("nil PoolerHealthState returns false", func(t *testing.T) {
		assert.False(t, LeaderNeedsReplacement(nil))
	})

	t.Run("no AvailabilityStatus returns false", func(t *testing.T) {
		p := poolerHealth(t)
		assert.False(t, LeaderNeedsReplacement(p))
	})

	t.Run("AvailabilityStatus with no signals returns false", func(t *testing.T) {
		p := poolerHealth(t)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{}
		assert.False(t, LeaderNeedsReplacement(p))
	})

	t.Run("continue_leadership_signal INELIGIBLE returns true", func(t *testing.T) {
		p := poolerHealth(t)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			ContinueLeadershipSignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE,
		}
		assert.True(t, LeaderNeedsReplacement(p))
	})

	t.Run("continue_leadership_signal ELIGIBLE returns false", func(t *testing.T) {
		p := poolerHealth(t)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			ContinueLeadershipSignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_ELIGIBLE,
		}
		assert.False(t, LeaderNeedsReplacement(p))
	})

	t.Run("CohortEligibility INELIGIBLE returns true even without a leadership signal", func(t *testing.T) {
		// Graceful-shutdown path: the pooler advertises INELIGIBLE without
		// touching continue_leadership_signal, and the analyzer must still
		// trigger replacement.
		p := poolerHealth(t)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE,
		}
		assert.True(t, LeaderNeedsReplacement(p))
	})

	t.Run("CohortEligibility ELIGIBLE returns false", func(t *testing.T) {
		p := poolerHealth(t)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_ELIGIBLE,
		}
		assert.False(t, LeaderNeedsReplacement(p))
	})
}

func TestPoolerPrefersNotToBecomeLeader(t *testing.T) {
	tests := []struct {
		name string
		av   *clustermetadatapb.AvailabilityStatus
		want bool
	}{
		{name: "nil availability status treated as willing", av: nil, want: false},
		{
			name: "UNKNOWN (default) treated as willing",
			av:   &clustermetadatapb.AvailabilityStatus{},
			want: false,
		},
		{
			name: "ELIGIBLE returns false",
			av: &clustermetadatapb.AvailabilityStatus{
				BecomeLeaderEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_ELIGIBLE,
			},
			want: false,
		},
		{
			name: "INELIGIBLE returns true",
			av: &clustermetadatapb.AvailabilityStatus{
				BecomeLeaderEligibilitySignal: clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, PoolerPrefersNotToBecomeLeader(tc.av))
		})
	}
}
