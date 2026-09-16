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

package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// decidedStatus builds a ConsensusStatus decided at term, naming leader and
// carrying cohort as the durability-relevant cohort members.
func decidedStatus(term int64, leader string, cohort []*clustermetadatapb.ID, policy *clustermetadatapb.DurabilityPolicy) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber:       rn(term, 0),
					LeaderId:         leaderID(leader),
					CohortMembers:    cohort,
					DurabilityPolicy: policy,
				},
			},
		},
	}
}

func TestConfirmLatestRule(t *testing.T) {
	poolerA := id("pooler-a", "zone1")
	poolerB := id("pooler-b", "zone1")
	poolerC := id("pooler-c", "zone1")
	cohort := []*clustermetadatapb.ID{poolerA, poolerB, poolerC}
	policy := topoclient.AtLeastN(2)

	t.Run("single clean decision, fully reached: confirmed", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{decidedStatus(5, "a", cohort, policy)}
		rule, confidence, err := ConfirmLatestRule(statuses, cohort)
		require.NoError(t, err)
		assert.Equal(t, RuleConfidenceConfirmed, confidence)
		assert.Equal(t, int64(5), rule.GetDecision().GetRuleNumber().GetCoordinatorTerm())
	})

	t.Run("decided rule with a pending proposal: proposal preserved", func(t *testing.T) {
		status := decidedStatus(5, "a", cohort, policy)
		status.CurrentPosition.Position.Proposal = &clustermetadatapb.ShardRule{
			RuleNumber: rn(6, 0),
			LeaderId:   leaderID("b"),
		}
		rule, confidence, err := ConfirmLatestRule([]*clustermetadatapb.ConsensusStatus{status}, cohort)
		require.NoError(t, err)
		assert.Equal(t, RuleConfidenceConfirmed, confidence)
		assert.Equal(t, int64(5), rule.GetDecision().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, int64(6), rule.GetProposal().GetRuleNumber().GetCoordinatorTerm())
	})

	t.Run("overlapping primary claims at different terms: higher term wins, no error", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			decidedStatus(5, "a", cohort, policy),
			decidedStatus(7, "b", cohort, policy),
		}
		rule, confidence, err := ConfirmLatestRule(statuses, cohort)
		require.NoError(t, err)
		assert.Equal(t, RuleConfidenceConfirmed, confidence)
		assert.Equal(t, "b", rule.GetDecision().GetLeaderId().GetName())
	})

	t.Run("rogue quorum: unreached members alone satisfy policy -> unconfirmed", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{decidedStatus(5, "a", cohort, policy)}
		rule, confidence, err := ConfirmLatestRule(statuses, []*clustermetadatapb.ID{poolerA})
		require.NoError(t, err)
		assert.Equal(t, RuleConfidenceUnconfirmed, confidence)
		assert.Equal(t, int64(5), rule.GetDecision().GetRuleNumber().GetCoordinatorTerm())
	})

	t.Run("rogue quorum: unreached members alone insufficient -> confirmed", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{decidedStatus(5, "a", cohort, policy)}
		rule, confidence, err := ConfirmLatestRule(statuses, []*clustermetadatapb.ID{poolerA, poolerB})
		require.NoError(t, err)
		assert.Equal(t, RuleConfidenceConfirmed, confidence)
		assert.Equal(t, int64(5), rule.GetDecision().GetRuleNumber().GetCoordinatorTerm())
	})

	t.Run("no rule observed: error", func(t *testing.T) {
		rule, confidence, err := ConfirmLatestRule(nil, nil)
		require.Error(t, err)
		assert.Nil(t, rule)
		assert.Equal(t, RuleConfidenceUnconfirmed, confidence)
	})
}
