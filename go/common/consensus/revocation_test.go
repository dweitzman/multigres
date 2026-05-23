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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// coordA and coordB are distinct coordinator IDs used across revocation tests.
var (
	coordA = &clustermetadatapb.ID{Name: "coord-a"}
	coordB = &clustermetadatapb.ID{Name: "coord-b"}

	ts1 = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ts2 = timestamppb.New(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))

	// zeroOutgoingRule is a placeholder outgoing_rule used in tests where the
	// specific outgoing rule value doesn't matter — only that it's non-nil so
	// ValidateRevocation accepts the revocation.
	zeroOutgoingRule = &clustermetadatapb.RuleNumber{}
)

func TestValidateRevocation(t *testing.T) {
	// outgoingAt4 represents the highest rule observed across the cohort when
	// the revocation was authored — coordinator_term=4, which is what you'd see
	// when the node being tested has committed rule at term=4.
	outgoingAt4 := &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
	revocationAt5 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		AcceptedCoordinatorId:  coordA,
		CoordinatorInitiatedAt: ts1,
		OutgoingDecision:       outgoingAt4,
	}

	tests := []struct {
		name       string
		status     *clustermetadatapb.ConsensusStatus
		revocation *clustermetadatapb.TermRevocation
		wantErr    string
	}{
		{
			name:       "NilRevocation_Refused",
			status:     nil,
			revocation: nil,
			wantErr:    "revocation is nil",
		},
		{
			name:   "NilCoordinatorID_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				CoordinatorInitiatedAt: ts1,
				OutgoingDecision:       zeroOutgoingRule,
			},
			wantErr: "accepted_coordinator_id is required",
		},
		{
			name:   "NilTimestamp_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordA,
				OutgoingDecision:      zeroOutgoingRule,
			},
			wantErr: "coordinator_initiated_at is required",
		},
		{
			name:   "NilOutgoingRule_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
			},
			wantErr: "outgoing_rule is required",
		},
		{
			// outgoing_rule.coordinator_term must be strictly less than
			// revoked_below_term. NewTermRevocation always produces values that
			// satisfy this, but a hand-built revocation (e.g. from an external
			// agent constructing a cert) could violate it and ValidateRevocation
			// catches it on read.
			name:   "OutgoingRuleTermAtOrAboveRevokedBelow_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
				OutgoingDecision:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			},
			wantErr: "outgoing_rule coordinator_term 5 >= revoked_below_term 5",
		},
		{
			name:       "NilStatus_Refused",
			status:     nil,
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: unknown WAL position",
		},
		{
			name:       "NilPosition_Refused",
			status:     &clustermetadatapb.ConsensusStatus{},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: unknown WAL position",
		},
		{
			name: "BadLsn_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{
							CoordinatorTerm: 4,
						},
					},
					Lsn: "abc",
				},
			},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: failed to parse LSN: unexpected EOF",
		},
		{
			name: "WALSafety_RuleTermBelowRevocation_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
		},
		{
			name: "WALSafety_RuleTermEqualsRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(5),
			},
			revocation: revocationAt5,
			wantErr:    "coordinator term 5 >= revoked_below_term 5",
		},
		{
			name: "WALSafety_RuleTermAboveRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(7),
			},
			revocation: revocationAt5,
			wantErr:    "coordinator term 7 >= revoked_below_term 5",
		},
		{
			// if the committed rule is beyond outgoing_rule, the
			// revocation is stale
			name: "CommittedRuleBeyondOutgoingRule_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(1),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       2,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
				OutgoingDecision:       zeroOutgoingRule, // {0,0} < committed {1,0}
			},
			wantErr: "committed rule (1.0) is beyond outgoing_rule (0.0)",
		},
		{
			// committed rule equals outgoing_rule — not beyond it, so OK.
			name: "CommittedRuleEqualsOutgoingRule_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(0),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       1,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
				OutgoingDecision:       zeroOutgoingRule, // {0,0} == committed {0,0}
			},
		},
		{
			// Stored term=10 > requested term=5, and stored outgoing_rule >= incoming
			// outgoing_rule so the override rule does not apply.
			name: "StoredTerm_HigherThanRequested_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      10,
					AcceptedCoordinatorId: coordA,
					OutgoingDecision:      outgoingAt4, // same as incoming — no override
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
			wantErr:    "already accepted term 10 > requested 5",
		},
		{
			// incoming revocation has a higher outgoing_rule than the
			// stored one, so it supersedes it even though revokedBelowTerm is lower.
			// Scenario: stored is "outgoing=2 → term=5", incoming is "outgoing=3 → term=4".
			name: "StoredTerm_HigherButIncomingHasHigherOutgoingRule_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      5,
					AcceptedCoordinatorId: coordA,
					OutgoingDecision:      &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				},
				CurrentPosition: positionAtCoordTerm(3),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       4,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
				OutgoingDecision:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
			},
		},
		{
			name: "StoredTerm_LowerThanRequested_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      3,
					AcceptedCoordinatorId: coordA,
				},
				CurrentPosition: positionAtCoordTerm(2),
			},
			revocation: revocationAt5,
		},
		{
			name: "SameTerm_SameCoordinator_SameTimestamp_Idempotent",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation:  revocationAt5,
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
		},
		{
			name: "SameTerm_DifferentCoordinator_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordA,
					CoordinatorInitiatedAt: ts1,
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordB,
				CoordinatorInitiatedAt: ts1,
				OutgoingDecision:       outgoingAt4,
			},
			wantErr: "already accepted term 5 from coordinator",
		},
		{
			name: "SameTerm_SameCoordinator_DifferentTimestamp_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordA,
					CoordinatorInitiatedAt: ts1,
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts2,
				OutgoingDecision:       outgoingAt4,
			},
			wantErr: "different coordinator_initiated_at",
		},
		{
			name: "WALAndStoredTerm_BothChecked_WALFails",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(6),
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      3,
					AcceptedCoordinatorId: coordA,
				},
			},
			revocation: revocationAt5,
			wantErr:    "coordinator term 6 >= revoked_below_term 5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRevocation(tc.status, tc.revocation)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// positionAtCoordTerm builds a PoolerPosition whose recorded rule is at the
// given coordinator term.
func positionAtCoordTerm(coordTerm int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordTerm,
			},
		},
		Lsn: "16/B374D848",
	}
}

func TestNewTermRevocation(t *testing.T) {
	coord := &clustermetadatapb.ID{Name: "coord-1"}

	t.Run("empty statuses returns error", func(t *testing.T) {
		rev, err := NewTermRevocation(nil, coord)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "statuses must be non-empty")
		require.Nil(t, rev)
	})

	t.Run("no cohort member has a recorded rule returns error", func(t *testing.T) {
		// Bootstrap-shaped scenario: cohort visible but nobody reports a
		// rule. NewTermRevocation refuses; the agent should construct the
		// revocation directly with an explicit outgoing_rule.
		statuses := []*clustermetadatapb.ConsensusStatus{{}, {}}
		rev, err := NewTermRevocation(statuses, coord)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no cohort member reports a recorded rule")
		require.Nil(t, rev)
	})

	t.Run("revocation-term-only statuses with no recorded rule return error", func(t *testing.T) {
		// Same shape: statuses carry a stored revocation but no recorded
		// rule. NewTermRevocation requires at least one rule to derive
		// outgoing_rule from.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}},
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7}},
		}
		rev, err := NewTermRevocation(statuses, coord)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no cohort member reports a recorded rule")
		require.Nil(t, rev)
	})

	t.Run("revocation term + recorded rule: max across both", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7},
				CurrentPosition: positionAtCoordTerm(4),
			},
			{
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5},
				CurrentPosition: positionAtCoordTerm(4),
			},
			{
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: positionAtCoordTerm(4),
			},
		}
		rev, err := NewTermRevocation(statuses, coord)
		require.NoError(t, err)
		require.Equal(t, int64(8), rev.GetRevokedBelowTerm())
		require.Equal(t, "coord-1", rev.GetAcceptedCoordinatorId().GetName())
		require.NotNil(t, rev.GetCoordinatorInitiatedAt())
		require.Equal(t, int64(4), rev.GetOutgoingDecision().GetCoordinatorTerm())
	})

	t.Run("uses max of recorded rule terms", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: positionAtCoordTerm(4)},
			{CurrentPosition: positionAtCoordTerm(9)},
		}
		rev, err := NewTermRevocation(statuses, coord)
		require.NoError(t, err)
		require.Equal(t, int64(10), rev.GetRevokedBelowTerm())
	})

	t.Run("takes max across both revocation and rule terms", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 6}},
			{CurrentPosition: positionAtCoordTerm(11)},
		}
		rev, err := NewTermRevocation(statuses, coord)
		require.NoError(t, err)
		require.Equal(t, int64(12), rev.GetRevokedBelowTerm())
	})

	t.Run("outgoing_rule picks the highest RuleNumber across statuses", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: &clustermetadatapb.PoolerPosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4, LeaderSubterm: 2},
				},
				Lsn: "16/B374D848",
			}},
			{CurrentPosition: &clustermetadatapb.PoolerPosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4, LeaderSubterm: 5},
				},
				Lsn: "16/B374D900",
			}},
			{CurrentPosition: &clustermetadatapb.PoolerPosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 9},
				},
				Lsn: "16/B374D700",
			}},
		}
		rev, err := NewTermRevocation(statuses, coord)
		require.NoError(t, err)
		got := rev.GetOutgoingDecision()
		require.NotNil(t, got)
		require.Equal(t, int64(4), got.GetCoordinatorTerm())
		require.Equal(t, int64(5), got.GetLeaderSubterm())
	})
}

func TestIsRuleRevoked(t *testing.T) {
	ruleAt := func(term, subterm int64) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm},
		}
	}
	revocation := func(revokedBelow int64, outgoing *clustermetadatapb.RuleNumber) *clustermetadatapb.TermRevocation {
		return &clustermetadatapb.TermRevocation{RevokedBelowTerm: revokedBelow, OutgoingDecision: outgoing}
	}
	ruleNum := func(term, subterm int64) *clustermetadatapb.RuleNumber {
		return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm}
	}

	tests := []struct {
		name       string
		rule       *clustermetadatapb.ShardRule
		revocation *clustermetadatapb.TermRevocation
		want       bool
	}{
		{
			name:       "NilRevocation_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: nil,
			want:       false,
		},
		{
			name:       "ZeroRevokedBelow_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(0, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleTermAboveRevokedBelow_NotRevoked",
			rule:       ruleAt(5, 0),
			revocation: revocation(3, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleTermEqualsRevokedBelow_NotRevoked",
			rule:       ruleAt(3, 0),
			revocation: revocation(3, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndOverridesOutgoing_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndOverridesOutgoingBySubterm_NotRevoked",
			rule:       ruleAt(2, 5),
			revocation: revocation(3, ruleNum(2, 4)),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndEqualsOutgoing_Revoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, ruleNum(2, 0)),
			want:       true,
		},
		{
			name:       "RuleBelowRevokedAndBelowOutgoing_Revoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, ruleNum(2, 5)),
			want:       true,
		},
		{
			name:       "RuleBelowRevokedAndOutgoingNil_Revoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, nil),
			want:       true,
		},
		{
			name:       "RuleBelowRevokedAndOutgoingZero_Revoked",
			rule:       ruleAt(0, 0),
			revocation: revocation(3, &clustermetadatapb.RuleNumber{}),
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRuleRevoked(tt.rule, tt.revocation)
			assert.Equal(t, tt.want, got)
		})
	}
}
