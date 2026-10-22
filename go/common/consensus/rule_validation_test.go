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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestValidateRuleContents(t *testing.T) {
	a := id("a", "cell1")
	b := id("b", "cell1")
	c := id("c", "cell1")
	outsider := id("outsider", "cell1")
	cohort := []*clustermetadatapb.ID{a, b, c}

	tests := []struct {
		name    string
		leader  *clustermetadatapb.ID
		cohort  []*clustermetadatapb.ID
		policy  *clustermetadatapb.DurabilityPolicy
		wantErr string
	}{
		{name: "leader in the cohort", leader: a, cohort: cohort, policy: atLeast(2)},
		{name: "no leader yet", cohort: cohort, policy: atLeast(2)},
		{name: "no cohort yet", leader: a, policy: atLeast(2)},
		{
			name:    "leader outside the cohort",
			leader:  outsider,
			cohort:  cohort,
			policy:  atLeast(2),
			wantErr: "leader cell1_outsider is not in the cohort",
		},
		{
			name:    "cohort too small for the policy",
			leader:  a,
			cohort:  []*clustermetadatapb.ID{a, b},
			policy:  atLeast(3),
			wantErr: "cohort cannot achieve durability policy: durability not satisfied: 2 poolers, required 3",
		},
		{
			name:    "missing policy",
			leader:  a,
			cohort:  cohort,
			wantErr: "invalid durability policy: durability policy is nil",
		},
		{
			name:    "unsupported quorum type",
			leader:  a,
			cohort:  cohort,
			policy:  &clustermetadatapb.DurabilityPolicy{RequiredCount: 2},
			wantErr: "invalid durability policy: unsupported quorum type: QUORUM_TYPE_UNKNOWN",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRuleContents(tt.leader, tt.cohort, tt.policy)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

// The incoming rule's number is set from the kind of change, so these rows are about
// the contents and the policy transition. See TestValidateRuleChange_RuleNumber.
func TestValidateRuleChange_PolicyTransition(t *testing.T) {
	a, b, c, d, e := id("a", "cell1"), id("b", "cell1"), id("c", "cell1"), id("d", "cell1"), id("e", "cell1")
	f, g := id("f", "cell2"), id("g", "cell3") // other cells, so a multi-cell policy can hold
	rule := func(policy *clustermetadatapb.DurabilityPolicy, leader *clustermetadatapb.ID, cohort ...*clustermetadatapb.ID) *clustermetadatapb.ShardRule {
		r := makeRule(ruleNum(1, 0), policy, cohort...)
		r.LeaderId = leader
		return r
	}
	multiCell := &clustermetadatapb.DurabilityPolicy{
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
		RequiredCount: 2,
	}

	type rules struct{ outgoing, incoming *clustermetadatapb.ShardRule }
	between := func(outgoing, incoming *clustermetadatapb.ShardRule) rules { return rules{outgoing, incoming} }

	tests := []struct {
		name         string
		rules        rules
		kind         RuleChangeKind // LeaderLedChange when unset
		skip         bool           // skip the outgoing quorum
		wantErr      string
		wantBoth     []string // cohort of the policy applied before the write
		wantIncoming []string // cohort of the policy applied after the write
	}{
		{
			name:         "unchanged rule",
			rules:        between(rule(atLeast(2), a, a, b, c), rule(atLeast(2), a, a, b, c)),
			wantBoth:     []string{"cell1_a", "cell1_b", "cell1_c"},
			wantIncoming: []string{"cell1_a", "cell1_b", "cell1_c"},
		},
		{
			name:         "adding a member writes under the old cohort first",
			rules:        between(rule(atLeast(2), a, a, b, c), rule(atLeast(2), a, a, b, c, d)),
			wantBoth:     []string{"cell1_a", "cell1_b", "cell1_c"},
			wantIncoming: []string{"cell1_a", "cell1_b", "cell1_c", "cell1_d"},
		},
		{
			name:         "removing a member writes under the new cohort",
			rules:        between(rule(atLeast(2), a, a, b, c), rule(atLeast(2), a, a, b)),
			wantBoth:     []string{"cell1_a", "cell1_b"},
			wantIncoming: []string{"cell1_a", "cell1_b"},
		},
		{
			name:         "raising N keeps the cohort",
			rules:        between(rule(atLeast(2), a, a, b, c), rule(atLeast(3), a, a, b, c)),
			wantBoth:     []string{"cell1_a", "cell1_b", "cell1_c"},
			wantIncoming: []string{"cell1_a", "cell1_b", "cell1_c"},
		},
		{
			name:    "changing N and the cohort together is unsupported",
			rules:   between(rule(atLeast(2), a, a, b, c), rule(atLeast(3), a, a, b, c, d)),
			wantErr: "compute GUC transition: unsupported leader-led rule change: both N and cohort changed simultaneously",
		},
		{
			name:    "cohorts with no subset relation are unsupported",
			rules:   between(rule(atLeast(2), a, a, b, c), rule(atLeast(2), a, a, d, e)),
			wantErr: "compute GUC transition: unsupported leader-led rule change: neither cohort is a subset of the other",
		},
		{
			name:    "changing the policy family is unsupported",
			rules:   between(rule(atLeast(2), a, a, f, g), rule(multiCell, a, a, f, g)),
			wantErr: "compute GUC transition: unsupported leader-led rule change: policy types must match (got consensus.AtLeastNPolicy and consensus.multiCellPolicy)",
		},
		{
			name:    "a leader outside the incoming cohort",
			rules:   between(rule(atLeast(2), a, a, b, c), rule(atLeast(2), d, a, b, c)),
			wantErr: "leader cell1_d is not in the cohort",
		},
		{
			name:  "skipping the outgoing quorum allows any change, applied as one policy",
			rules: between(rule(atLeast(2), a, a, b, c), rule(atLeast(3), a, a, d, e)), kind: CoordinatorLedChange, skip: true,
			wantBoth:     []string{"cell1_a", "cell1_d", "cell1_e"},
			wantIncoming: []string{"cell1_a", "cell1_d", "cell1_e"},
		},
		{
			name:  "skipping the outgoing quorum still checks the incoming rule",
			rules: between(rule(atLeast(2), a, a, b, c), rule(atLeast(2), b, a, d, e)), kind: CoordinatorLedChange, skip: true,
			wantErr: "leader cell1_b is not in the cohort",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind := tt.kind
			if kind == 0 {
				kind = LeaderLedChange
			}
			incoming := proto.Clone(tt.rules.incoming).(*clustermetadatapb.ShardRule)
			incoming.RuleNumber = ruleNum(1, 1) // follows the outgoing (1,0) for a leader-led change
			if kind != LeaderLedChange {
				incoming.RuleNumber = ruleNum(2, 0)
			}

			transition, err := ValidateRuleChange(kind, tt.rules.outgoing, incoming, tt.skip)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantBoth, clusterIDStrings(transition.Both.Cohort))
			assert.ElementsMatch(t, tt.wantIncoming, clusterIDStrings(transition.Incoming.Cohort))
		})
	}
}

// Both kinds of change raise the rule number; they differ in how. A leader-led change
// keeps the coordinator term and advances the leader subterm by one. A coordinator-led
// change (with or without a cert) raises the coordinator term and restarts the subterm.
func TestValidateRuleChange_RuleNumber(t *testing.T) {
	a, b, c := id("a", "cell1"), id("b", "cell1"), id("c", "cell1")
	ruleAt := func(number *clustermetadatapb.RuleNumber) *clustermetadatapb.ShardRule {
		r := makeRule(number, atLeast(2), a, b, c)
		r.LeaderId = a
		return r
	}

	tests := []struct {
		name     string
		kind     RuleChangeKind
		outgoing *clustermetadatapb.RuleNumber
		incoming *clustermetadatapb.RuleNumber
		wantErr  string
	}{
		{name: "leader-led advances the subterm by one", kind: LeaderLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(3, 3)},
		{
			name: "leader-led cannot repeat the rule number", kind: LeaderLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(3, 2),
			wantErr: "incoming rule number 3.2 must exceed the outgoing rule number 3.2",
		},
		{
			name: "leader-led cannot go back", kind: LeaderLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(3, 1),
			wantErr: "incoming rule number 3.1 must exceed the outgoing rule number 3.2",
		},
		{
			name: "leader-led cannot skip a subterm", kind: LeaderLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(3, 4),
			wantErr: "a leader-led change advances the leader subterm by one: outgoing 2, incoming 4",
		},
		{
			name: "leader-led cannot raise the coordinator term", kind: LeaderLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(4, 0),
			wantErr: "a leader-led change keeps the coordinator term: outgoing 3, incoming 4",
		},
		{name: "coordinator-led raises the term and restarts the subterm", kind: CoordinatorLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(4, 0)},
		{
			name: "coordinator-led cannot keep the term", kind: CoordinatorLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(3, 3),
			wantErr: "a coordinator-led change must increase the coordinator term: outgoing 3, incoming 3",
		},
		{
			name: "coordinator-led cannot lower the term", kind: CoordinatorLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(2, 0),
			wantErr: "incoming rule number 2.0 must exceed the outgoing rule number 3.2",
		},
		{
			name: "coordinator-led must restart the subterm", kind: CoordinatorLedChange, outgoing: ruleNum(3, 2), incoming: ruleNum(4, 1),
			wantErr: "a coordinator-led change starts at leader subterm 0, got 1",
		},
		{name: "coordinator-led raises the term from the initial rule", kind: CoordinatorLedChange, outgoing: ruleNum(0, 1), incoming: ruleNum(5, 0)},
		{
			name: "an incoming rule needs a rule number", kind: LeaderLedChange, outgoing: ruleNum(3, 2), incoming: nil,
			wantErr: "incoming rule has no rule number",
		},
		{
			name: "a change needs a kind", kind: 0, outgoing: ruleNum(3, 2), incoming: ruleNum(3, 3),
			wantErr: "unknown rule change kind 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			incoming := ruleAt(tt.incoming)
			incoming.RuleNumber = tt.incoming

			_, err := ValidateRuleChange(tt.kind, ruleAt(tt.outgoing), incoming, false)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestValidateRuleChange_SkippingTheOutgoingQuorum(t *testing.T) {
	a, b, c, d := id("a", "cell1"), id("b", "cell1"), id("c", "cell1"), id("d", "cell1")
	outgoing := makeRule(ruleNum(3, 0), atLeast(2), a, b, c)
	outgoing.LeaderId = a
	incoming := makeRule(ruleNum(4, 0), atLeast(3), a, b, c, d) // N and the cohort change together
	incoming.LeaderId = a

	t.Run("a coordinator-led change may skip it", func(t *testing.T) {
		_, err := ValidateRuleChange(CoordinatorLedChange, outgoing, incoming, true)
		require.NoError(t, err)
	})

	t.Run("without skipping, the policy must be able to move", func(t *testing.T) {
		_, err := ValidateRuleChange(CoordinatorLedChange, outgoing, incoming, false)

		require.EqualError(t, err, "compute GUC transition: unsupported leader-led rule change: both N and cohort changed simultaneously")
	})

	t.Run("a leader-led change may not skip it", func(t *testing.T) {
		leaderLed := makeRule(ruleNum(3, 1), atLeast(2), a, b, c)
		leaderLed.LeaderId = a

		_, err := ValidateRuleChange(LeaderLedChange, outgoing, leaderLed, true)

		require.EqualError(t, err, "a leader-led change cannot skip the outgoing quorum")
	})
}

func TestValidateRuleChange_Attestation(t *testing.T) {
	a, b, c := id("a", "cell1"), id("b", "cell1"), id("c", "cell1")
	outgoing := makeRule(ruleNum(3, 0), atLeast(2), a, b, c)
	outgoing.LeaderId = a
	incoming := func() *clustermetadatapb.ShardRule {
		r := makeRule(ruleNum(3, 1), atLeast(2), a, b, c)
		r.LeaderId = a
		return r
	}

	_, err := ValidateRuleChange(LeaderLedChange, outgoing, incoming(), false)
	require.NoError(t, err)

	noCoordinator := incoming()
	noCoordinator.CoordinatorId = nil
	_, err = ValidateRuleChange(LeaderLedChange, outgoing, noCoordinator, false)
	require.EqualError(t, err, "incoming rule has no coordinator_id")

	noCreationTime := incoming()
	noCreationTime.CreationTime = nil
	_, err = ValidateRuleChange(LeaderLedChange, outgoing, noCreationTime, false)
	require.EqualError(t, err, "incoming rule has no creation_time")
}
