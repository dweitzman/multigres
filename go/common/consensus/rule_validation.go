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
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ValidateRuleContents checks that a rule's leader, cohort and durability policy are
// consistent, whoever proposes the rule: coordinator-led and leader-led rule changes
// both write through it.
//
// Errors are plain; callers add their own error codes. A rule with no cohort has nothing to check, as before the first cohort is
// established. A nil leader is allowed, as before the first leader is.
func ValidateRuleContents(
	leader *clustermetadatapb.ID,
	cohort []*clustermetadatapb.ID,
	policy *clustermetadatapb.DurabilityPolicy,
) error {
	if len(cohort) == 0 {
		return nil
	}
	p, err := NewPolicyFromProto(policy)
	if err != nil {
		return fmt.Errorf("invalid durability policy: %w", err)
	}
	if err := p.SatisfiedBy(cohort); err != nil {
		return fmt.Errorf("cohort cannot achieve durability policy: %w", err)
	}
	if leader != nil {
		if _, member := poolerKeysOf(cohort)[topoclient.ClusterIDString(leader)]; !member {
			return fmt.Errorf("leader %s is not in the cohort", topoclient.ClusterIDString(leader))
		}
	}
	return nil
}

// RuleChangeKind is who proposes a rule change. It sets how the rule number moves.
type RuleChangeKind int

const (
	// LeaderLedChange is the leader amending its own rule: the coordinator term stays
	// and the leader subterm advances by one.
	LeaderLedChange RuleChangeKind = iota + 1
	// CoordinatorLedChange is a coordinator's proposal, backed by a revocation: the
	// coordinator term increases and the leader subterm starts again at zero.
	CoordinatorLedChange
)

// ValidateRuleChange checks a proposed change from the outgoing rule to the incoming
// one, however it is proposed: the incoming rule is consistent (see
// ValidateRuleContents), its rule number follows from the outgoing one for this kind
// of change, and the policy can move from the outgoing rule to it. A coordinator-led
// proposal and a leader-led update each add their own checks around this one.
//
// skipOutgoingQuorum is set when the outgoing rule cannot commit further writes (a
// cert, or bootstrap with no outgoing cohort), so no policy has to hold across the
// change. Only a coordinator-led change can skip it.
//
// It returns the policies to apply around writing the incoming rule: Both before the
// write, Incoming after it. Both satisfies the outgoing and incoming policy at once.
func ValidateRuleChange(kind RuleChangeKind, outgoing, incoming *clustermetadatapb.ShardRule, skipOutgoingQuorum bool) (*PolicyTransition, error) {
	if incoming == nil {
		return nil, errors.New("no incoming rule")
	}
	if skipOutgoingQuorum && kind == LeaderLedChange {
		return nil, errors.New("a leader-led change cannot skip the outgoing quorum")
	}
	// Identity and timing are caller-supplied attestations. A rule that drops them
	// would leave rule_history and any audit with no way to tell a missing field from
	// a deliberate zero value.
	if incoming.GetCoordinatorId() == nil {
		return nil, errors.New("incoming rule has no coordinator_id")
	}
	if incoming.GetCreationTime() == nil {
		return nil, errors.New("incoming rule has no creation_time")
	}
	if err := ValidateRuleContents(incoming.GetLeaderId(), incoming.GetCohortMembers(), incoming.GetDurabilityPolicy()); err != nil {
		return nil, err
	}
	if err := validateRuleNumberChange(kind, outgoing.GetRuleNumber(), incoming.GetRuleNumber()); err != nil {
		return nil, err
	}
	return policyTransitionBetween(outgoing, incoming, skipOutgoingQuorum)
}

func validateRuleNumberChange(kind RuleChangeKind, outgoing, incoming *clustermetadatapb.RuleNumber) error {
	if ruleNumberIsUnset(incoming) {
		return errors.New("incoming rule has no rule number")
	}
	if CompareRuleNumbers(incoming, outgoing) <= 0 {
		return fmt.Errorf("incoming rule number %s must exceed the outgoing rule number %s",
			FormatRuleNumber(incoming), FormatRuleNumber(outgoing))
	}
	switch kind {
	case LeaderLedChange:
		if incoming.GetCoordinatorTerm() != outgoing.GetCoordinatorTerm() {
			return fmt.Errorf("a leader-led change keeps the coordinator term: outgoing %d, incoming %d",
				outgoing.GetCoordinatorTerm(), incoming.GetCoordinatorTerm())
		}
		if incoming.GetLeaderSubterm() != outgoing.GetLeaderSubterm()+1 {
			return fmt.Errorf("a leader-led change advances the leader subterm by one: outgoing %d, incoming %d",
				outgoing.GetLeaderSubterm(), incoming.GetLeaderSubterm())
		}
	case CoordinatorLedChange:
		if incoming.GetCoordinatorTerm() <= outgoing.GetCoordinatorTerm() {
			return fmt.Errorf("a coordinator-led change must increase the coordinator term: outgoing %d, incoming %d",
				outgoing.GetCoordinatorTerm(), incoming.GetCoordinatorTerm())
		}
		if incoming.GetLeaderSubterm() != 0 {
			return fmt.Errorf("a coordinator-led change starts at leader subterm 0, got %d", incoming.GetLeaderSubterm())
		}
	default:
		return fmt.Errorf("unknown rule change kind %d", kind)
	}
	return nil
}

func policyTransitionBetween(outgoing, incoming *clustermetadatapb.ShardRule, skipOutgoingQuorum bool) (*PolicyTransition, error) {
	incomingPolicy, err := NewPolicyWithCohort(incoming.GetCohortMembers(), incoming.GetDurabilityPolicy())
	if err != nil {
		return nil, err
	}
	if skipOutgoingQuorum {
		return &PolicyTransition{Both: incomingPolicy, Incoming: incomingPolicy}, nil
	}
	outgoingPolicy, err := NewPolicyWithCohort(outgoing.GetCohortMembers(), outgoing.GetDurabilityPolicy())
	if err != nil {
		return nil, err
	}
	transition, err := BuildPolicyTransition(outgoingPolicy, incomingPolicy)
	if err != nil {
		return nil, fmt.Errorf("compute GUC transition: %w", err)
	}
	return transition, nil
}
