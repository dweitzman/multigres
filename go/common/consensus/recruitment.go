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
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// RecruitmentResult holds the interpreted outcome of a successful recruitment
// round. It is passed to the buildProposal callback in CheckSufficientRecruitment.
type RecruitmentResult struct {
	// TermRevocation is the revocation used for this recruitment round,
	// taken from the first status (all recruited nodes accept the same one).
	TermRevocation *consensusdatapb.TermRevocation

	// BestRule is the highest committed ShardRule across all recruited nodes,
	// determined from current_position.rule (WAL-backed, authoritative).
	BestRule *consensusdatapb.ShardRule

	// EligibleLeaders are recruited nodes whose committed rule_number equals
	// BestRule's. These are the candidates the callback may choose as leader.
	//
	// For a hung cohort change, EligibleLeaders contains only the nodes that
	// already have the hung rule in their WAL — they are the only nodes that
	// can re-propose it without writing a new rule_history entry.
	EligibleLeaders []*consensusdatapb.ConsensusStatus

	// HungProposal is non-nil when a cohort-change ShardRule reached WAL on
	// some recruited nodes but has not yet achieved durability. When non-nil:
	//   - The callback MUST re-propose this exact ShardRule (same rule_number
	//     and cohort). Only the leader may change.
	//   - On the promote path the new leader detects that the rule already
	//     exists in WAL and writes a fencing transaction instead of a new
	//     rule_history entry to force replication of the existing WAL position.
	HungProposal *consensusdatapb.ShardRule
}

// CheckSufficientRecruitment validates that the recruited nodes allow a safe
// leadership transition, calls buildProposal to obtain a proposal, then validates
// the proposal against the recruitment constraints before returning it.
//
// The statuses should be the ConsensusStatus values returned from Recruit() RPCs.
// All statuses are assumed to have accepted the same TermRevocation.
//
// # Hung cohort change detection
//
// A hung cohort change is detected when recruited nodes disagree on their
// committed cohort: some have a higher-numbered rule with a new cohort (the
// hung rule) while others are still at the older rule. Detection uses only
// WAL-backed current_position.rule — in-memory fields like highest_known_decision
// are not considered.
//
// # Pre-callback quorum validation
//
//   - Normal leader change: recruited ∩ current_cohort must satisfy the current
//     rule's durability policy.
//   - Hung cohort change: recruited ∩ smaller_cohort must satisfy the smaller
//     cohort's durability policy. "Smaller" is whichever of the before/after
//     cohorts has fewer members; since one is always a subset of the other in
//     the current system, this is equivalent to checking the stricter quorum.
//
// # Post-callback proposal validation
//
//  1. proposal.ProposalLeaderId must be among EligibleLeaders.
//  2. All proposed_rule.CohortMembers must be drawn from the recruited set (ensures
//     no node outside the revocation set joins the new cohort).
//  3. The proposed_rule's durability policy must be achievable with its cohort.
//  4. If HungProposal is non-nil, proposed_rule.RuleNumber must equal HungProposal.RuleNumber.
func CheckSufficientRecruitment(
	statuses []*consensusdatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	if len(statuses) == 0 {
		return nil, fmt.Errorf("no recruitment statuses provided")
	}

	// Step 1: Find the best committed rule (highest RuleNumber) across all
	// recruited nodes. This is the authoritative "before" state for a normal
	// leader change, and the hung rule for a cohort change.
	var bestRule *consensusdatapb.ShardRule
	for _, cs := range statuses {
		rule := cs.GetCurrentPosition().GetRule()
		if CompareRuleNumbers(rule.GetRuleNumber(), bestRule.GetRuleNumber()) > 0 {
			bestRule = rule
		}
	}
	if bestRule == nil {
		return nil, fmt.Errorf("no committed rule found among recruited nodes; cannot determine cohort")
	}

	// Step 2: Detect a hung cohort change.
	// A hung change exists if any recruited node is at an older rule with a
	// different cohort than bestRule (meaning bestRule's cohort change has not
	// yet achieved durability).
	var priorRule *consensusdatapb.ShardRule
	for _, cs := range statuses {
		rule := cs.GetCurrentPosition().GetRule()
		if rule == nil || CompareRuleNumbers(rule.GetRuleNumber(), bestRule.GetRuleNumber()) == 0 {
			continue
		}
		if !sameCohort(rule.GetCohortMembers(), bestRule.GetCohortMembers()) {
			if priorRule == nil || CompareRuleNumbers(rule.GetRuleNumber(), priorRule.GetRuleNumber()) > 0 {
				priorRule = rule
			}
		}
	}
	var hungProposal *consensusdatapb.ShardRule
	if priorRule != nil {
		hungProposal = bestRule
	}

	// Step 3: Choose the rule whose cohort and durability policy govern the
	// quorum check. For a hung change, use whichever of the two cohorts is
	// smaller (fewer members), since quorum of the smaller cohort guarantees
	// intersection with any quorum of the larger one.
	quorumRule := bestRule
	if hungProposal != nil && len(priorRule.GetCohortMembers()) < len(bestRule.GetCohortMembers()) {
		quorumRule = priorRule
	}

	// Step 4: Validate quorum.
	policy, err := NewPolicyFromProto(quorumRule.GetDurabilityPolicy())
	if err != nil {
		return nil, fmt.Errorf("failed to parse durability policy from rule: %w", err)
	}
	quorumCohort := quorumRule.GetCohortMembers()
	recruitedInCohort := cohortIntersect(quorumCohort, statuses)
	if err := policy.CheckSufficientRecruitment(quorumCohort, recruitedInCohort); err != nil {
		return nil, fmt.Errorf("insufficient recruitment: %w", err)
	}

	// Step 5: Build EligibleLeaders — the candidates the callback may choose from.
	// For a hung change, only nodes that already have the hung rule in WAL are
	// eligible; they can re-propose it without writing a new rule_history entry.
	// For a normal change, any node at bestRule is eligible.
	targetRule := bestRule
	eligibleLeaders := make([]*consensusdatapb.ConsensusStatus, 0, len(statuses))
	for _, cs := range statuses {
		rule := cs.GetCurrentPosition().GetRule()
		if CompareRuleNumbers(rule.GetRuleNumber(), targetRule.GetRuleNumber()) == 0 {
			eligibleLeaders = append(eligibleLeaders, cs)
		}
	}
	if len(eligibleLeaders) == 0 {
		return nil, fmt.Errorf("no eligible leaders found among recruited nodes")
	}

	result := RecruitmentResult{
		TermRevocation:  statuses[0].GetTermRevocation(),
		BestRule:        bestRule,
		EligibleLeaders: eligibleLeaders,
		HungProposal:    hungProposal,
	}

	// Step 6: Call the callback to build the proposal.
	proposal, err := buildProposal(result)
	if err != nil {
		return nil, fmt.Errorf("buildProposal: %w", err)
	}
	if proposal == nil {
		return nil, fmt.Errorf("buildProposal returned nil proposal")
	}

	// Step 7: Validate the returned proposal against the recruitment constraints.
	if err := validateProposal(proposal, result, statuses); err != nil {
		return nil, fmt.Errorf("proposal validation: %w", err)
	}

	return proposal, nil
}

// validateProposal checks that the returned proposal is consistent with the
// recruitment result.
func validateProposal(
	proposal *consensusdatapb.CoordinatorProposal,
	result RecruitmentResult,
	statuses []*consensusdatapb.ConsensusStatus,
) error {
	leaderID := proposal.GetProposalLeader().GetId()
	if leaderID == nil {
		return fmt.Errorf("proposal has no leader ID")
	}
	leaderKey := topoclient.ClusterIDString(leaderID)
	foundLeader := false
	for _, cs := range result.EligibleLeaders {
		if topoclient.ClusterIDString(cs.GetId()) == leaderKey {
			foundLeader = true
			break
		}
	}
	if !foundLeader {
		return fmt.Errorf("proposed leader %s is not among eligible leaders", leaderKey)
	}

	// For a hung cohort change the proposed rule must carry the same rule_number
	// as the hung rule — re-proposing a hung rule is the only safe action.
	if result.HungProposal != nil {
		proposedRN := proposal.GetProposedRule().GetRuleNumber()
		hungRN := result.HungProposal.GetRuleNumber()
		if CompareRuleNumbers(proposedRN, hungRN) != 0 {
			return fmt.Errorf("hung proposal: proposed_rule must use rule_number %v; got %v",
				hungRN, proposedRN)
		}
	}

	// All proposed cohort members must have been recruited (accepted the
	// TermRevocation). This prevents adding a node to the cohort that might
	// still be participating in the old epoch.
	recruitedKeys := make(map[string]struct{}, len(statuses))
	for _, cs := range statuses {
		if id := cs.GetId(); id != nil {
			recruitedKeys[topoclient.ClusterIDString(id)] = struct{}{}
		}
	}
	for _, member := range proposal.GetProposedRule().GetCohortMembers() {
		key := topoclient.ClusterIDString(member)
		if _, ok := recruitedKeys[key]; !ok {
			return fmt.Errorf("proposed cohort member %s was not recruited", key)
		}
	}

	// The proposed durability policy must be achievable with the proposed cohort
	// as a basic sanity check before we send Propose to all nodes.
	if r := proposal.GetProposedRule(); r != nil && r.GetDurabilityPolicy() != nil {
		p, err := NewPolicyFromProto(r.GetDurabilityPolicy())
		if err != nil {
			return fmt.Errorf("invalid durability policy in proposal: %w", err)
		}
		if err := p.CheckAchievable(r.GetCohortMembers()); err != nil {
			return fmt.Errorf("proposed durability policy not achievable with proposed cohort: %w", err)
		}
	}

	return nil
}

// sameCohort reports whether a and b contain exactly the same set of pooler IDs.
func sameCohort(a, b []*clustermetadatapb.ID) bool {
	if len(a) != len(b) {
		return false
	}
	aKeys := poolerKeysOf(a)
	for _, id := range b {
		if _, ok := aKeys[topoclient.ClusterIDString(id)]; !ok {
			return false
		}
	}
	return true
}

// cohortIntersect returns the IDs of recruited nodes (from statuses) that are
// members of cohort, deduplicated.
func cohortIntersect(cohort []*clustermetadatapb.ID, statuses []*consensusdatapb.ConsensusStatus) []*clustermetadatapb.ID {
	cohortKeys := poolerKeysOf(cohort)
	result := make([]*clustermetadatapb.ID, 0, len(cohort))
	seen := make(map[string]struct{})
	for _, cs := range statuses {
		id := cs.GetId()
		if id == nil {
			continue
		}
		key := topoclient.ClusterIDString(id)
		if _, inCohort := cohortKeys[key]; !inCohort {
			continue
		}
		if _, alreadySeen := seen[key]; alreadySeen {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, id)
	}
	return result
}
