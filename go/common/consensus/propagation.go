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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CheckPropagationPossible checks whether finalising an in-WAL rule change is
// feasible given the current observed statuses. It is the propagation
// counterpart of CheckProposalPossible.
//
// revocation must have propagation_intent set. The function verifies that:
//  1. Enough members of the outgoing cohort (from revocation.outgoing_decision)
//     could accept the revocation — ensuring the in-WAL proposal has a unique
//     rule number that no parallel quorum can overwrite.
//  2. At least one potential leader has the matching proposal at the
//     most-advanced position.
//
// Returns nil if propagation appears feasible; an error otherwise. Intended
// for pre-vote feasibility checks before committing to a Recruit round.
func CheckPropagationPossible(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
) error {
	propagationIntent := revocation.GetPropagationIntent()
	if propagationIntent == nil {
		return errors.New("revocation.propagation_intent is required for propagation")
	}

	candidates := filterByPotentialRevocation(revocation, statuses)
	if len(candidates) == 0 {
		return errors.New("no nodes could accept the proposed revocation")
	}

	if err := checkPropagationUniqueTermRevocation(revocation, candidates); err != nil {
		return err
	}

	for _, cs := range MostAdvancedStatuses(candidates) {
		if CompareRuleNumbers(cs.GetCurrentPosition().GetProposal().GetRuleNumber(), propagationIntent) == 0 {
			return nil
		}
	}
	return fmt.Errorf("most-advanced candidate nodes do not carry a proposal matching propagation_intent %v", propagationIntent)
}

// FindPropagationLeaders returns the recruited nodes eligible to serve as
// propagation leader for the given revocation. The eligible set consists of
// the most-advanced recruited nodes (by ComparePosition) whose in-WAL
// proposal matches revocation.propagation_intent.
//
// An error is returned if:
//   - revocation.propagation_intent is not set
//   - no recruited node accepted the revocation
//   - the outgoing cohort quorum is not met by the recruited nodes
//   - no most-advanced recruited node has the matching proposal
//
// Callers are responsible for choosing a leader from the returned set (e.g.
// the first element). FindPropagationLeaders does not make that selection.
func FindPropagationLeaders(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
) ([]*clustermetadatapb.ConsensusStatus, error) {
	propagationIntent := revocation.GetPropagationIntent()
	if propagationIntent == nil {
		return nil, errors.New("revocation.propagation_intent is required")
	}

	recruited := filterByRevocation(revocation, statuses)
	if len(recruited) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}

	if err := checkPropagationUniqueTermRevocation(revocation, recruited); err != nil {
		return nil, err
	}

	var leaders []*clustermetadatapb.ConsensusStatus
	for _, cs := range MostAdvancedStatuses(recruited) {
		if CompareRuleNumbers(cs.GetCurrentPosition().GetProposal().GetRuleNumber(), propagationIntent) == 0 {
			leaders = append(leaders, cs)
		}
	}
	if len(leaders) == 0 {
		return nil, fmt.Errorf("no most-advanced recruited node has a proposal matching propagation_intent %v", propagationIntent)
	}
	return leaders, nil
}

// checkPropagationUniqueTermRevocation verifies that enough members of the outgoing
// cohort are present in statuses to ensure the in-WAL proposal has a unique rule
// number. The outgoing ShardRule (cohort + durability policy) is derived from
// statuses by matching revocation.outgoing_decision — same approach as the
// transition quorum check in buildProposalCore.
func checkPropagationUniqueTermRevocation(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
) error {
	expectedOutgoing := revocation.GetOutgoingDecision()
	if expectedOutgoing == nil {
		return errors.New("revocation.outgoing_rule is required (use NewTermRevocation to construct revocations)")
	}

	// Find the full ShardRule for the outgoing decision from the status set.
	var outgoingRule *clustermetadatapb.ShardRule
	for _, cs := range statuses {
		decision := cs.GetCurrentPosition().GetDecision()
		if decision != nil && CompareRuleNumbers(decision.GetRuleNumber(), expectedOutgoing) == 0 && outgoingRule == nil {
			outgoingRule = decision
		}
	}
	if outgoingRule == nil {
		return fmt.Errorf("no status reports the expected outgoing rule %v; cannot determine cohort for quorum check", expectedOutgoing)
	}

	outgoingPolicy, err := NewPolicyFromProto(outgoingRule.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse outgoing durability policy: %w", err)
	}
	cohort := outgoingRule.GetCohortMembers()
	recruited := statusesToIDs(filterCohortStatuses(cohort, statuses))
	if err := outgoingPolicy.CheckSufficientRecruitment(cohort, recruited); err != nil {
		return fmt.Errorf("insufficient outgoing cohort recruitment for propagation: %w", err)
	}
	return nil
}
