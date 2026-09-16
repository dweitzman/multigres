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

// RuleConfidence classifies whether a rule from ConfirmLatestRule could have
// been superseded by an undetected newer decision.
type RuleConfidence int

const (
	// RuleConfidenceUnconfirmed: the unreached cohort members could, on
	// their own, satisfy the durability policy — a newer decision might
	// exist among exactly them.
	RuleConfidenceUnconfirmed RuleConfidence = iota
	// RuleConfidenceConfirmed: the unreached members alone can't satisfy
	// the policy, so no undetected newer decision could exist.
	RuleConfidenceConfirmed
)

// ConfirmLatestRule returns the most advanced known RulePosition (decision
// plus any pending proposal, same contract as HighestKnownRule, which this
// uses internally), plus a RuleConfidence verdict on whether it can be
// trusted as final rather than a best guess.
//
// Use this only where the result is the terminal answer for an action with
// no downstream quorum re-check (e.g. deciding whether a disk still backs an
// active cohort). Callers that already run their own quorum check before
// acting (recruitment, recovery actions) should use HighestKnownRule
// directly — the confidence computation would be redundant there.
//
// err is non-nil only when no status carries a rule, or the decision's
// durability policy proto is invalid; both leave rule nil and confidence
// meaningless.
//
// reachedIDs must come from the caller (e.g. the topology-known ID of the
// RPC target), not each status's self-reported ConsensusStatus.Id — a
// pooler with a missing or spoofed self-ID must not cover for a pooler that
// was never actually reached.
func ConfirmLatestRule(
	statuses []*clustermetadatapb.ConsensusStatus,
	reachedIDs []*clustermetadatapb.ID,
) (rule *clustermetadatapb.RulePosition, confidence RuleConfidence, err error) {
	best := HighestKnownRule(statuses)
	if best == nil {
		return nil, RuleConfidenceUnconfirmed, errors.New("ConfirmLatestRule: no rule observed in any status")
	}

	decision := best.GetDecision()
	policy, err := NewPolicyFromProto(decision.GetDurabilityPolicy())
	if err != nil {
		return nil, RuleConfidenceUnconfirmed, fmt.Errorf("ConfirmLatestRule: invalid durability policy on observed rule: %w", err)
	}

	confidence = RuleConfidenceConfirmed
	if rogueQuorumCheck(policy, decision.GetCohortMembers(), reachedIDs) != nil {
		confidence = RuleConfidenceUnconfirmed
	}
	return best, confidence, nil
}
