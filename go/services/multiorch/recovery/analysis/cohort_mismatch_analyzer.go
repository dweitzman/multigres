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

package analysis

import (
	"errors"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis/eligibility"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// CohortMismatchAnalyzer detects drift between the desired cohort and the
// recorded cohort on the leader: a healthy non-member should be ADDed
// (ProblemPoolerNotInCohort), and a member eligibility.DecideAll says to remove
// should be REMOVEd (ProblemCohortMember{Ineligible,Quarantined,Unhealthy,
// Lagging}). Both surface a single ReconcileCohortAction, which re-derives
// and applies the same decision — see that action's doc.
//
// TODO: this currently includes every healthy, eligible pooler in the cohort.
// In the future we'll likely want to constrain cohort size (e.g. cap at
// durability_required_count + 1) and choose the best-qualified members from
// the available poolers. That requires a fitness heuristic (LSN proximity,
// cell topology, last-known leadership history, etc.) that hasn't been
// designed yet.
type CohortMismatchAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *CohortMismatchAnalyzer) Name() types.CheckName {
	return "CohortMismatch"
}

func (a *CohortMismatchAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewReconcileCohortAction()
}

func (a *CohortMismatchAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// We only act when the shard has a reachable, ready leader to receive the
	// rule update. Bootstrap and failover paths set up the cohort separately.
	if !leaderServing(sa) {
		return nil, nil
	}

	// Detect against proposals as well as decisions to ensure we surface a problem, but
	// the action to resolve will defer taking action if a proposal is in progress.
	undecidedRule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
	tombstoned := func(id *clustermetadatapb.ID) bool {
		_, ok := sa.TombstoneIDs[topoclient.ComponentIDString(id)]
		return ok
	}
	decisions := eligibility.DecideAll(sa.Now, cohortThresholds(sa.Policy), undecidedRule, sa.Analyses, tombstoned)

	return a.buildProblems(sa, decisions), nil
}

// buildProblems picks one Decision to act on. Cohort changes are
// compare-and-swapped on the leader's outgoing rule number, so acting on
// more than one per cycle would just race a single RPC against itself —
// unless they're batched into one call (see ReconcileCohortAction, which
// batches all same-tier candidates for whichever single Decision this picks).
// Priority: an Urgent removal first (nothing lost by acting now), then an
// addition (grows the safety margin), then a non-urgent removal (softest —
// costs nothing to defer).
func (a *CohortMismatchAnalyzer) buildProblems(sa *ShardAnalysis, decisions []eligibility.Decision) []types.Problem {
	pick := func(want func(eligibility.Decision) bool) []types.Problem {
		for _, d := range decisions {
			if want(d) {
				return []types.Problem{a.problem(sa, d)}
			}
		}
		return nil
	}
	if p := pick(func(d eligibility.Decision) bool { return d.Op == eligibility.OpRemove && d.Urgent }); p != nil {
		return p
	}
	if p := pick(func(d eligibility.Decision) bool { return d.Op == eligibility.OpAdd }); p != nil {
		return p
	}
	return pick(func(d eligibility.Decision) bool { return d.Op == eligibility.OpRemove })
}

func (a *CohortMismatchAnalyzer) problem(sa *ShardAnalysis, d eligibility.Decision) types.Problem {
	code := d.Reason
	if d.Op == eligibility.OpAdd {
		code = types.ProblemPoolerNotInCohort
	}
	return types.Problem{
		Code:           code,
		CheckName:      "CohortMismatch",
		PoolerID:       d.ID,
		ShardKey:       sa.ShardKey,
		Description:    d.Description,
		Priority:       types.PriorityNormal,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewReconcileCohortAction(),
	}
}

// cohortThresholds adapts the policy's cohort-quality durations to
// eligibility.Thresholds.
func cohortThresholds(p AvailabilityPolicy) eligibility.Thresholds {
	return eligibility.Thresholds{
		UnhealthyRemoval:     p.MemberUnhealthyRemovalThreshold,
		UnhealthyReadmission: p.MemberUnhealthyReadmissionThreshold,
		LagEviction:          p.MemberLagEvictionThreshold,
		LagReadmission:       p.MemberLagReadmissionThreshold,
	}
}
