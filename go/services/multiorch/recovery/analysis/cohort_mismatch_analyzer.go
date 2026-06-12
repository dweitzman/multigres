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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

const (
	// walReceiverStatusStreaming is the pg_stat_wal_receiver.status value that
	// indicates a standby is actively connected to and streaming from its primary.
	walReceiverStatusStreaming = "streaming"

	// cohortCandidateMaxReplayLag bounds how far behind a standby may be replayed
	// and still be eligible to join the synchronous cohort. Beyond this, adding it
	// would stall writes waiting for its acknowledgement.
	cohortCandidateMaxReplayLag = 10 * time.Second
)

// CohortMismatchAnalyzer detects drift between the desired cohort and the
// recorded cohort on the leader.
//
// Two flavors of drift are reported:
//
//   - ProblemPoolerNotInCohort: a healthy, replicating, eligible pooler exists
//     in the shard but is absent from the leader's recorded cohort.
//   - ProblemCohortMemberIneligible: a current cohort member has self-reported
//     INELIGIBLE via AvailabilityStatus.CohortEligibilityStatus and should be
//     removed.
//
// Both surface a single ReconcileCohortAction; the action interprets the
// problem code and applies the appropriate ADD/REMOVE.
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

func (a *CohortMismatchAnalyzer) ProblemCode() types.ProblemCode {
	// This analyzer can produce two problem codes; ProblemCode() returns the
	// primary one. The recovery loop uses this for routing/logging — the
	// per-problem code on each emitted Problem is what matters at execution.
	return types.ProblemPoolerNotInCohort
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
	if sa.HighestTermDiscoveredLeaderID == nil || !sa.LeaderReachable || !sa.LeaderPostgresReady {
		return nil, nil
	}

	// Build a set of current cohort member ID strings for O(1) lookup.
	cohortIDs := make(map[string]struct{}, len(sa.LeaderStandbyIDs))
	for _, id := range sa.LeaderStandbyIDs {
		cohortIDs[topoclient.MultiPoolerIDString(id)] = struct{}{}
	}

	// Count current cohort members that are fit, so removals can be guarded by
	// durability: we never drop a member if doing so would leave fewer fit
	// members than the durability policy requires.
	fitMembers := 0
	for _, pa := range sa.Analyses {
		if _, inCohort := cohortIDs[topoclient.MultiPoolerIDString(pa.PoolerID)]; inCohort && fitForCohort(pa) {
			fitMembers++
		}
	}
	requiredCount := int(sa.BootstrapDurabilityPolicy.GetRequiredCount())

	var problems []types.Problem
	for _, pa := range sa.Analyses {
		// Removal candidates: current cohort members that are no longer fit
		// (unreachable, not streaming, lagging, paused, or self-reported
		// INELIGIBLE).
		if _, inCohort := cohortIDs[topoclient.MultiPoolerIDString(pa.PoolerID)]; inCohort {
			if fitForCohort(pa) {
				continue
			}
			// Quorum guard: an unfit member is not counted in fitMembers, so the
			// fit members that remain after removing it is exactly fitMembers.
			// Only remove if those still satisfy the durability requirement —
			// keeping a degraded member is safer than dropping below the floor.
			//
			// TODO(durability accounting): confirm whether the leader counts
			// toward RequiredCount and whether sync replication is ANY-N or
			// FIRST-N. This guard is deliberately conservative (errs toward
			// keeping members) until that is settled.
			if fitMembers < requiredCount {
				continue
			}
			problems = append(problems, types.Problem{
				Code:           types.ProblemCohortMemberIneligible,
				CheckName:      "CohortMismatch",
				PoolerID:       pa.PoolerID,
				ShardKey:       pa.ShardKey,
				Description:    fmt.Sprintf("Cohort member %s is no longer fit for the cohort", pa.PoolerID.Name),
				Priority:       types.PriorityNormal,
				Scope:          types.ScopePooler,
				DetectedAt:     time.Now(),
				RecoveryAction: a.factory.NewReconcileCohortAction(),
			})
			continue
		}

		// Addition candidates: replicas not currently in the cohort that are fit.
		if !fitForCohort(pa) {
			continue
		}
		problems = append(problems, types.Problem{
			Code:           types.ProblemPoolerNotInCohort,
			CheckName:      "CohortMismatch",
			PoolerID:       pa.PoolerID,
			ShardKey:       pa.ShardKey,
			Description:    fmt.Sprintf("Pooler %s is replicating and eligible but not in the cohort", pa.PoolerID.Name),
			Priority:       types.PriorityNormal,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewReconcileCohortAction(),
		})
	}
	return problems, nil
}

// fitForCohort reports whether a pooler is healthy enough to be (or remain) a
// synchronous cohort member: it has a recent successful health check, is
// initialized, is actively streaming from the primary within the replay-lag
// bound and not paused, and is not self-reporting INELIGIBLE. The same predicate
// drives both additions (a fit non-member should join) and removals (a member
// that is no longer fit should leave, subject to the durability quorum guard).
//
// A stalled, lagging, paused, or unreachable standby can't reliably acknowledge
// writes, so counting it toward durability would hurt durability/latency.
//
// TODO: long-term, most of these checks should fold into the pooler's
// self-reported cohort eligibility signal. The pooler is in the best position to
// know whether it can durably serve as a cohort member — whether it has a working
// backup, whether it's drained, and — the key fitness signal — whether its
// replication lag is low enough to acknowledge writes without hurting latency.
// That lag indicator should live on AvailabilityStatus so the analyzer can simply
// trust CohortEligibilityStatus rather than reconstruct the judgment here.
func fitForCohort(pa *PoolerAnalysis) bool {
	if pa == nil || !pa.LastCheckValid || !pa.IsInitialized {
		return false
	}
	rs := pa.ReplicationStatus
	if rs.GetIsWalReplayPaused() || rs.GetWalReceiverStatus() != walReceiverStatusStreaming {
		return false
	}
	if lag := rs.GetLag(); lag != nil && lag.AsDuration() > cohortCandidateMaxReplayLag {
		return false
	}
	if types.PoolerIsCohortIneligible(pa.AvailabilityStatus) {
		return false
	}
	return true
}
