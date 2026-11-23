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

// TODO: stateless helpers for interpreting consensus and availability signals
// (like LeaderNeedsReplacement) live here to avoid an import cycle between the
// analysis and actions packages. Once these utilities are needed more broadly they
// should move to go/common/consensus or a similar shared package.

import (
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// LeaderNeedsReplacement reports whether a primary pooler has voluntarily
// signalled that it should be replaced via a new election. It reads the
// self-reported AvailabilityStatus from the consensus status RPC.
//
// Two independent signals trigger replacement:
//
//   - CohortEligibilityStatus.Signal == INELIGIBLE: the pooler is unwilling to
//     remain in the cohort (e.g. graceful shutdown advertises this before
//     stopping postgres). Not term-gated — eligibility is a current preference
//     and staleness comes from the freshness of the surrounding health
//     snapshot; a node that doesn't want to be in the cohort certainly should
//     not continue as leader.
//
//   - LeadershipStatus.Signal == NOT_SERVING with leader_term ==
//     current primary term: the pooler is the named leader but isn't serving
//     (its postgres is a standby). The pooler derives it from current state;
//     the term check keeps a snapshot from being applied to a newer rule.
//
// TODO: once the coordinator synthesizes AvailabilityStatus into
// PoolerHealthState directly (see clustermetadata.proto TODO), read from
// PoolerHealthState.AvailabilityStatus instead of ConsensusStatus so that
// coordinator-synthesized signals (e.g. TEMPORARILY_UNAVAILABLE for unreachable
// nodes) are handled here too.
func LeaderNeedsReplacement(p *multiorchdatapb.PoolerHealthState) bool {
	av := p.GetAvailabilityStatus()
	if PoolerIsCohortIneligible(av) {
		return true
	}
	leadershipStatus := av.GetLeadershipStatus()
	if leadershipStatus == nil {
		return false
	}
	if leadershipStatus.Signal != clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_NOT_SERVING {
		return false
	}
	// Verify the signal is for the current primary term, not a stale one.
	// Uses the possibly-undecided rule (not just the decision) so a signal
	// about a self-promotion that crashed before its own write was confirmed
	// decided is still recognized.
	position := p.GetConsensusStatus().GetCurrentPosition().GetPosition()
	primaryTerm := commonconsensus.PossiblyUndecidedRule(position).GetRuleNumber().GetCoordinatorTerm()
	return leadershipStatus.LeaderTerm != 0 && leadershipStatus.LeaderTerm == primaryTerm
}

// PoolerIsCohortIneligible reports whether a pooler has self-reported that it
// is unwilling to serve as a consensus cohort member. UNKNOWN (the default for
// poolers that don't publish the field, e.g. older versions) and ELIGIBLE both
// return false. Eligibility is a current preference and not term-gated;
// staleness comes from the freshness of the surrounding health snapshot.
func PoolerIsCohortIneligible(av *clustermetadatapb.AvailabilityStatus) bool {
	signal := av.GetCohortEligibilityStatus().GetSignal()
	return signal == clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE
}
