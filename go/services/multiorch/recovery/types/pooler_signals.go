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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// LeaderNeedsReplacement reports whether a primary pooler has voluntarily
// signalled that it should be replaced via a new election. It reads the
// self-reported AvailabilityStatus from the consensus status RPC.
//
// Two independent signals trigger replacement:
//
//   - cohort_eligibility_signal == INELIGIBLE: the pooler is unwilling to
//     remain in the cohort (e.g. graceful shutdown advertises this before
//     stopping postgres).
//   - continue_leadership_signal == INELIGIBLE: the pooler is named leader by
//     the rule but isn't fit to continue (e.g. postgres isn't out of
//     recovery). Republished fresh every snapshot rather than latched, so
//     unlike a one-time event it can't be a leftover from a previous election
//     cycle — no term correlation needed.
//
// TODO: once the coordinator synthesizes AvailabilityStatus into
// PoolerHealthState directly (see clustermetadata.proto TODO), read from
// PoolerHealthState.AvailabilityStatus instead of ConsensusStatus so that
// coordinator-synthesized signals (e.g. TEMPORARILY_UNAVAILABLE for unreachable
// nodes) are handled here too.
func LeaderNeedsReplacement(p *multiorchdatapb.PoolerHealthState) bool {
	av := p.GetAvailabilityStatus()
	return PoolerIsCohortIneligible(av) || av.GetContinueLeadershipSignal() == clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE
}

// PoolerIsCohortIneligible reports whether a pooler has self-reported that it
// is unwilling to serve as a consensus cohort member. UNKNOWN (the default for
// poolers that don't publish the field, e.g. older versions) and ELIGIBLE both
// return false. Eligibility is a current preference and not term-gated;
// staleness comes from the freshness of the surrounding health snapshot.
func PoolerIsCohortIneligible(av *clustermetadatapb.AvailabilityStatus) bool {
	return av.GetCohortEligibilitySignal() == clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE
}

// PoolerPrefersNotToBecomeLeader reports whether a pooler has self-reported
// that it would rather not be elected leader in a future term. Advisory only
// (see AvailabilityStatus.become_leader_eligibility_signal) — callers use this
// as a tiebreak preference among candidates, never a hard exclusion.
func PoolerPrefersNotToBecomeLeader(av *clustermetadatapb.AvailabilityStatus) bool {
	return av.GetBecomeLeaderEligibilitySignal() == clustermetadatapb.EligibilitySignal_ELIGIBILITY_SIGNAL_INELIGIBLE
}
