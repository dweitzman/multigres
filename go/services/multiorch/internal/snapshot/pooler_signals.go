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

package snapshot

import (
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CohortIneligible reports whether the pooler has self-reported that it is unwilling
// to serve as a consensus cohort member. UNKNOWN (the default for poolers that don't
// publish the field, e.g. older versions) and ELIGIBLE both return false. Eligibility
// is a current preference and not term-gated; staleness comes from the freshness of
// the surrounding health snapshot.
func (h *PoolerHealthState) CohortIneligible() bool {
	signal := h.GetAvailabilityStatus().GetCohortEligibilityStatus().GetSignal()
	return signal == clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE
}

// ClaimsLeadership reports whether the pooler's own consensus status names itself
// leader of the highest rule it knows, including a proposal not yet decided. It is
// the pooler's claim, not a verdict: the claim can be stale, revoked or superseded
// (see commonconsensus.IsActiveLeader for write authority). A pooler that claims
// leadership but is not the shard's intended leader is a stale leader.
func (h *PoolerHealthState) ClaimsLeadership() bool {
	return commonconsensus.SelfConsensusRole(h.GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader
}

// RequestsLeaderReplacement reports whether a leader pooler has voluntarily
// signalled that it should be replaced via a new election. It reads the
// self-reported AvailabilityStatus.
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
//   - LeadershipStatus.Signal == REQUESTING_DEMOTION with leader_term ==
//     current primary term: emitted by Recruit's primary-demote path after a
//     postgres crash so the coordinator can correlate the request with the
//     specific term and avoid acting on a leftover signal from a previous
//     election cycle that was never cleared (e.g. after a process restart).
//
// TODO: once the coordinator synthesizes AvailabilityStatus into the health state
// directly (see clustermetadata.proto TODO), read from it instead of ConsensusStatus
// so that coordinator-synthesized signals (e.g. TEMPORARILY_UNAVAILABLE for
// unreachable nodes) are handled here too.
func (h *PoolerHealthState) RequestsLeaderReplacement() bool {
	av := h.GetAvailabilityStatus()
	if h.CohortIneligible() {
		return true
	}
	leadershipStatus := av.GetLeadershipStatus()
	if leadershipStatus == nil {
		return false
	}
	if leadershipStatus.Signal != clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION {
		return false
	}
	// Verify the signal is for the current primary term, not a stale one. Uses the
	// possibly-undecided rule (not just the decision) so a signal about a
	// self-promotion that crashed before its own write was confirmed decided is
	// still recognized.
	position := h.GetConsensusStatus().GetCurrentPosition().GetPosition()
	primaryTerm := commonconsensus.PossiblyUndecidedRule(position).GetRuleNumber().GetCoordinatorTerm()
	return leadershipStatus.LeaderTerm != 0 && leadershipStatus.LeaderTerm == primaryTerm
}

// FreshAndInitialized reports whether the pooler's health snapshot is within
// freshness and initialized: the bar for treating its report as usable evidence at
// all (its consensus state, replication status, identity, etc.), independent of
// whether it could actually be recruited into a new term. Use this for "do I have
// any trustworthy signal to reason from"; use Recruitable for "could this member
// actually win a Recruit round". A pooler's report can be fresh and initialized
// while it is not recruitable (e.g. draining), and the two questions must not be
// conflated. This is purely about data trustworthiness, a third axis distinct from
// both network connectivity (StreamConnected) and recruitment eligibility.
func (h *PoolerHealthState) FreshAndInitialized(now time.Time, freshness time.Duration) bool {
	return h.ObservedWithin(now, freshness) && h.GetStatus().GetIsInitialized()
}

// Recruitable reports whether the pooler is, as of now, usable as a target for
// recruitment or cohort admission. Each check here is also enforced synchronously
// server-side (Recruit/ValidateRevocation refuses the same cases with
// FAILED_PRECONDITION), so this doesn't itself guard correctness; it exists so
// callers' feasibility judgments (is a recruitment quorum reachable, is this
// pooler admissible) agree with what an actual Recruit attempt would do, instead
// of counting a member that would just be refused.
func (h *PoolerHealthState) Recruitable(now time.Time, freshness time.Duration) bool {
	if !h.FreshAndInitialized(now, freshness) {
		return false
	}
	// Draining (graceful shutdown or an admin-stopped WAL receiver).
	if h.CohortIneligible() {
		return false
	}
	// No cached position — e.g. a flapping pooler whose consensus status hasn't
	// completed a read yet. Without this, a failover could look feasible forever
	// while every real Recruit attempt is refused, so collective backoff never
	// engages (nothing ever persists).
	if h.GetConsensusStatus().GetCurrentPosition() == nil {
		return false
	}
	// Hasn't caught back up from a pg_rewind yet.
	return h.GetConsensusStatus().GetRecruitBlockedUntil() == nil
}
