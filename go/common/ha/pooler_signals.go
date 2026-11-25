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

package ha

import (
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// This file is the interpretation layer for pooler signals: the one place that
// turns what a pooler reports into plain answers for HA decisions. Callers ask
// these questions rather than reading raw signals; see
// docs/ha/pooler-signals.md.

// PoolerReport is what HA decisions know about one pooler: its own report plus
// how topology configures it.
type PoolerReport struct {
	Consensus      *clustermetadatapb.ConsensusStatus
	Availability   *clustermetadatapb.AvailabilityStatus
	LeadershipMode clustermetadatapb.LeadershipMode
}

// LeaderNeedsReplacement reports whether a leader's own report says it should
// be replaced: it is leaving the cohort (CohortIneligible) or it can't act as
// the leader it believes it is (CannotActAsLeader).
func LeaderNeedsReplacement(r PoolerReport) bool {
	return CohortIneligible(r.Availability) || CannotActAsLeader(r)
}

// CannotActAsLeader reports whether a pooler believes it is the leader but its
// postgres can't act as one: the highest rule it knows names it leader while its
// postgres is a standby. The pooler reads its rule before its postgres status,
// and the rule naming it is written to its own postgres, which refuses the write
// until pg_promote has taken it out of recovery. So this means postgres was
// demoted, never that a promotion is under way.
//
// Older poolers reported this themselves as REQUESTING_DEMOTION at their current
// term; that is honored too until no such poolers remain.
func CannotActAsLeader(r PoolerReport) bool {
	// A static leader leads without consensus, so there is no one to replace it.
	if r.LeadershipMode == clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC {
		return false
	}
	if PostgresIsStandby(r.Availability) && commonconsensus.SelfConsensusRole(r.Consensus) == commonconsensus.ConsensusRoleLeader {
		return true
	}
	ls := r.Availability.GetLeadershipStatus()
	if ls.GetSignal() != clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION {
		return false
	}
	position := r.Consensus.GetCurrentPosition().GetPosition()
	term := commonconsensus.PossiblyUndecidedRule(position).GetRuleNumber().GetCoordinatorTerm()
	return ls.GetLeaderTerm() != 0 && ls.GetLeaderTerm() == term
}

// PostgresIsStandby reports whether the pooler's postgres is running as a
// standby (in recovery and accepting connections).
func PostgresIsStandby(av *clustermetadatapb.AvailabilityStatus) bool {
	return av.GetFacts().GetPostgresStatus() == clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
}

// PostgresIsPromoting reports whether the pooler is promoting its postgres.
func PostgresIsPromoting(av *clustermetadatapb.AvailabilityStatus) bool {
	return av.GetFacts().GetPostgresStatus() == clustermetadatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
}

// CohortIneligible reports whether a pooler has said it is unwilling to serve
// as a cohort member. UNKNOWN (including from poolers that don't publish the
// field) and ELIGIBLE both return false. Eligibility is a current intent and
// not term-gated; staleness comes from the freshness of the surrounding report.
func CohortIneligible(av *clustermetadatapb.AvailabilityStatus) bool {
	signal := av.GetCohortEligibilityStatus().GetSignal()
	return signal == clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE
}

// PrefersNotToLead reports whether a pooler would rather another candidate lead.
// It only orders otherwise-equal candidates; see LeadershipPreference.
func PrefersNotToLead(av *clustermetadatapb.AvailabilityStatus) bool {
	return av.GetIntent().GetLeadershipPreference() == clustermetadatapb.LeadershipPreference_LEADERSHIP_PREFERENCE_PREFER_NOT
}
