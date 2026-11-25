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
// be replaced: it is leaving the cohort (CohortIneligible) or its postgres was
// demoted (LeaderPostgresDemoted).
func LeaderNeedsReplacement(r PoolerReport) bool {
	// A static leader leads without consensus, so there is no one to replace it.
	if r.LeadershipMode == clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC {
		return false
	}
	return CohortIneligible(r.Availability) || LeaderPostgresDemoted(r)
}

// LeaderPostgresDemoted reports whether the rule in a pooler's own WAL names it
// leader while its postgres is a standby. Only a primary can write that rule,
// and the pooler reads it before its postgres status, so this means postgres
// was demoted after promoting. Two cases don't count:
//   - a rule learned only by RPC (ReplicationPrimary): Promote records it
//     before pg_promote runs;
//   - a rule revoked by a recruit the pooler accepted: recruiting a leader
//     makes it a standby, and a coordinator is replacing it.
//
// Older poolers reported this themselves as REQUESTING_DEMOTION at their current
// term; that is honored too until no such poolers remain.
func LeaderPostgresDemoted(r PoolerReport) bool {
	walPosition := r.Consensus.GetCurrentPosition().GetPosition()
	walRule := commonconsensus.PossiblyUndecidedRule(walPosition)
	if PostgresIsStandby(r.Availability) &&
		commonconsensus.RuleNamesLeader(walRule, r.Consensus.GetId()) &&
		!commonconsensus.IsRuleRevoked(walPosition, r.Consensus.GetTermRevocation()) {
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

// PromotionPhase is how far a pooler has got in becoming leader, from its own
// report.
type PromotionPhase int

const (
	// PromotionNone: the pooler isn't becoming leader.
	PromotionNone PromotionPhase = iota
	// PromotionRunning: the pooler is becoming leader and Promote is running.
	PromotionRunning
	// PromotionAbandoned: the pooler is becoming leader but no Promote is
	// running, so none will finish.
	PromotionAbandoned
)

// LeaderPromotionPhase reports whether the pooler is becoming leader: a rule
// naming it is recorded by RPC ahead of its WAL, or proposed in its WAL but not
// yet decided.
func LeaderPromotionPhase(r PoolerReport) PromotionPhase {
	self := r.Consensus.GetId()
	walPosition := r.Consensus.GetCurrentPosition().GetPosition()
	rpcPosition := commonconsensus.ReplicationPrimaryOrNil(r.Consensus).GetPosition()
	recordedAhead := commonconsensus.RuleNamesLeader(commonconsensus.PossiblyUndecidedRule(rpcPosition), self) &&
		commonconsensus.CompareRulePosition(rpcPosition, walPosition) > 0
	proposedInWAL := !commonconsensus.IsRuleDecided(walPosition) &&
		commonconsensus.RuleNamesLeader(walPosition.GetProposal(), self)
	switch {
	case !recordedAhead && !proposedInWAL:
		return PromotionNone
	case PromoteRunning(r.Availability):
		return PromotionRunning
	default:
		return PromotionAbandoned
	}
}

// PromoteRunning reports whether a Promote is running on the pooler. Older
// poolers don't report it, but do report PROMOTING during pg_promote.
func PromoteRunning(av *clustermetadatapb.AvailabilityStatus) bool {
	return av.GetFacts().GetPromoteRunning() || PostgresIsPromoting(av)
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
