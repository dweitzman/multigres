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
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// These helpers are the one place multiorch interprets what a pooler reports
// (its postgres status, consensus position and availability status) into plain
// answers. Analyzers ask these questions rather than reading raw signals.

// LeaderNeedsReplacement reports whether a leader pooler's own report says it
// should be replaced: it is leaving the cohort (CohortIneligible) or it can't act
// as the leader it believes it is (CannotActAsLeader).
func LeaderNeedsReplacement(p *multiorchdatapb.PoolerHealthState) bool {
	return PoolerIsCohortIneligible(p.GetAvailabilityStatus()) || CannotActAsLeader(p)
}

// CannotActAsLeader reports whether a pooler believes it is the leader but its
// postgres can't act as one: the highest rule it knows names it leader while its
// postgres is a standby. Both facts come from the same report, so a stale report
// can't convict a leader that has since promoted. Promote runs pg_promote before
// writing the rule that names this pooler, so a promotion never looks like this.
//
// Older poolers reported this themselves as REQUESTING_DEMOTION at their current
// term; that is honored too until no such poolers remain.
func CannotActAsLeader(p *multiorchdatapb.PoolerHealthState) bool {
	cs := p.GetConsensusStatus()
	if PostgresIsStandby(p) && commonconsensus.SelfConsensusRole(cs) == commonconsensus.ConsensusRoleLeader {
		return true
	}
	ls := p.GetAvailabilityStatus().GetLeadershipStatus()
	if ls.GetSignal() != clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION {
		return false
	}
	position := cs.GetCurrentPosition().GetPosition()
	term := commonconsensus.PossiblyUndecidedRule(position).GetRuleNumber().GetCoordinatorTerm()
	return ls.GetLeaderTerm() != 0 && ls.GetLeaderTerm() == term
}

// PostgresIsStandby reports whether the pooler's postgres is running as a
// standby (in recovery and accepting connections).
func PostgresIsStandby(p *multiorchdatapb.PoolerHealthState) bool {
	return p.GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
}

// PostgresIsPromoting reports whether the pooler is promoting its postgres.
func PostgresIsPromoting(p *multiorchdatapb.PoolerHealthState) bool {
	return p.GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
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
