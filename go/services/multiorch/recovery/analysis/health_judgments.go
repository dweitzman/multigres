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
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// The helpers below derive analyzer-relevant judgments from a pooler's raw
// health in the snapshot, so the snapshot stays the single source of truth and
// there is no parallel cached copy.

// freshAndInitialized reports whether p's health snapshot is within freshness
// and initialized — the bar for treating p's report as usable evidence at all
// (its consensus state, replication status, identity, etc.), independent of
// whether p could actually be recruited into a new term. Use this for "do I
// have any trustworthy signal to reason from" judgments; use recruitable for
// "could this member actually win a Recruit round" judgments — a pooler's
// report can be fresh and initialized while it is not recruitable (e.g.
// draining), and the two questions must not be conflated. Named for exactly
// what it checks, not "reachable": this is purely about data trustworthiness,
// a third axis distinct from both network connectivity (StreamConnected) and
// recruitment eligibility (recruitable).
func freshAndInitialized(p *snapshot.PoolerHealthState, now time.Time, freshness time.Duration) bool {
	hs, ok := snapshot.HealthWithin(p, now, freshness)
	return ok && hs.GetStatus().GetIsInitialized()
}

// recruitable reports whether p is, as of now, usable as a target for
// recruitment or cohort admission. Each check here is also enforced
// synchronously server-side (Recruit/ValidateRevocation refuses the same
// cases with FAILED_PRECONDITION), so this doesn't itself guard correctness —
// it exists so callers' feasibility judgments (is a recruitment quorum
// reachable, is this pooler admissible) agree with what an actual Recruit
// attempt would do, instead of counting a member that would just be refused.
func recruitable(p *snapshot.PoolerHealthState, now time.Time, freshness time.Duration) bool {
	if !freshAndInitialized(p, now, freshness) {
		return false
	}
	hs, _ := snapshot.HealthWithin(p, now, freshness)
	// Draining (graceful shutdown or an admin-stopped WAL receiver).
	if types.PoolerIsCohortIneligible(hs.GetAvailabilityStatus()) {
		return false
	}
	// No cached position — e.g. a flapping pooler whose consensus status
	// hasn't completed a read yet. Without this, a failover could look
	// feasible forever while every real Recruit attempt is refused, so
	// collective backoff never engages (nothing ever persists).
	if hs.GetConsensusStatus().GetCurrentPosition() == nil {
		return false
	}
	// Hasn't caught back up from a pg_rewind yet.
	return hs.GetConsensusStatus().GetRecruitBlockedUntil() == nil
}

// compareLeaderTimeline compares two leader snapshots by rule position. LSN is
// intentionally excluded from the comparison (CompareRulePosition already
// stops at decision-then-proposal): for leaders, the coordinator term must be
// unique per promotion, so equal terms indicate a consensus bug rather than a
// resolvable tie.
func compareLeaderTimeline(a, b *snapshot.PoolerHealthState) int {
	return commonconsensus.CompareRulePosition(
		a.GetConsensusStatus().GetCurrentPosition().GetPosition(),
		b.GetConsensusStatus().GetCurrentPosition().GetPosition(),
	)
}

// analyzeAllPoolers runs fn against each pooler in sa, collecting all problems.
// Both the shard snapshot and the pooler's snapshot are passed so callbacks can
// access shard-level context alongside pooler-specific state.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *snapshot.Shard, fn func(*snapshot.Shard, *snapshot.PoolerHealthState) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Poolers {
		p, err := fn(sa, poolerAnalysis)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if p != nil {
			problems = append(problems, *p)
		}
	}
	return problems, firstErr
}
