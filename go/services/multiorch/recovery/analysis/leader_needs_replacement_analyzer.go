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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// LeaderNeedsReplacementAnalyzer decides, once per recovery cycle, whether a
// shard's leader must be replaced. It asks one question: is the shard provably
// making durable write progress under its current leader? The answer comes from
// four checks, in order (see judgeLeader):
//
//  1. Progress is impossible → replace. Durable consensus facts that won't
//     change on their own: the leader resigned or shut down, it does not back
//     the current rule, or enough of its cohort revoked that rule that it can
//     no longer reach a quorum.
//  2. Progress is halted → replace. Current observations that could recover on
//     their own, so each has an anti-flap allowance: the leader's postgres is
//     down or still in recovery, or enough of its cohort has stopped streaming
//     from it. Checked before the watermark because they are fresher: a
//     watermark may be QuorumCommitStaleAfter old.
//  3. Progress is proven → keep. A quorum acknowledged a write within
//     QuorumCommitStaleAfter (the heartbeat's quorum-commit watermark).
//  4. Otherwise → replace: writes are not provably committing. An undecided
//     promotion reports LeaderPromotionIncomplete, an established leader
//     LeaderProgressUnproven. The problem is always reported, since writes are
//     unavailable either way, but the failover is deferred (Problem.NotBefore)
//     while progress may be imminent: for QuorumCommitStaleAfter after the
//     current rule was created (its leader hasn't had time to commit a
//     watermark), and for up to MaxPromotionTime while a promotion is
//     propagating (its candidate is mid pg_promote(), or followers are
//     receiving its WAL).
//
// While a promotion is in flight, the halted checks that a promotion itself
// causes (a candidate still in recovery, followers reconnecting) don't apply.
//
// The evidence behind each check lives in leader_evidence.go (the leader's own
// report), cohort_support.go (its followers' reports) and leader_progress.go
// (the watermark and WAL progress). This file only decides what that evidence
// means.
//
// A replacement verdict is then gated on whether a failover could succeed
// (non-actionable outcomes are alert-only):
//   - replace + feasible   → the cause code (actionable → AppointLeader).
//   - replace + infeasible → ShardStuck, or NoHealthyCohortMembers when orch is blind.
//   - keep                 → no problem, or ShardAtRisk if losing the leader would strand the shard.
//
// "Feasible" is CheckSufficientRecruitment: a strict majority of the outgoing
// cohort reachable (unique rule number) with the remainder unable to satisfy the
// policy (revocation). For ShardAtRisk we run it excluding the current leader — the
// question is whether we could recover if the leader were lost.
//
// TODO(pooler-reported health): this analyzer reasons about postgres running/ready
// directly (leaderPostgresReady/Running). Directionally it should trust a pooler's
// self-reported fitness — the pooler knows it is e.g. mid-restart and still fit —
// with backstops for when a pooler is wrong, rather than second-guessing postgres
// state here.
type LeaderNeedsReplacementAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *LeaderNeedsReplacementAnalyzer) Name() types.CheckName {
	return "LeaderNeedsReplacement"
}

func (a *LeaderNeedsReplacementAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *LeaderNeedsReplacementAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	rule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
	leaderID := rule.GetLeaderId()
	cohort := rule.GetCohortMembers()

	// No rule at all yet, or a rule naming neither a leader nor a cohort — the
	// initial, unbootstrapped state. ShardNeedsInitialization owns that, so do
	// nothing here; there's no established policy to read yet either.
	if leaderID == nil && len(cohort) == 0 {
		return nil, nil
	}

	policy, err := commonconsensus.NewPolicyFromProto(rule.GetDurabilityPolicy())
	if err != nil {
		return nil, mterrors.Wrap(err, "leader-needs-replacement: durability policy unavailable")
	}

	// A non-empty cohort with no designated leader needs one recruited.
	if leaderID == nil {
		return a.emitFailover(sa, nil, policy, cohort, replaceLeader(types.ProblemLeaderUnspecified,
			fmt.Sprintf("Shard %s has cohort members but no designated leader", sa.ShardKey))), nil
	}

	verdict, replace := a.judgeLeader(sa, leaderID, cohort, policy)
	if !replace {
		return a.atRiskProblemIfDegraded(sa, policy, cohort, leaderID), nil
	}
	return a.emitFailover(sa, leaderID, policy, cohort, verdict), nil
}

// leaderVerdict is why a leader must be replaced, and the earliest time to act.
type leaderVerdict struct {
	cause       types.ProblemCode
	description string
	notBefore   time.Time
}

func replaceLeader(cause types.ProblemCode, description string) leaderVerdict {
	return leaderVerdict{cause: cause, description: description}
}

// judgeLeader applies the checks documented on LeaderNeedsReplacementAnalyzer.
// It returns replace=false when the leader provably makes progress.
func (a *LeaderNeedsReplacementAnalyzer) judgeLeader(
	sa *ShardAnalysis,
	leaderID *clustermetadatapb.ID,
	cohort []*clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
) (verdict leaderVerdict, replace bool) {
	inFlight := promotionInFlight(sa, leaderID)

	revoked, disconnected := a.classifyFollowers(sa, cohort, leaderID)

	// 1. Progress is impossible.
	if verdict, ok := a.progressImpossible(sa, leaderID, cohort, policy, revoked); ok {
		return verdict, true
	}

	// 2. Progress is halted.
	if verdict, ok := a.progressHalted(sa, cohort, policy, inFlight, revoked, disconnected); ok {
		return verdict, true
	}

	// 3. Progress is proven.
	if quorumCommitFresh(sa) {
		return leaderVerdict{}, false
	}

	// 4. Otherwise, progress is unproven. Defer while it may still be imminent:
	// a new rule's leader needs QuorumCommitStaleAfter to commit a watermark,
	// and a propagating promotion gets up to MaxPromotionTime.
	created := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition).GetCreationTime().AsTime()
	if inFlight {
		verdict = replaceLeader(types.ProblemLeaderPromotionIncomplete,
			fmt.Sprintf("Shard %s is promoting a new leader that has not yet committed a quorum write", sa.ShardKey))
		verdict.notBefore = created.Add(sa.Policy.QuorumCommitStaleAfter)
		if a.promotionPropagating(sa, leaderID, cohort, policy) {
			verdict.notBefore = created.Add(sa.Policy.MaxPromotionTime)
		}
		return verdict, true
	}
	verdict = replaceLeader(types.ProblemLeaderProgressUnproven,
		fmt.Sprintf("Shard %s has no quorum-commit watermark newer than %s", sa.ShardKey, sa.Policy.QuorumCommitStaleAfter))
	verdict.notBefore = created.Add(sa.Policy.QuorumCommitStaleAfter)
	return verdict, true
}

// progressImpossible reports the first durable consensus fact proving the
// leader cannot make progress, if any: its own intent first, then its own
// bookkeeping, then its followers'.
func (a *LeaderNeedsReplacementAnalyzer) progressImpossible(
	sa *ShardAnalysis,
	leaderID *clustermetadatapb.ID,
	cohort []*clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
	revoked []*clustermetadatapb.ID,
) (leaderVerdict, bool) {
	// The leader asked to step down. leaderHasResigned is the fast path (its
	// REQUESTING_DEMOTION/INELIGIBLE health broadcast); the SHUTDOWN tombstone is
	// the durable fallback for when that ephemeral broadcast is lost.
	// TODO: this still waits the shared failover grace; it could skip it (intent
	// doesn't flap; multi-orch safety is the Recruit CAS), cutting the write outage.
	if leaderHasResigned(sa) || leaderShutdownTombstoned(sa, leaderID) {
		return replaceLeader(types.ProblemLeaderResigned,
			fmt.Sprintf("Leader for shard %s is stepping down", sa.ShardKey)), true
	}

	// The leader accepted a revocation of the current rule (a recruit that stalled
	// before establishing a successor). Revocations are durable, so even a stale
	// report of one is conclusive.
	if leaderRevokedCurrentRule(sa) {
		return replaceLeader(types.ProblemLeaderNotSelfConfirmed,
			fmt.Sprintf("Leader for shard %s has revoked its own rule", sa.ShardKey)), true
	}

	// The leader's fresh report shows it neither leads under the current rule nor
	// has accepted the promotion to it — e.g. its Promote was lost while the
	// followers' SetPrimary landed. Writes only flow through the leader, so this
	// disqualifies it regardless of follower support. ruleWithinGrace allows for
	// the leader's report trailing its followers' (Promote and SetPrimary are
	// sent concurrently).
	currentRule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
	if leaderObservedLive(sa) && !leaderBacksCurrentRule(sa, leaderID) && !ruleWithinGrace(sa, currentRule) {
		return replaceLeader(types.ProblemLeaderNotSelfConfirmed,
			fmt.Sprintf("Leader for shard %s does not confirm its own role in the current rule", sa.ShardKey)), true
	}

	// Enough followers revoked the rule that the rest cannot satisfy the
	// durability policy, so no quorum can commit under it.
	if revocationSufficient(policy, cohort, revoked) {
		return replaceLeader(types.ProblemLeaderLacksCohortSupport,
			fmt.Sprintf("Leader for shard %s has lost its rule to revocations by its cohort", sa.ShardKey)), true
	}
	return leaderVerdict{}, false
}

// progressHalted reports the first current observation showing the leader is
// not making progress right now, if any. Each could recover on its own, so
// each is judged with an allowance for transient blips.
func (a *LeaderNeedsReplacementAnalyzer) progressHalted(
	sa *ShardAnalysis,
	cohort []*clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
	inFlight bool,
	revoked, disconnected []*clustermetadatapb.ID,
) (leaderVerdict, bool) {
	// Enough followers are pointed at the leader yet not streaming (past
	// ConnectReplicasToNewLeaderGrace) that the rest cannot form a quorum. Not
	// during a promotion: the candidate's pg_promote() switches timeline,
	// briefly disconnecting every cascading follower at once. A promotion whose
	// followers never reconnect shows no WAL progress, so it isn't deferred
	// (see promotionPropagating).
	if !inFlight && revocationSufficient(policy, cohort, append(revoked, disconnected...)) {
		return replaceLeader(types.ProblemLeaderUnreachableByCohort,
			fmt.Sprintf("Leader for shard %s is not reachable by a durability-sufficient set of its cohort", sa.ShardKey)), true
	}

	// The leader's postgres cannot serve writes. Judged only from a fresh report.
	if !leaderObservedLive(sa) {
		return leaderVerdict{}, false
	}
	// A STANDBY answers pg_isready, so without this it would pass the readiness
	// check below forever. Not during a promotion: the candidate is still a
	// standby until its pg_promote() runs.
	if leaderInRecovery(sa) && !inFlight {
		return replaceLeader(types.ProblemLeaderUnhealthy,
			fmt.Sprintf("Leader for shard %s is reachable but its postgres is in recovery (not a primary)", sa.ShardKey)), true
	}
	// A promotion in flight is mid pg_promote(), so briefly not ready is expected.
	if !leaderPostgresReady(sa) && !(inFlight && leaderMidPromote(sa)) && !a.leaderRecentlyReady(sa) {
		return replaceLeader(types.ProblemLeaderUnhealthy,
			fmt.Sprintf("Leader for shard %s is reachable but its postgres is unhealthy", sa.ShardKey)), true
	}
	return leaderVerdict{}, false
}

// leaderRecentlyReady is an anti-flap allowance: a leader whose postgres process
// is alive and answered pg_isready within LeaderPostgresResponseThreshold is
// given that long to recover locally before failing over.
func (a *LeaderNeedsReplacementAnalyzer) leaderRecentlyReady(sa *ShardAnalysis) bool {
	if !leaderPostgresRunning(sa) {
		return false
	}
	lastReady := leaderLastPostgresReadyTime(sa)
	return !lastReady.IsZero() && sa.Now.Sub(lastReady) <= a.factory.Config().GetLeaderPostgresResponseThreshold()
}

// promotionPropagating reports whether an in-flight promotion still shows
// progress short of a quorum commit, within MaxPromotionTime of its proposal:
// the candidate is mid pg_promote(), or a durability-sufficient set of followers
// is receiving fresh WAL from it.
func (a *LeaderNeedsReplacementAnalyzer) promotionPropagating(
	sa *ShardAnalysis,
	leaderID *clustermetadatapb.ID,
	cohort []*clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
) bool {
	created := sa.HighestPosition.GetProposal().GetCreationTime()
	if created == nil || sa.Now.Sub(created.AsTime()) > sa.Policy.MaxPromotionTime {
		return false
	}
	return leaderMidPromote(sa) || receiveLsnStillAdvancing(sa, cohort, leaderID, policy)
}

// atRiskProblemIfDegraded returns a ShardAtRisk warning when a healthy leader
// could not be recovered from if lost BECAUSE cohort members are currently
// unreachable — a genuine degradation — and nil otherwise. It deliberately does
// NOT warn when the cohort is merely at its policy floor (e.g. 2 members under
// AtLeast(2)): that is the operator's chosen posture, not an anomaly, and would
// otherwise fire forever. The distinguisher: recovery is infeasible now but WOULD
// be feasible if every cohort member were reachable, i.e. standbys are missing.
func (a *LeaderNeedsReplacementAnalyzer) atRiskProblemIfDegraded(sa *ShardAnalysis, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID) []types.Problem {
	recoverableIfLeaderLost := recruitmentFeasible(policy, cohort, recruitableCohort(sa, cohort, leaderID))
	recoverableIfFullyReachable := recruitmentFeasible(policy, cohort, cohortWithout(cohort, leaderID))
	if !recoverableIfLeaderLost && recoverableIfFullyReachable {
		return a.atRiskProblem(sa, leaderID,
			fmt.Sprintf("Shard %s could not recover if its leader were lost: cohort members are unreachable", sa.ShardKey))
	}
	return nil
}

// emitFailover applies the feasibility gate to a leader that must be replaced. A
// safe failover needs *sufficient recruitment*: reach a strict majority of the
// outgoing cohort (so the new rule number is unique) and leave the un-reachable
// remainder unable to satisfy the durability policy (so the outgoing rule is
// revoked). If that is impossible the failover can't proceed, and we split on why:
//   - No fresh, usable observation of any shard pooler → NoHealthyCohortMembers: orch
//     is blind and cannot trust its (stale-derived) view of the leader, so it does
//     not convict it. Often transient; clears when fresh health returns.
//   - Some poolers are reachable but not a sufficient quorum → ShardStuck: a
//     confident verdict that progress is halted and a human must intervene.
//
// Both are alert-only. Otherwise the cause is actionable via AppointLeader. The old
// leader is not excluded from the reachable set — even an unhealthy-but-reachable
// leader can still participate in the recruit that establishes the new term.
func (a *LeaderNeedsReplacementAnalyzer) emitFailover(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, verdict leaderVerdict) []types.Problem {
	if !recruitmentFeasible(policy, cohort, recruitableCohort(sa, cohort, nil)) {
		return a.blindOrStuck(sa, leaderID, cohort,
			fmt.Sprintf("Shard %s needs a new leader (%s) but cannot reach a sufficient recruitment quorum", sa.ShardKey, verdict.cause))
	}
	problems := a.shardProblem(sa, leaderID, verdict.cause, types.PriorityEmergency, a.factory.NewAppointLeaderAction(), verdict.description)
	problems[0].NotBefore = verdict.notBefore
	return problems
}

// blindOrStuck returns the alert-only problem for an infeasible failover: no usable
// health of any cohort member → NoHealthyCohortMembers (blind); otherwise a sub-quorum
// cohort → ShardStuck (with stuckDescription).
func (a *LeaderNeedsReplacementAnalyzer) blindOrStuck(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, cohort []*clustermetadatapb.ID, stuckDescription string) []types.Problem {
	if !hasUsableShardHealth(sa, cohort) {
		return a.shardProblem(sa, leaderID, types.ProblemNoHealthyCohortMembers, types.PriorityEmergency, a.factory.NewAlertOnlyAction(),
			fmt.Sprintf("Shard %s has no healthy cohort members: no initialized pooler has a fresh, valid health report, so the leader cannot be judged", sa.ShardKey))
	}
	return a.shardProblem(sa, leaderID, types.ProblemShardStuck, types.PriorityEmergency, a.factory.NewAlertOnlyAction(), stuckDescription)
}

// atRiskProblem builds the ShardAtRisk warning. It is ScopePooler (anchored to the
// healthy leader) and PriorityNormal — deliberately NOT shard-wide/emergency — so
// it does not suppress the replica recoveries (standby adds) that resolve the risk.
func (a *LeaderNeedsReplacementAnalyzer) atRiskProblem(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, description string) []types.Problem {
	return []types.Problem{{
		Code:           types.ProblemShardAtRisk,
		CheckName:      a.Name(),
		PoolerID:       leaderID,
		ShardKey:       sa.ShardKey,
		Description:    description,
		Priority:       types.PriorityNormal,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAlertOnlyAction(),
	}}
}

// shardProblem builds the single shard-scoped problem this analyzer emits.
func (a *LeaderNeedsReplacementAnalyzer) shardProblem(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, code types.ProblemCode, priority types.Priority, action types.RecoveryAction, description string) []types.Problem {
	return []types.Problem{{
		Code:           code,
		CheckName:      a.Name(),
		PoolerID:       leaderID,
		ShardKey:       sa.ShardKey,
		Description:    description,
		Priority:       priority,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: action,
	}}
}
