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
// three steps (see judgeLeader):
//
//  1. Progress is impossible → replace. Durable consensus facts that won't
//     change on their own: the leader resigned or shut down, it does not back
//     the current rule, or enough of its cohort revoked that rule that it can
//     no longer reach a quorum. These act at once.
//  2. Otherwise, wait for proof of progress: a quorum-commit watermark (the
//     heartbeat's last quorum-acknowledged write) no older than a patience.
//     The patience is QuorumCommitStaleAfter, unless a blocker is present.
//     Each blocker has its own, shorter patience in the availability policy.
//     A blocker is a state that would stop writes if it persisted: the
//     leader's postgres is down, unready or still in recovery, or enough of its
//     cohort is not streaming from it. Blockers are causes, not outcomes, so
//     they never convict alone; they only shorten the wait, and a fresh commit
//     overrides them (the blocker was wrong, or already recovered).
//  3. Commits stalled longer than the patience → replace, with the blocker's
//     code, or LeaderProgressUnproven when there is none (LeaderPromotionIncomplete
//     for an undecided promotion). The problem is always reported, since writes
//     are unavailable either way, but the failover is deferred
//     (Problem.NotBefore) while progress may be imminent: for the patience after
//     the current rule was created (its leader hasn't had time to commit a
//     watermark), and for up to MaxPromotionTime while a promotion is
//     propagating (its candidate is mid pg_promote(), or followers are
//     receiving its WAL).
//
// While a promotion is in flight, the blockers that a promotion itself causes
// (a candidate still in recovery, followers reconnecting) don't apply.
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

	verdict, progressProven := a.judgeLeader(sa, leaderID, cohort, policy)
	if progressProven {
		return a.atRiskProblemIfDegraded(sa, policy, cohort, leaderID), nil
	}
	// Anything short of proof is reported as a problem; verdict.notBefore only
	// says when the failover for it may run.
	return a.emitFailover(sa, leaderID, policy, cohort, verdict), nil
}

// leaderVerdict says why the shard has a leader problem, and when its failover
// may run. The problem is always reported. notBefore only defers acting on it
// while progress may be imminent, and is the zero time when there is no reason
// to wait.
type leaderVerdict struct {
	cause       types.ProblemCode
	description string
	notBefore   time.Time
}

// replaceLeader is a verdict that the leader should be replaced, to run at once
// unless notBefore is later set.
func replaceLeader(cause types.ProblemCode, description string) leaderVerdict {
	return leaderVerdict{cause: cause, description: description}
}

// judgeLeader applies the steps documented on LeaderNeedsReplacementAnalyzer.
// It returns progressProven=true when the shard provably makes progress, and
// otherwise the verdict for the problem to report, whose notBefore may defer
// the failover.
func (a *LeaderNeedsReplacementAnalyzer) judgeLeader(
	sa *ShardAnalysis,
	leaderID *clustermetadatapb.ID,
	cohort []*clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
) (verdict leaderVerdict, progressProven bool) {
	inFlight := promotionInFlight(sa, leaderID)
	revoked, disconnected := a.classifyFollowers(sa, cohort, leaderID)

	// 1. Progress is impossible.
	if verdict, ok := a.progressImpossible(sa, leaderID, cohort, policy, revoked); ok {
		return verdict, false
	}

	// 2. Wait for proof of progress; a blocker shortens the wait.
	patience := sa.Policy.QuorumCommitStaleAfter
	blocker, blocked := a.findBlocker(sa, cohort, policy, inFlight, revoked, disconnected)
	if blocked {
		patience = blocker.patience
	}
	if !commitsStalledFor(sa, patience) {
		return leaderVerdict{}, true
	}

	// 3. Stalled, so writes are unavailable and this is a problem to report now.
	// Whether to act on it now is separate: defer the failover (notBefore) while
	// progress may still be imminent. A new rule's leader needs the patience to
	// commit its first watermark, and a propagating promotion gets up to
	// MaxPromotionTime.
	created := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition).GetCreationTime().AsTime()
	switch {
	case blocked:
		verdict = replaceLeader(blocker.cause, blocker.description)
	case inFlight:
		verdict = replaceLeader(types.ProblemLeaderPromotionIncomplete,
			fmt.Sprintf("Shard %s is promoting a new leader that has not yet committed a quorum write", sa.ShardKey))
	default:
		verdict = replaceLeader(types.ProblemLeaderProgressUnproven,
			fmt.Sprintf("Shard %s has no quorum-commit watermark newer than %s", sa.ShardKey, patience))
	}
	verdict.notBefore = created.Add(patience)
	if inFlight && !blocked && a.promotionPropagating(sa, leaderID, cohort, policy) {
		verdict.notBefore = created.Add(sa.Policy.MaxPromotionTime)
	}
	return verdict, false
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

// blocker is a state that would stop writes if it persisted. It is evidence of a
// cause, not of a stall, so it only shortens how long judgeLeader waits for
// commits to resume.
type blocker struct {
	cause       types.ProblemCode
	description string
	// patience is how long commits may be stalled before this blocker convicts.
	// Zero means a state that cannot be a blip, so it convicts at once.
	patience time.Duration
}

// findBlocker reports the first blocker, if any, from the followers' and the
// leader's current state.
func (a *LeaderNeedsReplacementAnalyzer) findBlocker(
	sa *ShardAnalysis,
	cohort []*clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
	inFlight bool,
	revoked, disconnected []*clustermetadatapb.ID,
) (blocker, bool) {
	// Enough followers are pointed at the leader yet not streaming (past
	// ConnectReplicasToNewLeaderGrace) that the rest cannot form a quorum. Not
	// during a promotion: the candidate's pg_promote() switches timeline,
	// briefly disconnecting every cascading follower at once. A promotion whose
	// followers never reconnect shows no WAL progress, so it isn't deferred
	// (see promotionPropagating).
	lapsed := append(append([]*clustermetadatapb.ID{}, revoked...), disconnected...)
	if !inFlight && revocationSufficient(policy, cohort, lapsed) {
		return blocker{
			cause:       types.ProblemLeaderUnreachableByCohort,
			description: fmt.Sprintf("Leader for shard %s is not reachable by a durability-sufficient set of its cohort", sa.ShardKey),
			patience:    sa.Policy.FollowerDisconnectPatience,
		}, true
	}

	// The leader's postgres cannot serve writes. Judged only from a fresh report.
	if !leaderObservedLive(sa) {
		return blocker{}, false
	}
	// A STANDBY answers pg_isready, so without this it would look fine forever.
	// Not during a promotion: the candidate is still a standby until its
	// pg_promote() runs. A standby cannot commit, so there is nothing to wait for.
	if leaderInRecovery(sa) && !inFlight {
		return blocker{
			cause:       types.ProblemLeaderUnhealthy,
			description: fmt.Sprintf("Leader for shard %s is reachable but its postgres is in recovery (not a primary)", sa.ShardKey),
		}, true
	}
	if leaderPostgresReady(sa) || (inFlight && leaderMidPromote(sa)) {
		return blocker{}, false // a promotion mid pg_promote() is briefly not ready by design
	}
	// Not ready and not running: a dead postgres cannot recover on its own the
	// way a blip can, and the pooler's own restart is slower than failing over.
	if !leaderPostgresRunning(sa) {
		return blocker{
			cause:       types.ProblemLeaderUnhealthy,
			description: fmt.Sprintf("Leader for shard %s is reachable but its postgres is not running", sa.ShardKey),
		}, true
	}
	// Running but not ready: starting or wedged. It may come back, so wait.
	return blocker{
		cause:       types.ProblemLeaderUnhealthy,
		description: fmt.Sprintf("Leader for shard %s is reachable but its postgres is not ready", sa.ShardKey),
		patience:    sa.Policy.PostgresUnreadyPatience,
	}, true
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
