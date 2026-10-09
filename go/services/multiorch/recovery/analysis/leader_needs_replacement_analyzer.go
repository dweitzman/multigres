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
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// LeaderNeedsReplacementAnalyzer judges a shard's leader/durability situation and
// emits at most one shard-level problem per cycle. It reasons on two independent
// axes (see leaderReplacementCause for the judgment, leaderFitnessCause for the
// second axis), then crosses the verdict with failover feasibility:
//
//   - Rule support: does a durability-sufficient set of the cohort — the leader
//     itself, mandatorily, plus enough followers — back the shard's highest-known
//     rule? Consensus-bookkeeping evidence (self-reports, revocations, streaming).
//   - Leader fitness: given the rule is supported, is the specific pooler named
//     leader physically able to serve writes right now? Postgres-liveness
//     evidence, orthogonal to consensus. Rule support (LeaderNotSelfConfirmed,
//     LeaderLacksCohortSupport) is checked first: a leader nobody backs cannot be judged fit no matter how
//     healthy its postgres looks. The LeaderQuorumWritesStalled backstop (is the
//     quorum-commit watermark advancing?) is the last fitness check, applied
//     before either healthy verdict is returned.
//   - Could a failover succeed? Only if a durability-sufficient set of reachable,
//     initialized poolers is available to recruit a replacement.
//
// Crossing the leader verdict with failover feasibility (non-actionable outcomes are
// alert-only):
//   - replace + feasible   → the cause code (actionable → AppointLeader).
//   - replace + infeasible → ShardStuck, or NoHealthyCohortMembers when blind.
//   - healthy              → no problem, or ShardAtRisk if losing the leader would strand the shard.
//   - inconclusive         → LeaderHealthUnknown, or the infeasible codes above when we also can't recruit.
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

	undecidedRule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
	leaderID := undecidedRule.GetLeaderId()
	cohort := undecidedRule.GetCohortMembers()

	// No rule at all yet, or a rule naming neither a leader nor a cohort — the
	// initial, unbootstrapped state. ShardNeedsInitialization owns that, so do
	// nothing here; there's no established policy to read yet either.
	if leaderID == nil && len(cohort) == 0 {
		return nil, nil
	}

	policy, err := commonconsensus.NewPolicyFromProto(undecidedRule.GetDurabilityPolicy())
	if err != nil {
		return nil, mterrors.Wrap(err, "leader-needs-replacement: durability policy unavailable")
	}

	// A non-empty cohort with no designated leader needs one recruited.
	if leaderID == nil {
		return a.emitFailover(sa, nil, policy, cohort, types.ProblemLeaderUnspecified,
			fmt.Sprintf("Shard %s has cohort members but no designated leader", sa.ShardKey)), nil
	}

	// Suppress failover briefly while the leader is mid-promotion (see
	// inPromotionGrace).
	if inPromotionGrace(sa) {
		a.factory.Logger().Info("primary promotion in progress within grace, suppressing failover",
			"shard_key", sa.ShardKey.String(),
			"promoting_primary", topoclient.ComponentIDString(leaderID),
			"rule_age", sa.Now.Sub(undecidedRule.GetCreationTime().AsTime()))
		return nil, nil
	}

	// Judge the leader: healthy, must-replace (with a cause), or inconclusive.
	cause, description, inconclusive := a.leaderReplacementCause(sa, cohort, leaderID, policy)

	switch {
	case inconclusive:
		// We can neither confirm the leader healthy nor conclusively convict it.
		// Never fail over; surface only a blind spot we cannot act through.
		return a.emitInconclusive(sa, leaderID, policy, cohort), nil
	case cause == "":
		// Healthy leader — but warn if losing it now would strand the shard.
		return a.atRiskProblemIfDegraded(sa, policy, cohort, leaderID), nil
	default:
		// Must replace: gate on whether a failover could actually succeed.
		return a.emitFailover(sa, leaderID, policy, cohort, cause, description), nil
	}
}

// inPromotionGrace reports whether failover should be briefly suppressed because
// the leader is mid-promotion: a freshly-created leadership rule needs a moment
// for followers to reconnect and start streaming before "are followers vouching?"
// is meaningful. The grace holds while the leader reports promoting (postgres
// still running) AND the rule is younger than ConnectReplicasToNewLeaderGrace. The
// rule-age bound is the point — a leader that claims to be promoting forever but
// never gains followers cannot make progress, so once the grace lapses we stop
// honoring the claim and let normal detection fail it over.
//
// TODO: remove the PROMOTING-status coupling. current_position's Decision
// (read by IsActiveLeader) can't move before WAL catch-up — decided must
// mean durably confirmed. replication_primary already updates early via
// RecordTermPrimary, recording only what the pooler was told. Have orch's
// self-check accept that as self-asserted-but-unconfirmed leadership while
// promoting instead; the postgres monitor self-resigns if it turns out
// unbacked (rule absent → resign → LeaderUnspecified → re-recruit).
func inPromotionGrace(sa *ShardAnalysis) bool {
	if !leaderPromoting(sa) || !leaderObservedLive(sa) || !leaderPostgresRunning(sa) {
		return false
	}
	ruleAge := sa.Now.Sub(commonconsensus.PossiblyUndecidedRule(sa.HighestPosition).GetCreationTime().AsTime())
	return ruleAge < sa.Policy.ConnectReplicasToNewLeaderGrace
}

// leaderReplacementCause returns one of three verdicts: healthy (cause=="",
// inconclusive==false), replace (cause!=""), or inconclusive (cause=="",
// inconclusive==true — can neither confirm healthy nor conclusively convict; the
// caller must NOT treat it as healthy). It judges two independent axes in order:
//
//   - Rule support: does a durability-sufficient set of the cohort — the leader
//     itself, mandatorily, plus enough followers — currently back the shard's
//     highest-known rule? This is consensus-bookkeeping evidence (self-reports,
//     revocations, streaming), and it gates everything below: a leader nobody
//     (including itself) backs cannot be judged fit to serve no matter how
//     healthy its postgres looks.
//   - Leader fitness: given the rule is supported, is the specific pooler named
//     leader physically able to serve writes right now? Postgres-liveness
//     evidence, orthogonal to consensus.
//
// The cause follows the first-hand vs observer-derived principle (see the
// leader problem docs in the types package).
func (a *LeaderNeedsReplacementAnalyzer) leaderReplacementCause(
	sa *ShardAnalysis,
	cohort []*clustermetadatapb.ID,
	leaderID *clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
) (cause types.ProblemCode, description string, inconclusive bool) {
	// First-hand, authoritative intent to step down — act immediately, bypassing
	// progress/liveness signals. leaderHasResigned is the fast path (the leader's
	// REQUESTING_DEMOTION/INELIGIBLE health broadcast); the SHUTDOWN tombstone is the
	// durable fallback for when that ephemeral broadcast is lost.
	// TODO: first-hand causes still wait the shared failover grace; they could skip it
	// (intent doesn't flap; multi-orch safety is the Recruit CAS), cutting the write outage.
	if leaderHasResigned(sa) || leaderShutdownTombstoned(sa, leaderID) {
		return types.ProblemLeaderResigned,
			fmt.Sprintf("Leader for shard %s is stepping down", sa.ShardKey), false
	}

	vouching, cutOff := a.classifyFollowerReachability(sa, cohort, leaderID)
	leaderPart := leaderParticipation(sa, vouching)

	switch {
	case leaderPart == participationLapsed:
		// The leader's own report proves it doesn't back its own rule (never
		// confirmed the term, or self-revoked with no successor decided yet) —
		// disqualifying on its own regardless of follower support, since writes
		// only ever flow through the leader.
		return types.ProblemLeaderNotSelfConfirmed,
			fmt.Sprintf("Leader for shard %s does not confirm its own role in the current rule", sa.ShardKey), false
	case revocationSufficient(policy, cohort, cutOff):
		// Followers conclusively lapsed are sufficient to revoke the term: the
		// members not lapsed (including the leader) can no longer satisfy the
		// policy. Mirrors the recruitment-feasibility gate: same conclusive
		// revocation to detect the failure as to act.
		return types.ProblemLeaderLacksCohortSupport,
			fmt.Sprintf("Leader for shard %s is not backed by a durability-sufficient set of its cohort", sa.ShardKey), false
	case leaderPart == participationActive || policy.SatisfiedBy(vouching) == nil:
		// The rule is supported — either the leader confirms itself directly, or
		// a durability-sufficient set of followers vouches for it indirectly (you
		// cannot stream from a dead primary). Move to the fitness axis.
		return a.leaderFitnessCause(sa, cohort, leaderID, policy)
	default:
		// NEITHER — inconclusive, NOT healthy: we may simply not have looked long
		// enough (a freshly (re)started orch, or followers mid-reconnect).
		return "", "", true
	}
}

// leaderParticipation answers the same question as classifyFollowerToLeader,
// but for the leader itself: does it back its own rule? A fresh, decided,
// non-revoked, self-naming report (commonconsensus.IsActiveLeader) is direct
// proof. When there is no fresh observation of the leader at all, a vouching
// follower is indirect proof instead (you cannot stream from a dead primary)
// — but only followers already confirmed active count, precisely so a
// follower whose own evidence was discounted above (e.g.
// self-revoked-but-streaming) can't indirectly vouch for the leader either.
// vouching is classifyFollowerReachability's result. Note this indirect path
// is only consulted absent a fresh observation — a directly-observed leader
// whose own report fails the direct-proof check goes straight to lapsed, not
// through this fallback. A genuinely mid-promotion leader can still fail the
// direct-proof check (see inPromotionGrace's doc for why); that's tolerated
// by suppressing failover earlier in Analyze(), before this is ever called,
// not by this function treating it as active.
func leaderParticipation(sa *ShardAnalysis, vouching []*clustermetadatapb.ID) ruleParticipation {
	if sa.Leader == nil {
		return participationUnknown
	}
	if leaderObservedLive(sa) {
		cs := sa.Leader.Health().GetConsensusStatus()
		if commonconsensus.IsActiveLeader(cs) {
			return participationActive
		}
		rule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
		if created := rule.GetCreationTime(); created != nil && sa.Now.Sub(created.AsTime()) <= sa.Policy.ConnectReplicasToNewLeaderGrace {
			return participationAdapting
		}
		return participationLapsed
	}
	if len(vouching) > 0 {
		return participationActive
	}
	return participationUnknown
}

// leaderFitnessCause judges the leader-fitness axis: given the rule is
// already known to be supported, is this specific pooler's postgres actually
// able to serve writes right now? Postgres state is first-hand — no quorum
// corroboration needed, since a pooler's own state is its own to report; only the
// quorum-commit backstop consults cohort-observed evidence. Shares
// leaderReplacementCause's three-way verdict contract (named returns to make
// that explicit), since it's a direct delegate of it.
//
// The last check before either healthy verdict is quorumCommitStuckCause, the
// LeaderQuorumWritesStalled backstop: a live, postgres-ready leader can still fail to
// make durable progress.
func (a *LeaderNeedsReplacementAnalyzer) leaderFitnessCause(
	sa *ShardAnalysis,
	cohort []*clustermetadatapb.ID,
	leaderID *clustermetadatapb.ID,
	policy commonconsensus.DurabilityPolicy,
) (cause types.ProblemCode, description string, inconclusive bool) {
	if !leaderObservedLive(sa) {
		// No direct observation to judge postgres fitness from. But
		// leaderReplacementCause only reaches this axis once rule support is
		// already confirmed — directly, or via a vouching cohort proving the
		// leader alive without our own observation of it — so only the
		// quorum-commit backstop (which any cohort member's report can feed)
		// remains.
		return a.quorumCommitStuckCause(sa, cohort, leaderID, policy)
	}
	// A leader whose postgres is in recovery (a STANDBY) cannot accept writes, yet
	// answers pg_isready continuously — so it would pass BOTH the healthy fast-path
	// and the anti-flap grace below forever, masking the absence of a writable
	// primary. Convict it so a real primary is promoted. (PROMOTING is handled
	// upstream by inPromotionGrace; transient non-ready states by the anti-flap
	// timeout — see leaderInRecovery.)
	//
	// TODO: this is a tactical guard. The durable fix is on the pooler side: a
	// pooler that knows it should be acting as leader would detect in its postgres
	// monitor that it is in recovery mode and publish that it needs to resign
	// leadership — replace this guard once that lands.
	if leaderInRecovery(sa) {
		return types.ProblemLeaderUnhealthy,
			fmt.Sprintf("Leader for shard %s is reachable but its postgres is in recovery (not a primary)", sa.ShardKey), false
	}
	if leaderPostgresReady(sa) {
		return a.quorumCommitStuckCause(sa, cohort, leaderID, policy)
	}
	// The leader's own postgres is not ready. Anti-flap: treat as healthy while
	// the process is alive and postgres responded within the response window;
	// once it lapses, a wedged postgres must not block failover forever.
	// (Interim guard, replaced by the LSN progress signal when LeaderQuorumWritesStalled lands.)
	if leaderPostgresRunning(sa) {
		threshold := a.factory.Config().GetLeaderPostgresResponseThreshold()
		lastReady := leaderLastPostgresReadyTime(sa)
		if !lastReady.IsZero() && time.Since(lastReady) <= threshold {
			return "", "", false
		}
	}
	return types.ProblemLeaderUnhealthy,
		fmt.Sprintf("Leader for shard %s is reachable but its postgres is unhealthy", sa.ShardKey), false
}

// quorumCommitStuckCause checks the LeaderQuorumWritesStalled backstop: the leader looks
// healthy but quorum commits have stalled even though replicas can still
// show raw LSN progress (they replay WAL ahead of the primary's own
// synchronous-quorum ack). Absence of evidence must not convict, so this is
// healthy (cause=="") when no pooler has reported a quorum_commit_ts yet.
func (a *LeaderNeedsReplacementAnalyzer) quorumCommitStuckCause(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy) (types.ProblemCode, string, bool) {
	freshest := freshestQuorumCommitTs(sa)
	if !consensus.QuorumCommitStale(freshest, sa.Now, sa.Policy.QuorumCommitStaleAfter) {
		return "", "", false
	}
	// A DECIDED rule already proves a quorum-acked commit succeeded under this
	// leadership (the finalize commit is itself quorum-gated), so the
	// backlog-draining excuse only applies while still undecided.
	if !commonconsensus.IsRuleDecided(sa.HighestPosition) && a.receiveLsnStillAdvancing(sa, cohort, leaderID, policy) {
		return "", "", false
	}
	return types.ProblemLeaderQuorumWritesStalled,
		fmt.Sprintf("Leader for shard %s appears healthy but quorum commits have not advanced in over %s", sa.ShardKey, sa.Policy.QuorumCommitStaleAfter), false
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
func (a *LeaderNeedsReplacementAnalyzer) emitFailover(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, cause types.ProblemCode, description string) []types.Problem {
	if !recruitmentFeasible(policy, cohort, recruitableCohort(sa, cohort, nil)) {
		return a.blindOrStuck(sa, leaderID, cohort,
			fmt.Sprintf("Shard %s needs a new leader (%s) but cannot reach a sufficient recruitment quorum", sa.ShardKey, cause))
	}
	return a.shardProblem(sa, leaderID, cause, types.PriorityEmergency, a.factory.NewAppointLeaderAction(), description)
}

// emitInconclusive handles a leader we can neither confirm healthy nor convict. It
// never fails over; it emits an alert-only problem describing why we can't tell:
// blind (no cohort health) → NoHealthyCohortMembers; a must-replace leader with no
// recruitment quorum → ShardStuck; otherwise a recoverable-but-unconfirmed cohort →
// LeaderHealthUnknown (a warning: the leader may be fine, we just lack conclusive
// evidence). Transient at cold start; persistent means orch has lost sight of the
// leader with an ambiguous cohort.
//
// TODO(propagation): the progress axis will further split LeaderHealthUnknown into
// ShardWritesBlockedOnPropagation when a quorum is catching up but not yet current.
func (a *LeaderNeedsReplacementAnalyzer) emitInconclusive(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID) []types.Problem {
	if recruitmentFeasible(policy, cohort, recruitableCohort(sa, cohort, nil)) {
		return a.shardProblem(sa, leaderID, types.ProblemLeaderHealthUnknown, types.PriorityNormal, a.factory.NewAlertOnlyAction(),
			fmt.Sprintf("Shard %s leader health is unknown: orch cannot confirm it is serving a quorum nor that its cohort is cut off from it", sa.ShardKey))
	}
	return a.blindOrStuck(sa, leaderID, cohort,
		fmt.Sprintf("Shard %s cannot confirm leader progress and cannot reach a sufficient recruitment quorum", sa.ShardKey))
}

// blindOrStuck returns the alert-only problem for an infeasible failover: no usable
// health of any cohort member → NoHealthyCohortMembers (blind); otherwise a sub-quorum
// cohort → ShardStuck (with stuckDescription). Shared by emitFailover/emitInconclusive.
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
