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

// Package eligibility holds cohort-membership fitness logic shared by the
// analyzer (deciding what to propose) and the action (re-verifying a
// proposal still holds before applying it) — the analysis and actions
// packages can't import each other (analysis already depends on actions via
// RecoveryActionFactory), so this needs a home neither one owns.
//
// Deliberately minimal-dependency: only *store.Pooler and time.Time in,
// types.ProblemCode out. Long-term direction is for this kind of logic to
// live in go/common/availability, operating on just basic per-pooler health
// signals and a policy — this package is a step in that direction, not the
// final home.
package eligibility

import (
	"fmt"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// Op is a cohort-membership change.
type Op int

const (
	OpAdd Op = iota
	OpRemove
)

// Decision is what DecideAll recommends doing about a cohort ID right now.
type Decision struct {
	// ID is the pooler this Decision is about.
	ID *clustermetadatapb.ID
	Op Op
	// Reason and Description are meaningful only when Op == OpRemove; Op ==
	// OpAdd always means types.ProblemPoolerNotInCohort.
	Reason      types.ProblemCode
	Description string
	// Urgent is an OpRemove that costs nothing to act on immediately
	// (tombstoned/quarantined — see evaluate's unconditional). It bypasses
	// the durability-safety check entirely, unlike a non-urgent OpRemove.
	Urgent bool
}

// DecideAll judges every relevant ID in a shard: every pooler in poolers,
// plus any cohort member named in rule with no corresponding pooler
// (vanished entirely; tombstoned says whether that's a confirmed SHUTDOWN
// vs. a transient gap). Both the analyzer and the action call this, so they
// can't compute different answers for the same pooler.
//
// Non-urgent removals accumulate incrementally against a shrinking cohort,
// since two individually-safe removals aren't always safe together; a
// candidate that would break that is skipped, not returned. Urgent removals
// (tombstoned/quarantined) and additions have no such limit and can all
// batch into one UpdateConsensusRule call (see ReconcileCohortAction).
//
// Removing an unreachable follower assumes orch reaching the leader (to make
// the change) but not the follower is itself evidence the leader can't
// reach it either. TODO: use the leader's own replication stats instead,
// once available — a more direct signal than orch's own reachability.
func DecideAll(now time.Time, thresholds Thresholds, rule *clustermetadatapb.ShardRule, poolers []*store.Pooler, tombstoned func(*clustermetadatapb.ID) bool) []Decision {
	if tombstoned == nil {
		tombstoned = func(*clustermetadatapb.ID) bool { return false }
	}
	var decisions []Decision
	var nonUrgentRemovals []Decision

	consider := func(id *clustermetadatapb.ID, pa *store.Pooler, poolerTombstoned bool) {
		inCohort := isCohortMember(rule, id)
		reason, urgent := evaluate(now, thresholds, pa, inCohort, poolerTombstoned)
		switch {
		case inCohort && reason != "" && urgent:
			decisions = append(decisions, Decision{ID: id, Op: OpRemove, Reason: reason, Description: describeRemoval(now, id, pa, reason, poolerTombstoned), Urgent: true})
		case inCohort && reason != "":
			nonUrgentRemovals = append(nonUrgentRemovals, Decision{ID: id, Op: OpRemove, Reason: reason, Description: describeRemoval(now, id, pa, reason, poolerTombstoned)})
		case !inCohort && reason == "":
			decisions = append(decisions, Decision{ID: id, Op: OpAdd, Description: fmt.Sprintf("Pooler %s is replicating and eligible but not in the cohort", id.GetName())})
		}
	}

	seen := make(map[topoclient.ComponentID]struct{}, len(rule.GetCohortMembers()))
	for _, pa := range poolers {
		id := pa.Health().GetMultipooler().GetId()
		seen[topoclient.ComponentIDString(id)] = struct{}{}
		consider(id, pa, false)
	}
	for _, id := range rule.GetCohortMembers() {
		if _, ok := seen[topoclient.ComponentIDString(id)]; ok {
			continue
		}
		consider(id, nil, tombstoned(id))
	}

	return append(decisions, acceptSafeRemovals(rule, nonUrgentRemovals)...)
}

// acceptSafeRemovals returns the prefix-order subset of candidates that's
// safe to remove together: each is checked against the cohort as already
// shrunk by every previously-accepted candidate, not the original rule.
func acceptSafeRemovals(rule *clustermetadatapb.ShardRule, candidates []Decision) []Decision {
	if len(candidates) == 0 {
		return nil
	}
	policy, err := commonconsensus.NewPolicyFromProto(rule.GetDurabilityPolicy())
	if err != nil {
		return nil
	}
	cohort := rule.GetCohortMembers()
	leaderID := rule.GetLeaderId()

	var accepted []Decision
	for _, d := range candidates {
		trial := withoutID(cohort, d.ID)
		recruited := withoutID(trial, leaderID)
		if commonconsensus.CheckSufficientRecruitment(policy, trial, recruited) != nil {
			continue // unsafe together with what's already accepted; leave it in the cohort and keep trying others
		}
		cohort = trial
		accepted = append(accepted, d)
	}
	return accepted
}

// withoutID returns ids with target removed, or ids unchanged if target is
// nil or absent.
func withoutID(ids []*clustermetadatapb.ID, target *clustermetadatapb.ID) []*clustermetadatapb.ID {
	if target == nil {
		return ids
	}
	key := topoclient.ComponentIDString(target)
	out := make([]*clustermetadatapb.ID, 0, len(ids))
	for _, id := range ids {
		if topoclient.ComponentIDString(id) != key {
			out = append(out, id)
		}
	}
	return out
}

// isCohortMember reports whether id is currently named in rule's cohort.
func isCohortMember(rule *clustermetadatapb.ShardRule, id *clustermetadatapb.ID) bool {
	key := topoclient.ComponentIDString(id)
	for _, m := range rule.GetCohortMembers() {
		if topoclient.ComponentIDString(m) == key {
			return true
		}
	}
	return false
}

func describeRemoval(now time.Time, id *clustermetadatapb.ID, pa *store.Pooler, reason types.ProblemCode, tombstoned bool) string {
	if pa == nil {
		if tombstoned {
			return fmt.Sprintf("Cohort member %s is SHUTDOWN (cache tombstone); removing from cohort", id.GetName())
		}
		return fmt.Sprintf("Cohort member %s is no longer tracked by the pooler cache; removing from cohort", id.GetName())
	}
	switch reason {
	case types.ProblemCohortMemberIneligible:
		return fmt.Sprintf("Cohort member %s self-reported INELIGIBLE", id.GetName())
	case types.ProblemCohortMemberQuarantined:
		return fmt.Sprintf("Cohort member %s has self-quarantined", id.GetName())
	case types.ProblemCohortMemberUnhealthy:
		return fmt.Sprintf("Cohort member %s has failed health checks for %s", id.GetName(), unhealthyFor(pa, now))
	case types.ProblemCohortMemberLagging:
		lag, _ := lagOf(pa)
		return fmt.Sprintf("Cohort member %s replication lag %s exceeds eviction threshold", id.GetName(), lag)
	default:
		return ""
	}
}

// Thresholds bundles the duration knobs evaluate compares a pooler's
// unhealthy-duration and replication lag against.
type Thresholds struct {
	UnhealthyRemoval     time.Duration
	UnhealthyReadmission time.Duration
	LagEviction          time.Duration
	LagReadmission       time.Duration
}

// DefaultThresholds returns the built-in values used when no operator
// configuration is supplied.
func DefaultThresholds() Thresholds {
	return Thresholds{
		UnhealthyRemoval:     60 * time.Second,
		UnhealthyReadmission: 30 * time.Second,
		LagEviction:          1 * time.Minute,
		LagReadmission:       30 * time.Second,
	}
}

// evaluate reports why id currently fails the cohort quality bar, or "" if
// it passes, plus whether that reason justifies removal even if it leaves
// the shard unable to survive a subsequent leader failure. Tombstoned or
// quarantined poolers were never going to help a real failover, so removing
// them costs no actual protection ("unconditional"); ineligible, unhealthy,
// and lagging poolers might still ack a write in an emergency, so those stay
// gated on the caller's own durability-safety check.
//
// pa is id's current health rider, or nil if it has no live rider at all
// (vanished from the cache); tombstoned distinguishes a confirmed SHUTDOWN
// from a transient gap and is ignored when pa != nil.
//
// When inCohort is false, addition-only preconditions (initialized,
// streaming, not the leader, not RecruitBlockedUntil — see joinable) are
// folded in too, so callers only ever need this one check.
//
// inCohort picks which threshold tier applies: current membership is itself
// the memory of "was this pooler just excluded," so a value oscillating
// between the higher removal/eviction threshold and the lower readmission
// threshold stays excluded instead of flapping every cycle.
func evaluate(now time.Time, thresholds Thresholds, pa *store.Pooler, inCohort, tombstoned bool) (reason types.ProblemCode, unconditional bool) {
	if pa == nil {
		return types.ProblemCohortMemberIneligible, tombstoned
	}
	if pa.Health().GetMultipooler().GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED {
		return types.ProblemCohortMemberQuarantined, true
	}
	if types.PoolerIsCohortIneligible(pa.Health().GetAvailabilityStatus()) {
		return types.ProblemCohortMemberIneligible, false
	}
	if !inCohort && !joinable(pa) {
		return types.ProblemCohortMemberIneligible, false
	}
	unhealthyThreshold := thresholds.UnhealthyReadmission
	lagThreshold := thresholds.LagReadmission
	if inCohort {
		unhealthyThreshold = thresholds.UnhealthyRemoval
		lagThreshold = thresholds.LagEviction
	}
	if unhealthyFor(pa, now) > unhealthyThreshold {
		return types.ProblemCohortMemberUnhealthy, false
	}
	// hasLag is false when no lag reading is available at all — missing data
	// must never be treated as "exceeds," on either the removal or the
	// addition path.
	if lag, hasLag := lagOf(pa); hasLag && lag > lagThreshold {
		return types.ProblemCohortMemberLagging, false
	}
	return "", false
}

// unhealthyFor returns how long it's been since orch last successfully heard
// from pa (now minus LastSeen), or 0 if it has never been successfully
// checked (a brand-new pooler shouldn't be judged unhealthy before its first
// check lands).
//
// Deliberately keyed on LastSeen rather than gating on IsLastCheckValid:
// IsLastCheckValid only flips to false when a health check is actually
// attempted and fails. A pooler whose check goroutine stopped running
// entirely — dropped from the watch, crashed, network-partitioned before any
// failure was ever recorded — would keep a stale IsLastCheckValid=true
// forever and never be judged unhealthy under that gate, even though orch
// has heard nothing from it in a very long time. LastSeen only advances on
// an actual successful observation, so its age captures both "actively
// failing" and "gone silent" uniformly.
func unhealthyFor(pa *store.Pooler, now time.Time) time.Duration {
	// Check the field for nil directly rather than AsTime().IsZero(): an
	// absent Timestamp's AsTime() is the Unix epoch (1970), not Go's zero
	// time.Time, so IsZero() would never catch it and every never-seen
	// pooler would look decades stale instead of "no data yet."
	lastSeen := pa.Health().GetLastSeen()
	if lastSeen == nil {
		return 0
	}
	return now.Sub(lastSeen.AsTime())
}

// lagOf returns a standby's replication lag and true, or (0, false) if no
// lag reading is available (e.g. not a standby, or postgres hasn't reported
// one yet) — callers must treat missing data as "don't evict," not as zero
// lag.
func lagOf(pa *store.Pooler) (time.Duration, bool) {
	lag := pa.Health().GetStatus().GetReplicationStatus().GetLag()
	if lag == nil {
		return 0, false
	}
	return lag.AsDuration(), true
}

// joinable reports the addition-only preconditions folded into evaluate when
// inCohort is false: facts that only matter before membership, not after —
// a member mid pg_rewind is still a valid voter even though it wouldn't pass
// this today (see ConsensusStatus.RecruitBlockedUntil). The IsLeader check is
// here for the same reason: an acting primary adding itself isn't unsafe,
// just not what cohort reconciliation is for.
//
// walReceiverStreaming (not merely primary_conninfo set + replay unpaused)
// matters because admission clears the joining member's restore_command; a
// node still catching up from the archive would be stranded if admitted and
// stripped of it.
func joinable(pa *store.Pooler) bool {
	if commonconsensus.SelfConsensusRole(pa.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
		return false
	}
	if !pa.Health().IsLastCheckValid {
		return false
	}
	if !pa.IsInitialized() {
		return false
	}
	if primaryConnInfoHost(pa) == "" || !walReplayNotPaused(pa) {
		return false
	}
	if !walReceiverStreaming(pa) {
		return false
	}
	if pa.Health().GetConsensusStatus().GetRecruitBlockedUntil() != nil {
		return false
	}
	return true
}

// walReplayNotPaused reports whether the standby's WAL replay is active. A
// pooler with no replication status (e.g. a primary, or one we haven't
// observed replicating) returns false, so an unpopulated state errs toward
// repair rather than assuming health.
func walReplayNotPaused(p *store.Pooler) bool {
	rs := p.Health().GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	return !rs.GetIsWalReplayPaused()
}

// primaryConnInfoHost returns the standby's configured primary host, or "" if
// replication is not configured.
func primaryConnInfoHost(p *store.Pooler) string {
	return p.Health().GetStatus().GetReplicationStatus().GetPrimaryConnInfo().GetHost()
}

// walReceiverStreaming reports whether the standby's WAL receiver is
// genuinely pulling WAL from the leader — the signal cohort admission
// requires (a cohort member must advance only by streaming, never the
// archive). It additionally rejects the brief window after a receiver
// reconnect where postgres reports "streaming" before any WAL has actually
// arrived (LastReceiveLsn still empty). "waiting" stays healthy: the
// receiver is connected and current, the primary just has nothing new to
// send.
func walReceiverStreaming(p *store.Pooler) bool {
	rs := p.Health().GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	switch rs.GetWalReceiverStatus() {
	case "waiting":
		return true
	case "streaming":
		return rs.GetLastReceiveLsn() != ""
	default:
		return false
	}
}
