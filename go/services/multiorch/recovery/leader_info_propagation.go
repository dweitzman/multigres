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

package recovery

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// leaderInfoPropagationInterval is deliberately much shorter than a recovery
// cycle: telling a pooler who the leader is (or that it's rewind-ready) is
// always safe (SetPrimary no-ops when the pooler is already current), so
// this doesn't need recovery's grace periods or backoff.
const leaderInfoPropagationInterval = 1 * time.Second

const setPrimaryPropagationTimeout = 5 * time.Second

// leaderInfoStaleHealthThreshold skips propagation to a follower we haven't
// heard from recently: it's likely unreachable, and a shard's pass waits for
// every follower's SetPrimary, so a dead follower would hold the pass (and the
// next tick) up for the full setPrimaryPropagationTimeout.
const leaderInfoStaleHealthThreshold = 1 * time.Minute

// runLeaderInfoPropagation is a lightweight loop, independent of the
// recovery cycle, that keeps every follower's known leader identity and
// rewind-readiness current via SetPrimary. It exists because AppointLeaderAction
// can block on the leader's Promote RPC for the full AppointLeaderAction timeout, during
// which the (single-flight) recovery cycle can't do anything else — including
// re-informing a follower that the leader it's waiting on has just become
// rewind-ready. This loop runs on its own short interval so that information
// reaches followers promptly regardless of what the recovery cycle is doing.
func (re *Engine) runLeaderInfoPropagation(ctx context.Context) {
	// No bootstrap policy: propagation never reads it, and looking it up can block on topology.
	opts := store.SnapshotOptions{Now: time.Now(), AvailabilityPolicy: re.actionFactory.AvailabilityPolicy()}
	for _, shard := range store.SnapshotShards(re.poolerCache, opts) {
		re.propagateLeaderInfoForShard(ctx, shard)
	}
}

// propagateLeaderInfoForShard tells every follower in the shard about the
// current leader and its rewind-readiness, skipping any follower that already
// knows both. It judges from the frozen shard, so the leader,
// the rule and each pooler's state agree for the whole pass.
func (re *Engine) propagateLeaderInfoForShard(ctx context.Context, shard *snapshot.Shard) {
	leader := shard.IntendedLeaderHealth()
	rule := shard.HighestIntendedRule()
	if leader == nil || rule.IsEmpty() {
		return
	}

	// Ensure the leader holds a physical replication slot for every
	// cohort-eligible follower BEFORE any follower is pointed at it via
	// SetPrimary below. A late-joining follower cannot stream — and therefore
	// cannot join the cohort, and therefore cannot get its slot via the
	// cohort-add path — until its slot already exists on the primary; this
	// discovery-driven, declarative reconcile breaks that ordering deadlock
	// (§2.6). Idempotent and level-triggered, so a missed event or a leader
	// restart self-heals on the next tick.
	re.reconcileFollowerSlots(ctx, leader, shard.CohortEligibleFollowerIDs())

	leaderAddress := topoclient.PoolerAddressFor(leader.GetMultipooler())
	leaderRewindReady := commonconsensus.ReplicationPrimaryOrNil(leader.GetConsensusStatus()).GetRewindReady()

	// One goroutine per follower: each pooler appears at most once in a shard's
	// member list, so there's never more than one in-flight SetPrimary to the
	// same follower. Waiting for all of them keeps a slow/unreachable follower
	// from delaying the other followers in this pass.
	var wg sync.WaitGroup
	for _, pooler := range shard.Poolers {
		if pooler == leader {
			continue
		}
		wg.Add(1)
		go func(follower *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			re.propagateLeaderInfoToPooler(ctx, follower, shard.Now, leaderAddress, leaderRewindReady, rule)
		}(pooler)
	}
	wg.Wait()
}

// reconcileFollowerSlots notifies the leader of the current cohort-eligible
// follower set so it pre-creates their per-follower physical replication slots.
// Best-effort: a failure is logged and retried on the next tick. No-op when the
// set is empty (single-node shard, or nothing discovered yet).
func (re *Engine) reconcileFollowerSlots(ctx context.Context, leader *multiorchdatapb.PoolerHealthState, followerIDs []*clustermetadatapb.ID) {
	if len(followerIDs) == 0 {
		return
	}
	rpcCtx, cancel := context.WithTimeout(ctx, setPrimaryPropagationTimeout)
	defer cancel()
	if _, err := re.rpcClient.ReconcileFollowers(rpcCtx, leader.GetMultipooler(), &multipoolermanagerdatapb.ReconcileFollowersRequest{
		Followers: followerIDs,
	}); err != nil {
		re.logger.WarnContext(ctx, "leader-info propagation: ReconcileFollowers failed, will retry next tick",
			"leader", leader.GetMultipooler().GetId().GetName(), "error", err)
	}
}

// shouldPropagateLeaderInfo reports whether a follower needs a SetPrimary call
// to learn the current leader or its rewind-readiness, given followerPrimary (the
// follower's own last-reported ReplicationPrimary, nil if never told anything)
// and followerLastSeen (when we last heard from the follower, nil if never). now is
// passed in rather than read from the clock so this stays a pure function to
// unit test directly, independent of the health cache and RPC client.
func shouldPropagateLeaderInfo(
	followerPrimary *clustermetadatapb.ReplicationPrimary,
	followerLastSeen *timestamppb.Timestamp,
	now time.Time,
	leaderAddress *clustermetadatapb.PoolerAddress,
	leaderRewindReady bool,
	intendedRule commonconsensus.RuleTransition,
) bool {
	if now.Sub(followerLastSeen.AsTime()) > leaderInfoStaleHealthThreshold {
		return false // likely unreachable; don't let it hold up the pass
	}
	positionMatches := commonconsensus.ReplicationPrimaryMatches(followerPrimary, leaderAddress, intendedRule.Position())
	// rewind_ready only ever goes false -> true within a coordinator term (see
	// RecordTermPrimary), so never tell a follower that already believes true
	// otherwise — even if our own cached view of the leader is stale-false.
	if positionMatches && (followerPrimary.GetRewindReady() || followerPrimary.GetRewindReady() == leaderRewindReady) {
		return false // already knows everything we'd tell it
	}
	return true
}

// propagateLeaderInfoToPooler tells one follower about the current leader (its
// address, rewind-readiness and the intended rule) via SetPrimary, unless the
// follower already knows all three. follower is the follower's health as of the
// snapshot at now; the leader is described only by leaderAddress and
// leaderRewindReady.
func (re *Engine) propagateLeaderInfoToPooler(
	ctx context.Context,
	follower *multiorchdatapb.PoolerHealthState,
	now time.Time,
	leaderAddress *clustermetadatapb.PoolerAddress,
	leaderRewindReady bool,
	intendedRule commonconsensus.RuleTransition,
) {
	followerPrimary := commonconsensus.ReplicationPrimaryOrNil(follower.GetConsensusStatus())
	if !shouldPropagateLeaderInfo(followerPrimary, follower.GetLastSeen(), now, leaderAddress, leaderRewindReady, intendedRule) {
		return
	}

	toTell := &clustermetadatapb.ReplicationPrimary{
		Position:    intendedRule.Position(),
		Primary:     leaderAddress,
		RewindReady: leaderRewindReady,
	}
	rpcCtx, cancel := context.WithTimeout(ctx, setPrimaryPropagationTimeout)
	defer cancel()
	_, err := re.rpcClient.SetPrimary(rpcCtx, follower.GetMultipooler(), &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: toTell,
	})
	if err != nil {
		re.logger.WarnContext(ctx, "leader-info propagation: SetPrimary failed, will retry next tick",
			"follower", follower.GetMultipooler().GetId().GetName(), "error", err)
		return
	}

	// Optimistically cache the just-applied primary so the next reconcile
	// tick doesn't redundantly re-send it before the pooler's own streamed
	// health update reports it back to us. The pooler may have left the cache
	// since the snapshot was taken; there is then nothing left to update.
	entry, ok := re.poolerCache.Get(topoclient.ComponentIDString(follower.GetMultipooler().GetId()))
	if !ok {
		return
	}
	entry.Rider.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
		if h.ConsensusStatus == nil {
			h.ConsensusStatus = &clustermetadatapb.ConsensusStatus{}
		}
		current := commonconsensus.ReplicationPrimaryOrNil(h.ConsensusStatus)
		if merged, changed := commonconsensus.MergeReplicationPrimary(current, toTell); changed {
			h.ConsensusStatus.ReplicationPrimary = merged //nolint:gocritic // the linter is trying to protect against not using ReplicationPrimaryOrNil on read, but this is a write.
		}
	})
}
