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

package eligibility

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

var testThresholds = Thresholds{
	UnhealthyRemoval:     60 * time.Second,
	UnhealthyReadmission: 30 * time.Second,
	LagEviction:          time.Minute,
	LagReadmission:       30 * time.Second,
}

// healthyPooler returns a rider for a healthy, replicating, joinable REPLICA.
func healthyPooler(id *clustermetadatapb.ID) *store.Pooler {
	return store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:      &clustermetadatapb.Multipooler{Id: id},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: true,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
				WalReceiverStatus: "streaming",
				LastReceiveLsn:    "0/3000000",
			},
		},
	}, nil)
}

func TestUnhealthyFor(t *testing.T) {
	now := time.Now()

	t.Run("never seen returns 0", func(t *testing.T) {
		pa := store.NewPooler(&multiorchdatapb.PoolerHealthState{}, nil)
		assert.Equal(t, time.Duration(0), UnhealthyFor(pa, now))
	})

	t.Run("returns age since LastSeen", func(t *testing.T) {
		pa := store.NewPooler(&multiorchdatapb.PoolerHealthState{
			LastSeen: timestamppb.New(now.Add(-90 * time.Second)),
		}, nil)
		assert.InDelta(t, 90*time.Second, UnhealthyFor(pa, now), float64(time.Second))
	})
}

func TestLagOf(t *testing.T) {
	t.Run("no reading returns false", func(t *testing.T) {
		pa := store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Status: &multipoolermanagerdatapb.Status{ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{}},
		}, nil)
		lag, ok := LagOf(pa)
		assert.False(t, ok)
		assert.Equal(t, time.Duration(0), lag)
	})

	t.Run("returns the reported lag", func(t *testing.T) {
		pa := store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Status: &multipoolermanagerdatapb.Status{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{Lag: durationpb.New(45 * time.Second)},
			},
		}, nil)
		lag, ok := LagOf(pa)
		assert.True(t, ok)
		assert.Equal(t, 45*time.Second, lag)
	})
}

func TestIsCohortMember(t *testing.T) {
	memberID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "a"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "b"}
	rule := &clustermetadatapb.ShardRule{CohortMembers: []*clustermetadatapb.ID{memberID}}

	assert.True(t, IsCohortMember(rule, memberID))
	assert.False(t, IsCohortMember(rule, otherID))
}

func TestEvaluate(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "a"}
	now := time.Now()

	t.Run("nil rider is ineligible, unconditional iff tombstoned", func(t *testing.T) {
		reason, unconditional := Evaluate(now, testThresholds, id, nil, true, true)
		assert.Equal(t, types.ProblemCohortMemberIneligible, reason)
		assert.True(t, unconditional)

		reason, unconditional = Evaluate(now, testThresholds, id, nil, true, false)
		assert.Equal(t, types.ProblemCohortMemberIneligible, reason)
		assert.False(t, unconditional)
	})

	t.Run("quarantined is always unconditional", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().Multipooler.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED}
		reason, unconditional := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Equal(t, types.ProblemCohortMemberQuarantined, reason)
		assert.True(t, unconditional)
	})

	t.Run("self-reported ineligible is conditional", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		reason, unconditional := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Equal(t, types.ProblemCohortMemberIneligible, reason)
		assert.False(t, unconditional)
	})

	t.Run("healthy member passes", func(t *testing.T) {
		reason, _ := Evaluate(now, testThresholds, id, healthyPooler(id), true, false)
		assert.Empty(t, reason)
	})

	t.Run("healthy non-member is joinable", func(t *testing.T) {
		reason, _ := Evaluate(now, testThresholds, id, healthyPooler(id), false, false)
		assert.Empty(t, reason)
	})

	t.Run("member unhealthy past removal threshold is excluded", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().LastSeen = timestamppb.New(now.Add(-90 * time.Second))
		reason, unconditional := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Equal(t, types.ProblemCohortMemberUnhealthy, reason)
		assert.False(t, unconditional)
	})

	t.Run("member unhealthy within removal threshold is not excluded", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().LastSeen = timestamppb.New(now.Add(-5 * time.Second))
		reason, _ := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Empty(t, reason)
	})

	t.Run("non-member between readmission and removal threshold stays excluded", func(t *testing.T) {
		// 45s: below the 60s removal threshold, but above the 30s readmission
		// threshold — this is exactly the flap scenario the two-threshold
		// design prevents.
		pa := healthyPooler(id)
		pa.Health().LastSeen = timestamppb.New(now.Add(-45 * time.Second))
		reason, _ := Evaluate(now, testThresholds, id, pa, false, false)
		assert.Equal(t, types.ProblemCohortMemberUnhealthy, reason)
	})

	t.Run("non-member below readmission threshold is eligible again", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().LastSeen = timestamppb.New(now.Add(-10 * time.Second))
		reason, _ := Evaluate(now, testThresholds, id, pa, false, false)
		assert.Empty(t, reason)
	})

	t.Run("member lagging past eviction threshold is excluded", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().Status.ReplicationStatus.Lag = durationpb.New(2 * time.Minute)
		reason, unconditional := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Equal(t, types.ProblemCohortMemberLagging, reason)
		assert.False(t, unconditional)
	})

	t.Run("no lag reading is never treated as excessive lag", func(t *testing.T) {
		pa := healthyPooler(id)
		reason, _ := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Empty(t, reason)
	})

	t.Run("non-member missing an addition-only precondition is excluded", func(t *testing.T) {
		pa := healthyPooler(id)
		pa.Health().Status.ReplicationStatus.WalReceiverStatus = "stopped"
		reason, unconditional := Evaluate(now, testThresholds, id, pa, false, false)
		assert.Equal(t, types.ProblemCohortMemberIneligible, reason)
		assert.False(t, unconditional)
	})

	t.Run("member missing an addition-only precondition is not excluded", func(t *testing.T) {
		// The same fact (receiver not streaming) doesn't matter for an
		// existing member — that's ReplicaNotReplicating's job, not this one.
		pa := healthyPooler(id)
		pa.Health().Status.ReplicationStatus.WalReceiverStatus = "stopped"
		reason, _ := Evaluate(now, testThresholds, id, pa, true, false)
		assert.Empty(t, reason)
	})
}

func TestDecide(t *testing.T) {
	now := time.Now()
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "leader"}
	memberID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "member"}
	nonMemberID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "non-member"}

	atLeastN := func(n int32) *clustermetadatapb.DurabilityPolicy {
		return &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N, RequiredCount: n}
	}

	// wideCohort has enough spare capacity that removing one member and
	// losing the leader still leaves a majority satisfying AT_LEAST_2.
	wideCohort := func() *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				leaderID, memberID,
				{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "extra1"},
				{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "extra2"},
			},
			DurabilityPolicy: atLeastN(2),
		}
	}

	// tightCohort has no spare capacity: removing member and losing the
	// leader leaves nothing.
	tightCohort := func() *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			LeaderId:         leaderID,
			CohortMembers:    []*clustermetadatapb.ID{leaderID, memberID},
			DurabilityPolicy: atLeastN(1),
		}
	}

	t.Run("healthy non-member yields OpAdd", func(t *testing.T) {
		d := Decide(now, testThresholds, wideCohort(), nonMemberID, healthyPooler(nonMemberID), false)
		assert.Equal(t, OpAdd, d.Op)
		assert.NotEmpty(t, d.Description)
	})

	t.Run("ineligible member yields OpRemove when removal is safe", func(t *testing.T) {
		pa := healthyPooler(memberID)
		pa.Health().AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		d := Decide(now, testThresholds, wideCohort(), memberID, pa, false)
		assert.Equal(t, OpRemove, d.Op)
		assert.Equal(t, types.ProblemCohortMemberIneligible, d.Reason)
		assert.False(t, d.Urgent)
	})

	t.Run("ineligible member yields OpNone when removal is unsafe", func(t *testing.T) {
		pa := healthyPooler(memberID)
		pa.Health().AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		d := Decide(now, testThresholds, tightCohort(), memberID, pa, false)
		assert.Equal(t, OpNone, d.Op)
	})

	t.Run("quarantined member yields Urgent OpRemove even when unsafe", func(t *testing.T) {
		pa := healthyPooler(memberID)
		pa.Health().Multipooler.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED}
		d := Decide(now, testThresholds, tightCohort(), memberID, pa, false)
		require.Equal(t, OpRemove, d.Op)
		assert.Equal(t, types.ProblemCohortMemberQuarantined, d.Reason)
		assert.True(t, d.Urgent)
	})

	t.Run("tombstoned missing member yields Urgent OpRemove even when unsafe", func(t *testing.T) {
		d := Decide(now, testThresholds, tightCohort(), memberID, nil, true)
		require.Equal(t, OpRemove, d.Op)
		assert.True(t, d.Urgent)
	})

	t.Run("missing-without-tombstone member yields OpRemove only when safe", func(t *testing.T) {
		d := Decide(now, testThresholds, tightCohort(), memberID, nil, false)
		assert.Equal(t, OpNone, d.Op)

		d = Decide(now, testThresholds, wideCohort(), memberID, nil, false)
		assert.Equal(t, OpRemove, d.Op)
		assert.False(t, d.Urgent)
	})

	t.Run("healthy member yields OpNone", func(t *testing.T) {
		d := Decide(now, testThresholds, wideCohort(), memberID, healthyPooler(memberID), false)
		assert.Equal(t, OpNone, d.Op)
	})

	t.Run("ineligible non-member yields OpNone, not OpAdd", func(t *testing.T) {
		pa := healthyPooler(nonMemberID)
		pa.Health().AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
				Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
			},
		}
		d := Decide(now, testThresholds, wideCohort(), nonMemberID, pa, false)
		assert.Equal(t, OpNone, d.Op)
	})
}
