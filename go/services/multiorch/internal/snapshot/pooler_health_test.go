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

package snapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func withReplication(rs *multipoolermanagerdatapb.StandbyReplicationStatus) *PoolerHealthState {
	return &PoolerHealthState{Status: &multipoolermanagerdatapb.Status{ReplicationStatus: rs}}
}

func TestPoolerHealthState_NilReceiverReadsAsZero(t *testing.T) {
	var h *PoolerHealthState
	assert.Nil(t, h.GetMultipooler())
	assert.True(t, h.GetLastSeen().IsZero())
	assert.Nil(t, h.GetConsensusStatus())
	assert.True(t, h.GetLastPostgresReadyTime().IsZero())
	assert.Nil(t, h.GetAvailabilityStatus())
	assert.False(t, h.GetStreamConnected())
	assert.Nil(t, h.GetStatus())
	assert.Zero(t, h.GetStreamSnapshotsReceived())
	assert.Nil(t, h.ID())
	assert.Nil(t, h.IntendedPrimary())
	assert.False(t, h.ObservedWithin(time.Now(), time.Hour))
	assert.False(t, h.WalReplayNotPaused())
	assert.Empty(t, h.PrimaryConnInfoHost())
	assert.False(t, h.WalReceiverActive())
	assert.False(t, h.WalReceiverStreaming())
	assert.Nil(t, h.Clone())
	host, port := h.Endpoint()
	assert.Empty(t, host)
	assert.Zero(t, port)
}

func TestPoolerHealthState_CloneIsDeep(t *testing.T) {
	orig := &PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: podID("p1"), Hostname: "h"},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{IntendedPrimary: &clustermetadatapb.IntendedPrimary{RewindReady: false}},
		LastSeen:        time.Now(),
		StreamConnected: true,
	}
	clone := orig.Clone()

	clone.ConsensusStatus.IntendedPrimary.RewindReady = true
	clone.Multipooler.Hostname = "changed"
	clone.StreamConnected = false

	assert.False(t, orig.ConsensusStatus.IntendedPrimary.RewindReady, "editing the clone's nested message must not reach the original")
	assert.Equal(t, "h", orig.Multipooler.Hostname)
	assert.True(t, orig.StreamConnected)
}

func TestPoolerHealthState_IDAndEndpoint(t *testing.T) {
	h := &PoolerHealthState{Multipooler: &clustermetadatapb.Multipooler{
		Id: podID("p1"), Hostname: "host-1", PortMap: map[string]int32{"postgres": 5432},
	}}
	assert.Equal(t, "p1", h.ID().GetName())
	host, port := h.Endpoint()
	assert.Equal(t, "host-1", host)
	assert.Equal(t, int32(5432), port)
}

func TestPoolerHealthState_IntendedPrimaryPlaceholderReadsAsNil(t *testing.T) {
	real := &clustermetadatapb.IntendedPrimary{Primary: &clustermetadatapb.PoolerAddress{Host: "leader"}, Position: &clustermetadatapb.RulePosition{
		Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(3)},
	}}
	h := &PoolerHealthState{ConsensusStatus: &clustermetadatapb.ConsensusStatus{IntendedPrimary: real}}
	require.NotNil(t, h.IntendedPrimary())
	assert.Equal(t, "leader", h.IntendedPrimary().GetPrimary().GetHost())

	unset := &PoolerHealthState{ConsensusStatus: &clustermetadatapb.ConsensusStatus{}}
	assert.Nil(t, unset.IntendedPrimary())
}

func TestPoolerHealthState_ObservedWithin(t *testing.T) {
	now := time.Unix(1700000000, 0)
	seen := func(age time.Duration) *PoolerHealthState {
		return &PoolerHealthState{LastSeen: now.Add(-age)}
	}
	assert.True(t, seen(5*time.Second).ObservedWithin(now, 10*time.Second))
	assert.True(t, seen(10*time.Second).ObservedWithin(now, 10*time.Second), "exactly at the limit is still fresh")
	assert.False(t, seen(11*time.Second).ObservedWithin(now, 10*time.Second))
	assert.False(t, (&PoolerHealthState{}).ObservedWithin(now, time.Hour), "never observed is not fresh")
}

func TestPoolerHealthState_WalReceiver(t *testing.T) {
	tests := []struct {
		name                     string
		rs                       *multipoolermanagerdatapb.StandbyReplicationStatus
		active, streaming, notPd bool
	}{
		{name: "no replication status"},
		{name: "streaming with WAL", rs: &multipoolermanagerdatapb.StandbyReplicationStatus{WalReceiverStatus: "streaming", LastReceiveLsn: "0/1"}, active: true, streaming: true, notPd: true},
		{name: "streaming before any WAL arrived", rs: &multipoolermanagerdatapb.StandbyReplicationStatus{WalReceiverStatus: "streaming"}, active: true, notPd: true},
		{name: "waiting is healthy", rs: &multipoolermanagerdatapb.StandbyReplicationStatus{WalReceiverStatus: "waiting"}, active: true, streaming: true, notPd: true},
		{name: "receiver not running", rs: &multipoolermanagerdatapb.StandbyReplicationStatus{WalReceiverStatus: ""}, notPd: true},
		{name: "replay paused", rs: &multipoolermanagerdatapb.StandbyReplicationStatus{WalReceiverStatus: "streaming", LastReceiveLsn: "0/1", IsWalReplayPaused: true}, active: true, streaming: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := withReplication(tt.rs)
			assert.Equal(t, tt.active, h.WalReceiverActive(), "active")
			assert.Equal(t, tt.streaming, h.WalReceiverStreaming(), "streaming")
			assert.Equal(t, tt.notPd, h.WalReplayNotPaused() && tt.rs != nil, "replay not paused")
		})
	}
}

func TestPoolerHealthState_PrimaryConnInfoHost(t *testing.T) {
	h := withReplication(&multipoolermanagerdatapb.StandbyReplicationStatus{PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host"}})
	assert.Equal(t, "leader-host", h.PrimaryConnInfoHost())
	assert.Empty(t, withReplication(&multipoolermanagerdatapb.StandbyReplicationStatus{}).PrimaryConnInfoHost())
}

func TestPoolerHealthState_PostgresReadyWithin(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ready := func(age time.Duration) *PoolerHealthState {
		return &PoolerHealthState{LastPostgresReadyTime: now.Add(-age)}
	}
	assert.True(t, ready(5*time.Second).PostgresReadyWithin(now, 10*time.Second))
	assert.True(t, ready(10*time.Second).PostgresReadyWithin(now, 10*time.Second), "exactly at the limit counts")
	assert.False(t, ready(11*time.Second).PostgresReadyWithin(now, 10*time.Second))
	assert.False(t, (&PoolerHealthState{}).PostgresReadyWithin(now, time.Hour), "never ready is not within any age")
	assert.False(t, (*PoolerHealthState)(nil).PostgresReadyWithin(now, time.Hour))
}
