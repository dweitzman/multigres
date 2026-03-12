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

package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedsLeaderFailover(t *testing.T) {
	const (
		primary     NodeID = "node-1"
		replica     NodeID = "node-2"
		currentTick int64  = 1000
	)

	term := &Term{
		Seq:     1,
		Primary: primary,
		Members: []CohortMember{{ID: primary}, {ID: replica}},
		Policy:  AtLeastPolicy(2),
	}

	healthyHealth := NodeHealth{
		PostgresStatus: PostgresRunning,
		LastHeardTick:  currentTick,
	}

	t.Run("healthy primary", func(t *testing.T) {
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,
			NodeHealth:        map[NodeID]NodeHealth{primary: healthyHealth},
		}
		assert.False(t, NeedsLeaderFailover(status))
	})

	t.Run("primary postgres stopped", func(t *testing.T) {
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,
			NodeHealth: map[NodeID]NodeHealth{
				primary: {PostgresStatus: PostgresStopped, LastHeardTick: currentTick},
			},
		}
		assert.True(t, NeedsLeaderFailover(status))
	})

	t.Run("primary silent exactly at timeout boundary — still healthy", func(t *testing.T) {
		lastHeard := currentTick - HealthTimeoutTicks
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,
			NodeHealth: map[NodeID]NodeHealth{
				primary: {PostgresStatus: PostgresRunning, LastHeardTick: lastHeard},
			},
		}
		assert.False(t, NeedsLeaderFailover(status), "exactly at boundary should still be healthy")
	})

	t.Run("primary silent one tick past timeout boundary — unhealthy", func(t *testing.T) {
		lastHeard := currentTick - HealthTimeoutTicks - 1
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,
			NodeHealth: map[NodeID]NodeHealth{
				primary: {PostgresStatus: PostgresRunning, LastHeardTick: lastHeard},
			},
		}
		assert.True(t, NeedsLeaderFailover(status), "one tick past boundary should be unhealthy")
	})

	t.Run("primary never heard from (LastHeardTick=0) — healthy", func(t *testing.T) {
		// LastHeardTick=0 means the coordinator has never received a status update;
		// the staleness check is skipped and the node is not considered timed out.
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,
			NodeHealth: map[NodeID]NodeHealth{
				primary: {PostgresStatus: PostgresRunning, LastHeardTick: 0},
			},
		}
		assert.False(t, NeedsLeaderFailover(status))
	})

	t.Run("primary not in NodeHealth map — treat as unreachable", func(t *testing.T) {
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,
			NodeHealth:        map[NodeID]NodeHealth{}, // primary absent
		}
		assert.True(t, NeedsLeaderFailover(status))
	})

	t.Run("no cluster state known — cannot recommend failover", func(t *testing.T) {
		status := ShardStatus{
			Tick:       currentTick,
			NodeHealth: map[NodeID]NodeHealth{},
		}
		assert.False(t, NeedsLeaderFailover(status))
	})

	t.Run("only HighestSeenTerm, no quorum term — uses seen term's primary", func(t *testing.T) {
		status := ShardStatus{
			Tick:            currentTick,
			HighestSeenTerm: term,
			NodeHealth: map[NodeID]NodeHealth{
				primary: {PostgresStatus: PostgresStopped, LastHeardTick: currentTick},
			},
		}
		assert.True(t, NeedsLeaderFailover(status), "seen term's primary is stopped")
	})

	t.Run("quorum term primary used over seen term primary when both present", func(t *testing.T) {
		seenTerm := &Term{Seq: 2, Primary: replica} // seen term has replica as primary
		status := ShardStatus{
			Tick:              currentTick,
			HighestQuorumTerm: term,     // quorum term has `primary` as primary
			HighestSeenTerm:   seenTerm, // seen term has `replica` as primary
			NodeHealth: map[NodeID]NodeHealth{
				primary: healthyHealth,                                                 // quorum primary is healthy
				replica: {PostgresStatus: PostgresStopped, LastHeardTick: currentTick}, // seen primary is stopped
			},
		}
		// Should check quorum term's primary (healthy), not seen term's primary (stopped).
		assert.False(t, NeedsLeaderFailover(status))
	})
}
