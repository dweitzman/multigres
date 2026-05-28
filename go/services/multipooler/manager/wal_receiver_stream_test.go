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

package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateWalReceiverStreamState exercises the state machine that
// surfaces wal_receiver_not_streaming_since in StandbyReplicationStatus.
// The state lives on a MultiPoolerManager; tests build an otherwise-empty
// manager and drive the method directly.
func TestUpdateWalReceiverStreamState(t *testing.T) {
	t.Run("primary_conninfo unset returns zero and resets state", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		// Seed an active "stuck since" so we can prove the reset clears it.
		pm.walReceiverStreamState = walReceiverStreamState{
			initialized:        true,
			currentlyStreaming: false,
			notStreamingSince:  time.Now().Add(-time.Minute),
		}
		got := pm.updateWalReceiverStreamState("starting", false /*primaryConfigured*/)
		assert.True(t, got.IsZero(), "no replication configured should return zero time")
		assert.Equal(t, walReceiverStreamState{}, pm.walReceiverStreamState, "state should reset to zero value")
	})

	t.Run("first observation while streaming initializes without stamping", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		got := pm.updateWalReceiverStreamState("streaming", true)
		assert.True(t, got.IsZero(), "currently streaming should not return a since timestamp")
		assert.True(t, pm.walReceiverStreamState.initialized)
		assert.True(t, pm.walReceiverStreamState.currentlyStreaming)
		assert.True(t, pm.walReceiverStreamState.notStreamingSince.IsZero())
	})

	t.Run("first observation while not streaming stamps the timestamp", func(t *testing.T) {
		// Covers the bootstrap case: a freshly-started multipooler whose
		// WAL receiver hasn't yet connected. Caller is expected to apply
		// a duration threshold (e.g. >30s) before alerting.
		pm := &MultiPoolerManager{}
		before := time.Now()
		got := pm.updateWalReceiverStreamState("starting", true)
		after := time.Now()

		require.False(t, got.IsZero())
		assert.True(t, !got.Before(before) && !got.After(after), "since timestamp should be now()")
		assert.True(t, pm.walReceiverStreamState.initialized)
		assert.False(t, pm.walReceiverStreamState.currentlyStreaming)
	})

	t.Run("transition streaming -> not-streaming stamps timestamp", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		_ = pm.updateWalReceiverStreamState("streaming", true)

		before := time.Now()
		got := pm.updateWalReceiverStreamState("waiting", true)
		after := time.Now()

		require.False(t, got.IsZero())
		assert.True(t, !got.Before(before) && !got.After(after))
		assert.False(t, pm.walReceiverStreamState.currentlyStreaming)
	})

	t.Run("subsequent non-streaming observations keep the original timestamp", func(t *testing.T) {
		// Sticky behavior: the timestamp marks when streaming was lost,
		// not the latest poll time.
		pm := &MultiPoolerManager{}
		_ = pm.updateWalReceiverStreamState("streaming", true)
		first := pm.updateWalReceiverStreamState("waiting", true)
		require.False(t, first.IsZero())

		time.Sleep(2 * time.Millisecond)
		second := pm.updateWalReceiverStreamState("starting", true)
		third := pm.updateWalReceiverStreamState("", true)

		assert.Equal(t, first, second, "second non-streaming observation should not move the timestamp")
		assert.Equal(t, first, third, "third non-streaming observation should not move the timestamp")
	})

	t.Run("transition not-streaming -> streaming clears the timestamp", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		_ = pm.updateWalReceiverStreamState("streaming", true)
		_ = pm.updateWalReceiverStreamState("waiting", true)

		got := pm.updateWalReceiverStreamState("streaming", true)
		assert.True(t, got.IsZero())
		assert.True(t, pm.walReceiverStreamState.currentlyStreaming)
		assert.True(t, pm.walReceiverStreamState.notStreamingSince.IsZero())
	})

	t.Run("re-entering non-streaming after clearing stamps a fresh timestamp", func(t *testing.T) {
		// Clears + re-stamps. Confirms there's no leftover state from the
		// previous non-streaming window.
		pm := &MultiPoolerManager{}
		_ = pm.updateWalReceiverStreamState("streaming", true)
		first := pm.updateWalReceiverStreamState("waiting", true)
		_ = pm.updateWalReceiverStreamState("streaming", true)

		time.Sleep(2 * time.Millisecond)
		second := pm.updateWalReceiverStreamState("waiting", true)
		require.False(t, second.IsZero())
		assert.True(t, second.After(first), "fresh non-streaming epoch should produce a newer timestamp")
	})

	t.Run("primary_conninfo cleared in the middle resets state", func(t *testing.T) {
		// Mirrors the production path where a pooler's primary_conninfo
		// is cleared (e.g. promotion). We should drop the prior epoch
		// entirely so a future re-standby starts fresh.
		pm := &MultiPoolerManager{}
		_ = pm.updateWalReceiverStreamState("streaming", true)
		_ = pm.updateWalReceiverStreamState("waiting", true)

		got := pm.updateWalReceiverStreamState("", false /*primaryConfigured*/)
		assert.True(t, got.IsZero())
		assert.Equal(t, walReceiverStreamState{}, pm.walReceiverStreamState)
	})

	t.Run("empty wal_receiver_status counts as not-streaming", func(t *testing.T) {
		// pg_stat_wal_receiver returns no row (empty string) when the
		// receiver is fully stopped — that's a stuck condition for a
		// standby that has primary_conninfo set.
		pm := &MultiPoolerManager{}
		got := pm.updateWalReceiverStreamState("", true)
		require.False(t, got.IsZero())
	})
}
