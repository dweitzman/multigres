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

package dstsim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
)

// TestTraceBuffer tests the circular buffer implementation
func TestTraceBuffer(t *testing.T) {
	t.Run("AppendWithinCapacity", func(t *testing.T) {
		tb := dstsim.NewTraceBuffer[int, string, int](5)

		// Add 3 items (less than capacity)
		for i := 0; i < 3; i++ {
			tb.Append(dstsim.TickTrace[int, string, int]{Tick: int64(i)})
		}

		require.Equal(t, 3, tb.Len(), "should have 3 items")

		all := tb.All()
		require.Len(t, all, 3, "All() should return 3 items")
		require.Equal(t, int64(0), all[0].Tick, "first item")
		require.Equal(t, int64(2), all[2].Tick, "last item")
	})

	t.Run("AppendExceedsCapacity", func(t *testing.T) {
		tb := dstsim.NewTraceBuffer[int, string, int](3)

		// Add 5 items (more than capacity of 3)
		for i := 0; i < 5; i++ {
			tb.Append(dstsim.TickTrace[int, string, int]{Tick: int64(i)})
		}

		require.Equal(t, 3, tb.Len(), "should have exactly 3 items (capacity)")

		all := tb.All()
		require.Len(t, all, 3, "All() should return 3 items")
		// Should have items 2, 3, 4 (oldest 0, 1 were evicted)
		require.Equal(t, int64(2), all[0].Tick, "should have oldest retained item")
		require.Equal(t, int64(3), all[1].Tick, "should have middle item")
		require.Equal(t, int64(4), all[2].Tick, "should have newest item")
	})

	t.Run("Recent", func(t *testing.T) {
		tb := dstsim.NewTraceBuffer[int, string, int](10)

		// Add 7 items
		for i := 0; i < 7; i++ {
			tb.Append(dstsim.TickTrace[int, string, int]{Tick: int64(i)})
		}

		recent := tb.Recent(3)
		require.Len(t, recent, 3, "should return last 3 items")
		require.Equal(t, int64(4), recent[0].Tick, "oldest of recent")
		require.Equal(t, int64(6), recent[2].Tick, "newest")
	})

	t.Run("RecentAll", func(t *testing.T) {
		tb := dstsim.NewTraceBuffer[int, string, int](10)

		for i := 0; i < 5; i++ {
			tb.Append(dstsim.TickTrace[int, string, int]{Tick: int64(i)})
		}

		// Request more than available
		recent := tb.Recent(10)
		require.Len(t, recent, 5, "should return all available items")
	})

	t.Run("CircularWrapping", func(t *testing.T) {
		tb := dstsim.NewTraceBuffer[int, string, int](3)

		// Add 10 items to force multiple wraps
		for i := 0; i < 10; i++ {
			tb.Append(dstsim.TickTrace[int, string, int]{Tick: int64(i)})
		}

		all := tb.All()
		require.Len(t, all, 3, "should still have capacity items")
		// Should have last 3: 7, 8, 9
		require.Equal(t, int64(7), all[0].Tick)
		require.Equal(t, int64(8), all[1].Tick)
		require.Equal(t, int64(9), all[2].Tick)
	})
}
