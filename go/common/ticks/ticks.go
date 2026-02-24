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

package ticks

import "time"

// Tick represents a point in logical time.
// In production, ticks advance with wall-clock time (1 tick = TickResolution).
// In tests, ticks can be controlled explicitly for deterministic behavior.
type Tick int64

// TickDuration represents a span of logical time (like time.Duration but in ticks).
type TickDuration int64

// TickResolution defines how much real time corresponds to one tick in production.
// Tests can use different tick rates or control ticks explicitly.
const TickResolution = 100 * time.Millisecond

// DurationToTicks converts a time.Duration to ticks, rounding up to ensure
// we never underestimate timeouts.
//
// Examples:
//   - 100ms → 1 tick
//   - 150ms → 2 ticks (rounds up)
//   - 1ms → 1 tick (even tiny durations get at least 1 tick)
//   - negative durations → 0 ticks
func DurationToTicks(d time.Duration) TickDuration {
	if d < 0 {
		return 0
	}
	// Round up: (d + TickResolution - 1) / TickResolution
	ticks := (d + TickResolution - 1) / TickResolution
	return TickDuration(ticks)
}

// Add adds a duration to a tick, producing a new tick.
// Tick + TickDuration = Tick
func (t Tick) Add(d TickDuration) Tick {
	return Tick(int64(t) + int64(d))
}

// Sub subtracts another tick from this tick, producing a duration.
// Tick - Tick = TickDuration
func (t Tick) Sub(other Tick) TickDuration {
	return TickDuration(int64(t) - int64(other))
}

// Before checks if this tick comes before another tick.
func (t Tick) Before(other Tick) bool {
	return t < other
}

// After checks if this tick comes after another tick.
func (t Tick) After(other Tick) bool {
	return t > other
}

// Int64 returns the tick as an int64 (for use with DST simulator or serialization).
func (t Tick) Int64() int64 {
	return int64(t)
}

// ToTick converts an int64 to a Tick (for literals or deserialization).
func ToTick(t int64) Tick {
	return Tick(t)
}
