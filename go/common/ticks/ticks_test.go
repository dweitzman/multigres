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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDurationToTicks(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected TickDuration
	}{
		{"exact tick", 100 * time.Millisecond, 1},
		{"rounds up", 150 * time.Millisecond, 2},
		{"tiny duration", 1 * time.Millisecond, 1},
		{"multiple ticks", 500 * time.Millisecond, 5},
		{"just under", 99 * time.Millisecond, 1},
		{"just over", 101 * time.Millisecond, 2},
		{"negative", -100 * time.Millisecond, 0},
		{"zero", 0, 0},
		{"30 seconds", 30 * time.Second, 300},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DurationToTicks(tt.duration)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTickAdd(t *testing.T) {
	tick := Tick(100)
	duration := TickDuration(50)

	result := tick.Add(duration)

	assert.Equal(t, Tick(150), result)
}

func TestTickSub(t *testing.T) {
	tick1 := Tick(200)
	tick2 := Tick(150)

	result := tick1.Sub(tick2)

	assert.Equal(t, TickDuration(50), result)
}

func TestTickBefore(t *testing.T) {
	tick1 := Tick(100)
	tick2 := Tick(200)

	assert.True(t, tick1.Before(tick2))
	assert.False(t, tick2.Before(tick1))
	assert.False(t, tick1.Before(tick1))
}

func TestTickAfter(t *testing.T) {
	tick1 := Tick(100)
	tick2 := Tick(200)

	assert.True(t, tick2.After(tick1))
	assert.False(t, tick1.After(tick2))
	assert.False(t, tick1.After(tick1))
}

func TestTickConversions(t *testing.T) {
	// ToTick and Int64 should be inverses
	original := int64(12345)
	tick := ToTick(original)
	assert.Equal(t, original, tick.Int64())
}

func TestTickArithmetic(t *testing.T) {
	// Test a typical use case: deadline calculation
	currentTick := Tick(1000)
	timeout := DurationToTicks(5 * time.Second) // 50 ticks
	deadline := currentTick.Add(timeout)

	assert.Equal(t, Tick(1050), deadline)
	assert.True(t, currentTick.Before(deadline))
	assert.Equal(t, timeout, deadline.Sub(currentTick))
}
