// Copyright 2025 Supabase, Inc.
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

package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runRetryUntilAttempt runs the retry loop until maxAttempts, then succeeds.
func runRetryUntilAttempt(t *testing.T, r *Retryer, maxAttempts int) int {
	t.Helper()
	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		// During execution, Attempts() returns the current attempt number
		assert.Equal(t, attempt, r.Attempt())

		attempts++
		if attempts > maxAttempts {
			return nil
		}
		return errors.New("error")
	})
	require.NoError(t, err, "retry should eventually succeed")
	return attempts
}

func TestNew_CreatesRetryer(t *testing.T) {
	r := New(500*time.Millisecond, time.Minute)
	assert.Equal(t, 500*time.Millisecond, r.cfg.MinDelay)
	assert.Equal(t, time.Minute, r.cfg.MaxDelay)
	assert.NotNil(t, r.cfg.backoff, "backoff strategy should be set")
	assert.IsType(t, &exponentialFullJitterBackoff{}, r.cfg.backoff, "should use exponential full jitter by default")
}

func TestRetryer_Success_FirstAttempt(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		return nil // Success on first attempt
	})

	assert.NoError(t, err, "should succeed on first attempt")
	assert.Equal(t, 1, attempts)
	assert.Equal(t, 0, r.Attempt())
	assert.Empty(t, ft.delays, "no delays should occur on immediate success")
}

func TestRetryer_Success_AfterRetries(t *testing.T) {
	delays := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}
	r, ft := newRetryerWithFakeBackoff(delays)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		// Verify delays happen after each operation (not before)
		assert.Equal(t, attempts, len(ft.delays), "delay count should match completed attempts")
		attempts++
		if attempt < 3 {
			return errors.New("temporary error")
		}
		return nil // Success on 4th attempt (attempt index 3)
	})

	assert.NoError(t, err)
	assert.Equal(t, 4, attempts)
	assert.Equal(t, 3, r.Attempt())
	// Should have 3 delays: after attempts 0, 1, 2
	require.Len(t, ft.delays, 3, "should have 3 delays (after each failed attempt)")
	// Verify the backoff strategy was used
	assert.Equal(t, delays, ft.delays)
}

func TestRetryer_AppliesBackoffDelays(t *testing.T) {
	// Define custom backoff delays to verify retry orchestration uses them
	customDelays := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		100 * time.Millisecond,
	}
	r, ft := newRetryerWithFakeBackoff(customDelays)

	attempts := runRetryUntilAttempt(t, r, 5)

	assert.Equal(t, 6, attempts)
	require.Len(t, ft.delays, 5)
	// Verify the retry logic correctly applied the backoff delays
	assert.Equal(t, customDelays, ft.delays, "delays should match backoff strategy output")
}

func TestRetryer_DelayBeforeAttempt(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	r, ft := newRetryerWithFakeBackoff(delays, WithDelayBeforeAttempt())

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		// Make sure delay happened prior to calling this operation.
		assert.Equal(t, attempts, len(ft.delays))
		if attempts >= 3 {
			return nil
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
	require.Len(t, ft.delays, 3)
	// Verify delays occurred before each attempt
	assert.Equal(t, delays, ft.delays)
}

func TestRetryer_ContextCancellation(t *testing.T) {
	// Use real timer since context cancellation requires real timing to work correctly
	// Disable jitter for predictable timing
	r := New(10*time.Millisecond, time.Minute, withBackoff(newExponentialBackoffNoJitter(10*time.Millisecond, time.Minute)))

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	err := r.Do(ctx, func(attempt int) error {
		attempts++
		if attempt == 2 {
			cancel() // Cancel after 3rd attempt
		}
		return errors.New("error")
	})

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 3, attempts)
}

func TestRetryer_ContextTimeoutBeforeAttempt(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond}
	r, ft := newRetryerWithFakeBackoff(delays)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	cancel()

	attempts := 0
	err := r.Do(ctx, func(attempt int) error {
		attempts++
		return nil
	})

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, attempts, "should not execute operation if context times out during skip delay")
	assert.Len(t, ft.delays, 0)
}

func TestRetryer_ContextTimeoutDuringBackoff(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond}
	r, ft := newRetryerWithFakeBackoff(delays)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	attempts := 0
	err := r.Do(ctx, func(attempt int) error {
		cancel()
		attempts++
		return errors.New("error")
	})

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, attempts, 1)
	assert.Len(t, ft.delays, 0)
}

// Sentinel error tests

func TestRetryer_NonRetryableStopsRetries(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	r, ft := newRetryerWithFakeBackoff(delays)

	attempts := 0
	underlyingErr := errors.New("unrecoverable error")

	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempt == 2 {
			// NonRetryable on 3rd attempt
			return NonRetryableError(underlyingErr)
		}
		return errors.New("temporary error")
	})

	// Should return the unwrapped underlying error
	assert.Error(t, err)
	assert.Equal(t, underlyingErr, err)
	assert.Equal(t, 3, attempts, "should stop immediately after abort")
	// Should have 2 delays before abort
	assert.Len(t, ft.delays, 2)
	assert.Equal(t, delays, ft.delays)
}

func TestRetryer_NonRetryableWithNilError(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond}
	r, ft := newRetryerWithFakeBackoff(delays)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempt == 1 {
			return NonRetryableError(nil) // nil error returns nil
		}
		return errors.New("error")
	})

	// Should return nil when NonRetryableError wraps nil
	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)
	assert.Len(t, ft.delays, 1)
	assert.Equal(t, delays, ft.delays)
}

// Config validation tests

func TestNew_PanicsOnInvalidConfig(t *testing.T) {
	tests := []struct {
		name     string
		minDelay time.Duration
		maxDelay time.Duration
		opts     []Option
		panics   bool
	}{
		{
			name:     "negative MinDelay",
			minDelay: -1 * time.Second,
			maxDelay: time.Minute,
			panics:   true,
		},
		{
			name:     "zero MinDelay",
			minDelay: 0,
			maxDelay: time.Minute,
			panics:   true,
		},
		{
			name:     "negative MaxDelay",
			minDelay: time.Second,
			maxDelay: -1 * time.Minute,
			panics:   true,
		},
		{
			name:     "zero MaxDelay",
			minDelay: time.Second,
			maxDelay: 0,
			panics:   true,
		},
		{
			name:     "MinDelay greater than MaxDelay",
			minDelay: time.Minute,
			maxDelay: time.Second,
			panics:   true,
		},
		{
			name:     "valid config with DelayBeforeAttempt",
			minDelay: time.Second,
			maxDelay: time.Minute,
			opts:     []Option{WithDelayBeforeAttempt()},
			panics:   false,
		},
		{
			name:     "valid basic config",
			minDelay: time.Second,
			maxDelay: time.Minute,
			panics:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.panics {
				assert.Panics(t, func() {
					New(tt.minDelay, tt.maxDelay, tt.opts...)
				})
			} else {
				assert.NotPanics(t, func() {
					r := New(tt.minDelay, tt.maxDelay, tt.opts...)
					assert.NotNil(t, r)
				})
			}
		})
	}
}
