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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSeed represents a pair of seed values for deterministic random number generation.
type testSeed struct {
	s1, s2 uint64
}

// Test seeds for deterministic jitter testing.
// Each seed produces a specific random sequence for reproducible test behavior.
var (
	// seed1x1 produces Float64() ≈ 0.340286, giving ~34% of max delay
	seed1x1 = testSeed{1, 1}
	// seed2x2 produces Float64() ≈ 0.078291, giving ~8% of max delay (low value)
	seed2x2 = testSeed{2, 2}
	// seed42 produces a varied random sequence for variation testing
	seed42 = testSeed{42, 42}
	// Additional seeds for range testing
	seed10x20   = testSeed{10, 20}
	seed100x200 = testSeed{100, 200}
)

// Jitter fractions produced by each seed's first Float64() call.
// These can be multiplied by any duration to get the expected jittered delay.
const (
	// seed1x1 produces Float64() that results in 34.028597ms when applied to 100ms
	jitterFractionSeed1x1 = 0.34028597
	// seed2x2 produces Float64() that results in 7.829106ms when applied to 100ms
	jitterFractionSeed2x2 = 0.07829106
)

// jitter calculates the expected jittered delay for a given duration and jitter fraction.
func jitter(d time.Duration, fraction float64) time.Duration {
	return time.Duration(fraction * float64(d))
}

// fakeTimer is a deterministic timer for testing that completes immediately.
type fakeTimer struct {
	delays []time.Duration
}

func (f *fakeTimer) After(d time.Duration) <-chan time.Time {
	f.delays = append(f.delays, d)
	ch := make(chan time.Time, 1)
	ch <- time.Now() // Complete immediately
	return ch
}

// newRetryerWithFakeTimer creates a retryer with a fake timer for testing.
// Automatically disables jitter for deterministic tests.
func newRetryerWithFakeTimer(minDelay, maxDelay time.Duration, opts ...Option) (*Retryer, *fakeTimer) {
	// Prepend withDisableJitter to make tests deterministic by default
	allOpts := append([]Option{withDisableJitter()}, opts...)
	r := New(minDelay, maxDelay, allOpts...)
	ft := &fakeTimer{}
	r.timer = ft
	return r, ft
}

// newRetryerWithFakeTimerAndJitter creates a retryer with fake timer but WITH jitter enabled.
// Use this for tests that specifically test jitter behavior.
// Takes a testSeed parameter for deterministic jitter in tests.
func newRetryerWithFakeTimerAndJitter(minDelay, maxDelay time.Duration, seed testSeed, opts ...Option) (*Retryer, *fakeTimer) {
	r := New(minDelay, maxDelay, opts...)
	// Use provided seed for deterministic testing
	r.rng = rand.New(rand.NewPCG(seed.s1, seed.s2))
	ft := &fakeTimer{}
	r.timer = ft
	return r, ft
}

// Helper functions to reduce duplicate test setup code

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
	assert.False(t, r.cfg.disableJitter, "jitter should be enabled by default")
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
	r, ft := newRetryerWithFakeTimer(
		5*time.Millisecond,
		20*time.Millisecond,
	)

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
}

func TestRetryer_ExponentialBackoff(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		10*time.Millisecond,
		100*time.Millisecond,
	)

	attempts := runRetryUntilAttempt(t, r, 5)

	assert.Equal(t, 6, attempts)
	require.Len(t, ft.delays, 5)

	// Verify exponential growth: 10, 20, 40, 80, 100 (capped)
	assert.Equal(t, 10*time.Millisecond, ft.delays[0], "delay 0: 10 * 2^0")
	assert.Equal(t, 20*time.Millisecond, ft.delays[1], "delay 1: 10 * 2^1")
	assert.Equal(t, 40*time.Millisecond, ft.delays[2], "delay 2: 10 * 2^2")
	assert.Equal(t, 80*time.Millisecond, ft.delays[3], "delay 3: 10 * 2^3")
	assert.Equal(t, 100*time.Millisecond, ft.delays[4], "delay 4: capped at MaxDelay")
}

func TestRetryer_DelayBeforeAttempt(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		10*time.Millisecond,
		time.Minute,
		WithDelayBeforeAttempt(),
	)

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
	assert.Equal(t, 10*time.Millisecond, ft.delays[0]) // before attempt 0 (MinDelay)
	assert.Equal(t, 20*time.Millisecond, ft.delays[1]) // before attempt 1 (MinDelay * 2^1)
	assert.Equal(t, 40*time.Millisecond, ft.delays[2]) // before attempt 2 (MinDelay * 2^2)
}

func TestRetryer_ContextCancellation(t *testing.T) {
	// Use real timer since context cancellation requires real timing to work correctly
	// Disable jitter for predictable timing
	r := New(10*time.Millisecond, time.Minute, withDisableJitter())

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

func TestCalculateDelay(t *testing.T) {
	tests := []struct {
		name       string
		minDelay   time.Duration
		maxDelay   time.Duration
		attempt    int
		withJitter bool
		expected   time.Duration
	}{
		{
			name:       "first attempt no jitter",
			minDelay:   10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    0,
			withJitter: false,
			expected:   10 * time.Millisecond,
		},
		{
			name:       "second attempt no jitter",
			minDelay:   10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    1,
			withJitter: false,
			expected:   20 * time.Millisecond,
		},
		{
			name:       "third attempt no jitter",
			minDelay:   10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    2,
			withJitter: false,
			expected:   40 * time.Millisecond,
		},
		{
			name:       "with max delay cap",
			minDelay:   10 * time.Millisecond,
			maxDelay:   30 * time.Millisecond,
			attempt:    5,
			withJitter: false,
			expected:   30 * time.Millisecond,
		},
		{
			name:       "with full jitter",
			minDelay:   100 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    0,
			withJitter: true,
			expected:   jitter(100*time.Millisecond, jitterFractionSeed1x1),
		},
		{
			name:       "exponential growth",
			minDelay:   5 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    4,
			withJitter: false,
			expected:   80 * time.Millisecond, // 5 * 2^4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r *Retryer
			if tt.withJitter {
				r, _ = newRetryerWithFakeTimerAndJitter(tt.minDelay, tt.maxDelay, seed1x1)
			} else {
				r, _ = newRetryerWithFakeTimer(tt.minDelay, tt.maxDelay)
			}
			r.attempt = tt.attempt

			delay := r.calculateDelay()
			assert.Equal(t, tt.expected, delay)
		})
	}
}

func TestRetryer_DelayAfterAttempt(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		// Verify delay happens AFTER each operation (default behavior)
		// On first call: 0 delays so far. On second call: 1 delay. Etc.
		assert.Equal(t, attempts, len(ft.delays))
		attempts++
		if attempts >= 3 {
			return nil
		}
		return errors.New("error")
	})

	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
	require.Len(t, ft.delays, 2)                       // 2 delays after 3 attempts (no delay after success)
	assert.Equal(t, 10*time.Millisecond, ft.delays[0]) // after attempt 0
	assert.Equal(t, 20*time.Millisecond, ft.delays[1]) // after attempt 1
}

func TestRetryer_ContextTimeoutDuringSkip(t *testing.T) {
	// Use real timer for this test since we need actual timing
	// Disable jitter for predictable timing
	r := New(
		100*time.Millisecond,
		time.Minute,
		WithDelayBeforeAttempt(),
		withDisableJitter(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	attempts := 0
	err := r.Do(ctx, func(attempt int) error {
		attempts++
		return nil
	})

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 0, attempts, "should not execute operation if context times out during skip delay")
}

func TestRetryer_ContextTimeoutDuringBackoff(t *testing.T) {
	// Use real timer for this test
	// Disable jitter for predictable timing
	r := New(50*time.Millisecond, time.Minute, withDisableJitter())

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	attempts := 0
	err := r.Do(ctx, func(attempt int) error {
		attempts++
		return errors.New("error")
	})

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	// Should get at least 1-2 attempts before timeout
	assert.GreaterOrEqual(t, attempts, 1)
}

// Sentinel error tests

func TestRetryer_NonRetryableStopsRetries(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

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
}

func TestRetryer_NonRetryableWithNilError(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

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

func TestCalculateDelay_ExtremeAttemptCounts(t *testing.T) {
	tests := []struct {
		name          string
		minDelay      time.Duration
		maxDelay      time.Duration
		attempts      int
		expectedDelay time.Duration
	}{
		{
			name:          "attempt 100 with 1s min, 1m max - should cap at max",
			minDelay:      time.Second,
			maxDelay:      time.Minute,
			attempts:      100,
			expectedDelay: time.Minute,
		},
		{
			name:          "attempt 1000 with 1s min, 1m max - should cap at max",
			minDelay:      time.Second,
			maxDelay:      time.Minute,
			attempts:      1000,
			expectedDelay: time.Minute,
		},
		{
			name:          "attempt 50 with 1ms min, 1h max - should cap due to overflow protection",
			minDelay:      time.Millisecond,
			maxDelay:      time.Hour,
			attempts:      50,
			expectedDelay: time.Hour, // 2^50 would overflow, so caps at MaxDelay
		},
		{
			name:          "attempt 10 with 1s min, 1h max - no overflow, precise calculation",
			minDelay:      time.Second,
			maxDelay:      time.Hour,
			attempts:      10,
			expectedDelay: 1024 * time.Second, // 2^10 = 1024
		},
		{
			name:          "attempt 63 triggers overflow protection cap",
			minDelay:      time.Second,
			maxDelay:      time.Hour,
			attempts:      63, // Above the 62-attempt cap in calculateDelay
			expectedDelay: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := newRetryerWithFakeTimer(
				tt.minDelay,
				tt.maxDelay,
			)
			r.attempt = tt.attempts

			// Should not panic even with extreme values
			assert.NotPanics(t, func() {
				_ = r.calculateDelay()
			})

			delay := r.calculateDelay()

			// Should match expected delay
			assert.Equal(t, tt.expectedDelay, delay)

			// Delay should never be negative (overflow indicator)
			assert.GreaterOrEqual(t, delay, time.Duration(0))

			// Delay should never exceed MaxDelay
			assert.LessOrEqual(t, delay, tt.maxDelay)
		})
	}
}

func TestCalculateDelay_JitterVariesAroundTarget(t *testing.T) {
	tests := []struct {
		name        string
		minDelay    time.Duration
		maxDelay    time.Duration
		attempts    int
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "full jitter at MinDelay",
			minDelay:    100 * time.Millisecond,
			maxDelay:    time.Minute,
			attempts:    0,
			expectedMin: 0,
			expectedMax: 100 * time.Millisecond, // Full Jitter: [0, minDelay]
		},
		{
			name:        "full jitter at MaxDelay cap",
			minDelay:    10 * time.Millisecond,
			maxDelay:    50 * time.Millisecond,
			attempts:    3, // 10 * 2^3 = 80ms, capped to 50ms
			expectedMin: 0,
			expectedMax: 50 * time.Millisecond, // Full Jitter: [0, maxDelay]
		},
		{
			name:        "full jitter at high attempts",
			minDelay:    time.Second,
			maxDelay:    10 * time.Second,
			attempts:    5, // Would be 32s without cap, capped to 10s
			expectedMin: 0,
			expectedMax: 10 * time.Second, // Full Jitter: [0, maxDelay]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(
				tt.minDelay,
				tt.maxDelay,
			)
			r.attempt = tt.attempts

			delay := r.calculateDelay()
			assert.GreaterOrEqual(t, delay, tt.expectedMin)
			assert.LessOrEqual(t, delay, tt.expectedMax)
		})
	}
}

func TestCalculateDelay_FullJitterDeterministic(t *testing.T) {
	t.Run("different seeds produce different jittered delays", func(t *testing.T) {
		r1, _ := newRetryerWithFakeTimerAndJitter(100*time.Millisecond, time.Minute, seed1x1)
		r1.attempt = 0
		delay1 := r1.calculateDelay()

		r2, _ := newRetryerWithFakeTimerAndJitter(100*time.Millisecond, time.Minute, seed2x2)
		r2.attempt = 0
		delay2 := r2.calculateDelay()

		// Different seeds produce different delays
		assert.NotEqual(t, delay1, delay2, "different seeds should produce different delays")
		assert.Equal(t, jitter(100*time.Millisecond, jitterFractionSeed1x1), delay1)
		assert.Equal(t, jitter(100*time.Millisecond, jitterFractionSeed2x2), delay2)
	})

	t.Run("full jitter produces values in valid range", func(t *testing.T) {
		// Test multiple seeds to ensure all produce values in valid range
		seeds := []testSeed{seed10x20, seed42, seed100x200}

		for _, seed := range seeds {
			r, _ := newRetryerWithFakeTimerAndJitter(200*time.Millisecond, time.Minute, seed)
			r.attempt = 0

			delay := r.calculateDelay()
			// Full Jitter: should be between 0 and 200ms
			assert.GreaterOrEqual(t, delay, time.Duration(0), "delay should be non-negative")
			assert.LessOrEqual(t, delay, 200*time.Millisecond, "delay should not exceed computed value")
		}
	})
}

// TestCalculateDelay_DirectCall tests calling calculateDelay without going through Do().
func TestCalculateDelay_DirectCall(t *testing.T) {
	r := New(10*time.Millisecond, time.Minute)
	// r.attempts is 0 by default
	delay := r.calculateDelay()
	// With jitter enabled, delay will be random between 0 and 10ms
	// We can only verify it's in valid range
	assert.GreaterOrEqual(t, delay, time.Duration(0), "delay should be non-negative")
	assert.LessOrEqual(t, delay, 10*time.Millisecond, "delay should not exceed MinDelay on first attempt")
}
