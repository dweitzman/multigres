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

package backoff

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
// Takes a seed parameter for deterministic jitter in tests.
func newRetryerWithFakeTimerAndJitter(minDelay, maxDelay time.Duration, seed1, seed2 uint64, opts ...Option) (*Retryer, *fakeTimer) {
	r := New(minDelay, maxDelay, opts...)
	// Use provided seed for deterministic testing
	r.rng = rand.New(rand.NewPCG(seed1, seed2))
	ft := &fakeTimer{}
	r.timer = ft
	return r, ft
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

	assert.NoError(t, err)
	assert.Equal(t, 1, attempts)
	assert.Equal(t, 0, r.Attempts())
	assert.Empty(t, ft.delays, "no delays should occur on immediate success")
}

func TestRetryer_Success_AfterRetries(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		5*time.Millisecond,
		20*time.Millisecond,
	)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		// Make sure delays happen after this operation.
		assert.Equal(t, attempts, len(ft.delays))
		attempts++
		if attempt < 3 {
			return errors.New("temporary error")
		}
		return nil // Success on 4th attempt (attempt index 3)
	})

	assert.NoError(t, err)
	assert.Equal(t, 4, attempts)
	// When successful, r.attempts stays at the successful attempt index
	assert.Equal(t, 3, r.Attempts())
	// Should have 3 delays: after attempts 0, 1, 2
	assert.Len(t, ft.delays, 3)
	assert.Equal(t, 5*time.Millisecond, ft.delays[0])  // 5 * 2^0
	assert.Equal(t, 10*time.Millisecond, ft.delays[1]) // 5 * 2^1
	assert.Equal(t, 20*time.Millisecond, ft.delays[2]) // 5 * 2^2
}

func TestRetryer_ExponentialBackoff(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		10*time.Millisecond,
		100*time.Millisecond,
	)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempts > 5 {
			return nil // Stop after 5 attempts to avoid infinite loop
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	require.Len(t, ft.delays, 5) // 5 delays after 6 attempts

	// Verify exponential growth: 10, 20, 40, 80, 100 (capped)
	assert.Equal(t, 10*time.Millisecond, ft.delays[0])  // 10 * 2^0
	assert.Equal(t, 20*time.Millisecond, ft.delays[1])  // 10 * 2^1
	assert.Equal(t, 40*time.Millisecond, ft.delays[2])  // 10 * 2^2
	assert.Equal(t, 80*time.Millisecond, ft.delays[3])  // 10 * 2^3
	assert.Equal(t, 100*time.Millisecond, ft.delays[4]) // capped at MaxDelay
}

func TestRetryer_MaxDelay(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		10*time.Millisecond,
		25*time.Millisecond,
	)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempts > 5 {
			return nil
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	require.Len(t, ft.delays, 5)

	// Verify delays are capped at MaxDelay
	assert.Equal(t, 10*time.Millisecond, ft.delays[0]) // 10 * 2^0
	assert.Equal(t, 20*time.Millisecond, ft.delays[1]) // 10 * 2^1
	assert.Equal(t, 25*time.Millisecond, ft.delays[2]) // capped (would be 40)
	assert.Equal(t, 25*time.Millisecond, ft.delays[3]) // capped (would be 80)
	assert.Equal(t, 25*time.Millisecond, ft.delays[4]) // capped (would be 160)
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
	r := New(10*time.Millisecond, time.Minute)

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

func TestRetryer_UnlimitedAttempts(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		1*time.Millisecond,
		time.Minute,
	)

	attempts := 0
	maxTestAttempts := 15

	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempts >= maxTestAttempts {
			return nil // Success after 15 attempts
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	assert.Equal(t, maxTestAttempts, attempts)
	assert.Len(t, ft.delays, 14) // 14 delays after 15 attempts
}

func TestRetryer_AttemptsCounter(t *testing.T) {
	r, _ := newRetryerWithFakeTimer(
		1*time.Millisecond,
		time.Minute,
	)

	recordedAttempts := make([]int, 0)

	err := r.Do(context.Background(), func(attempt int) error {
		recordedAttempts = append(recordedAttempts, attempt)
		// During execution, Attempts() returns the current attempt number
		assert.Equal(t, attempt, r.Attempts())
		if attempt >= 4 {
			return nil
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, recordedAttempts)
	assert.Equal(t, 4, r.Attempts())
}

func TestCalculateDelay(t *testing.T) {
	tests := []struct {
		name        string
		minDelay    time.Duration
		maxDelay    time.Duration
		attempt     int
		withJitter  bool
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "first attempt no jitter",
			minDelay:    10 * time.Millisecond,
			maxDelay:    time.Minute,
			attempt:     0,
			withJitter:  false,
			expectedMin: 10 * time.Millisecond,
			expectedMax: 10 * time.Millisecond,
		},
		{
			name:        "second attempt no jitter",
			minDelay:    10 * time.Millisecond,
			maxDelay:    time.Minute,
			attempt:     1,
			withJitter:  false,
			expectedMin: 20 * time.Millisecond,
			expectedMax: 20 * time.Millisecond,
		},
		{
			name:        "third attempt no jitter",
			minDelay:    10 * time.Millisecond,
			maxDelay:    time.Minute,
			attempt:     2,
			withJitter:  false,
			expectedMin: 40 * time.Millisecond,
			expectedMax: 40 * time.Millisecond,
		},
		{
			name:        "with max delay cap",
			minDelay:    10 * time.Millisecond,
			maxDelay:    30 * time.Millisecond,
			attempt:     5,
			withJitter:  false,
			expectedMin: 30 * time.Millisecond,
			expectedMax: 30 * time.Millisecond,
		},
		{
			name:        "with full jitter",
			minDelay:    100 * time.Millisecond,
			maxDelay:    time.Minute,
			attempt:     0,
			withJitter:  true,
			expectedMin: 34028597 * time.Nanosecond, // Full Jitter with seed(1,1): 100ms * 0.340286
			expectedMax: 34028597 * time.Nanosecond,
		},
		{
			name:        "exponential growth",
			minDelay:    5 * time.Millisecond,
			maxDelay:    time.Minute,
			attempt:     4,
			withJitter:  false,
			expectedMin: 80 * time.Millisecond, // 5 * 2^4
			expectedMax: 80 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r *Retryer
			if tt.withJitter {
				r, _ = newRetryerWithFakeTimerAndJitter(tt.minDelay, tt.maxDelay, 1, 1)
			} else {
				r, _ = newRetryerWithFakeTimer(tt.minDelay, tt.maxDelay)
			}
			r.attempts = tt.attempt

			delay := r.calculateDelay()
			assert.Equal(t, tt.expectedMin, delay)
			assert.Equal(t, tt.expectedMax, delay)
		})
	}
}

func TestCalculateDelay_JitterVariation(t *testing.T) {
	// Test that Full Jitter produces different values across multiple attempts
	r, _ := newRetryerWithFakeTimerAndJitter(
		100*time.Millisecond,
		time.Minute,
		42, 42, // Use seed(42,42) for this test
	)
	r.attempts = 0

	delays := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		delay := r.calculateDelay()
		delays[delay] = true
		// Full Jitter: all delays should be between 0 and 100ms
		assert.GreaterOrEqual(t, delay, time.Duration(0))
		assert.LessOrEqual(t, delay, 100*time.Millisecond)
	}

	// Should have seen multiple different delay values due to jitter
	assert.Greater(t, len(delays), 5, "jitter should produce variation across multiple calls")
}

func TestRetryer_NoDelayAfterLastAttempt(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(
		10*time.Millisecond,
		time.Minute,
	)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempts > 3 {
			return nil
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	// With 4 attempts (3 errors, 1 success), should have 3 delays (after 0, 1, 2)
	assert.Len(t, ft.delays, 3, "should not delay after last attempt")
}

func TestRetryer_DelayAfterWorkCompletes(t *testing.T) {
	// This test verifies the logical behavior that delays are calculated
	// after work completes. We use fake timer so we can't test timing directly,
	// but we verify the delay sequence is correct.
	r, ft := newRetryerWithFakeTimer(
		10*time.Millisecond,
		time.Minute,
	)

	executionOrder := []string{}
	attempts := 0

	err := r.Do(context.Background(), func(attempt int) error {
		executionOrder = append(executionOrder, "work")
		attempts++
		if attempts > 3 {
			return nil
		}
		return errors.New("error")
	})

	assert.NoError(t, err)
	// Work should execute 4 times
	assert.Equal(t, []string{"work", "work", "work", "work"}, executionOrder)
	// Delays happen after work, before next work (3 delays for 4 attempts)
	assert.Len(t, ft.delays, 3)
}

func TestRetryer_ContextTimeoutDuringSkip(t *testing.T) {
	// Use real timer for this test since we need actual timing
	r := New(
		100*time.Millisecond,
		time.Minute,
		WithDelayBeforeAttempt(),
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
	r := New(50*time.Millisecond, time.Minute)

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

func TestRetryer_ExplicitRetry(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempt < 3 {
			return errors.New("retry") // Any error triggers retry
		}
		return nil // Success
	})

	assert.NoError(t, err)
	assert.Equal(t, 4, attempts)
	assert.Len(t, ft.delays, 3)
}

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

func TestRetryer_NonRetryableStopsImmediately(t *testing.T) {
	r, _ := newRetryerWithFakeTimer(1*time.Millisecond, time.Minute)

	attempts := 0
	underlyingErr := errors.New("auth failed")

	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		// NonRetryable immediately on first attempt
		return NonRetryableError(underlyingErr)
	})

	assert.Error(t, err)
	assert.Equal(t, underlyingErr, err)
	assert.Equal(t, 1, attempts, "should abort without retrying")
}

func TestRetryer_RegularErrorsStillRetry(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

	attempts := 0
	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		if attempt < 3 {
			// Regular errors should still trigger retry
			return errors.New("temporary error")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 4, attempts, "regular errors should still retry")
	assert.Len(t, ft.delays, 3)
}

func TestRetryer_NonRetryableErrorMessage(t *testing.T) {
	r, _ := newRetryerWithFakeTimer(1*time.Millisecond, time.Minute)

	underlyingErr := errors.New("database connection failed")
	err := r.Do(context.Background(), func(attempt int) error {
		return NonRetryableError(underlyingErr)
	})

	// The returned error should be the unwrapped underlying error
	assert.Error(t, err)
	assert.Equal(t, "database connection failed", err.Error())
}

func TestRetryer_MixedErrorTypes(t *testing.T) {
	r, ft := newRetryerWithFakeTimer(10*time.Millisecond, time.Minute)

	attempts := 0
	executionFlow := []string{}

	err := r.Do(context.Background(), func(attempt int) error {
		attempts++
		switch attempt {
		case 0:
			executionFlow = append(executionFlow, "regular error")
			return errors.New("temporary error")
		case 1:
			executionFlow = append(executionFlow, "another regular error")
			return errors.New("connection timeout")
		case 2:
			executionFlow = append(executionFlow, "another regular error")
			return errors.New("network timeout")
		case 3:
			executionFlow = append(executionFlow, "abort")
			return NonRetryableError(errors.New("permanent failure"))
		default:
			executionFlow = append(executionFlow, "should not reach")
			return nil
		}
	})

	assert.Error(t, err)
	assert.Equal(t, "permanent failure", err.Error())
	assert.Equal(t, 4, attempts)
	assert.Equal(t, []string{"regular error", "another regular error", "another regular error", "abort"}, executionFlow)
	assert.Len(t, ft.delays, 3, "should have 3 delays before non-retryable")
}

// Error detection tests using errors.As()

func TestNonRetryableError_Detection(t *testing.T) {
	r, _ := newRetryerWithFakeTimer(1*time.Millisecond, time.Minute)

	underlyingErr := errors.New("auth failed")
	err := r.Do(context.Background(), func(attempt int) error {
		return NonRetryableError(underlyingErr)
	})

	// Should return the underlying error directly, not wrapped
	assert.Equal(t, underlyingErr, err)

	// Test that NonRetryableError can be detected via errors.As()
	testErr := NonRetryableError(errors.New("test"))
	var nre *nonRetryableError
	assert.True(t, errors.As(testErr, &nre))
}

func TestNonRetryableError_ChainedDetection(t *testing.T) {
	// Test that NonRetryableError works with error chains
	baseErr := errors.New("database error")
	wrappedErr := fmt.Errorf("connection failed: %w", baseErr)
	nonRetryableErr := NonRetryableError(wrappedErr)

	// Should be able to detect via errors.As()
	var nre *nonRetryableError
	assert.True(t, errors.As(nonRetryableErr, &nre))

	// Should be able to unwrap to get original error
	assert.Equal(t, wrappedErr, nre.Unwrap())
	assert.ErrorIs(t, nre.Unwrap(), baseErr)
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
			r.attempts = tt.attempts

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
		name         string
		minDelay     time.Duration
		maxDelay     time.Duration
		attempts     int
		seed1, seed2 uint64
		expected     time.Duration
	}{
		{
			name:     "full jitter at MinDelay with seed(1,1)",
			minDelay: 100 * time.Millisecond,
			maxDelay: time.Minute,
			attempts: 0,
			seed1:    1,
			seed2:    1,
			expected: 34028597 * time.Nanosecond, // 100ms * 0.340286
		},
		{
			name:     "full jitter at MaxDelay cap with seed(2,2)",
			minDelay: 10 * time.Millisecond,
			maxDelay: 50 * time.Millisecond,
			attempts: 3, // 10 * 2^3 = 80ms, capped to 50ms
			seed1:    2,
			seed2:    2,
			expected: 3914553 * time.Nanosecond, // 50ms * 0.078291
		},
		{
			name:     "full jitter at high attempts with seed(3,3)",
			minDelay: time.Second,
			maxDelay: 10 * time.Second,
			attempts: 5, // Would be 32s without cap, capped to 10s
			seed1:    3,
			seed2:    3,
			expected: 3128509851 * time.Nanosecond, // 10s * 0.312851
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := newRetryerWithFakeTimerAndJitter(
				tt.minDelay,
				tt.maxDelay,
				tt.seed1,
				tt.seed2,
			)
			r.attempts = tt.attempts

			delay := r.calculateDelay()
			assert.Equal(t, tt.expected, delay)
		})
	}
}

func TestCalculateDelay_FullJitterDeterministic(t *testing.T) {
	t.Run("different seeds produce different jittered delays", func(t *testing.T) {
		r1, _ := newRetryerWithFakeTimerAndJitter(100*time.Millisecond, time.Minute, 1, 1)
		r1.attempts = 0
		delay1 := r1.calculateDelay()

		r2, _ := newRetryerWithFakeTimerAndJitter(100*time.Millisecond, time.Minute, 2, 2)
		r2.attempts = 0
		delay2 := r2.calculateDelay()

		// Different seeds produce different delays
		assert.NotEqual(t, delay1, delay2, "different seeds should produce different delays")
		assert.Equal(t, 34028597*time.Nanosecond, delay1) // seed(1,1): 100ms * 0.340286
		assert.Equal(t, 7829106*time.Nanosecond, delay2)  // seed(2,2): 100ms * 0.078291
	})

	t.Run("full jitter produces values in valid range", func(t *testing.T) {
		// Test multiple attempts with same seed to ensure all values in range
		seeds := []struct{ s1, s2 uint64 }{
			{10, 20}, {42, 42}, {100, 200},
		}

		for _, seed := range seeds {
			r, _ := newRetryerWithFakeTimerAndJitter(200*time.Millisecond, time.Minute, seed.s1, seed.s2)
			r.attempts = 0

			delay := r.calculateDelay()
			// Full Jitter: should be between 0 and 200ms
			assert.GreaterOrEqual(t, delay, time.Duration(0))
			assert.LessOrEqual(t, delay, 200*time.Millisecond)
		}
	})
}
