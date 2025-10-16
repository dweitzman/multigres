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
	"math"
	"math/rand/v2"
	"time"
)

// nonRetryableError signals that an operation should not be retried.
// The retry loop will stop immediately and return the underlying error.
type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string {
	if e.err == nil {
		return "non-retryable error"
	}
	return e.err.Error()
}

func (e *nonRetryableError) Unwrap() error {
	return e.err
}

// NonRetryableError wraps an error to signal that retries should stop immediately.
// Use this for permanent failures like authentication errors, invalid configuration, etc.
//
// Example:
//
//	if errors.Is(err, ErrUnauthorized) {
//	    return retry.NonRetryableError(err)
//	}
func NonRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// Timer is an interface for time operations, allowing for fake timers in tests.
type Timer interface {
	After(d time.Duration) <-chan time.Time
}

// realTimer implements Timer using real time.
type realTimer struct{}

func (realTimer) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Config holds the configuration for exponential backoff.
type Config struct {
	// MinDelay is the starting delay for exponential backoff (delay * 2^attempt).
	// Each retry doubles the delay up to MaxDelay.
	// Required.
	MinDelay time.Duration

	// MaxDelay is the maximum wait time between attempts.
	// Required.
	MaxDelay time.Duration

	// DelayBeforeAttempt starts with the first backoff delay instead of immediately
	// calling the operation. Useful when you've already tried once before calling Do().
	// Default: false (call operation immediately)
	DelayBeforeAttempt bool

	// disableJitter disables jitter for deterministic testing.
	// Not exported - only used by test helpers.
	disableJitter bool
}

// Retryer manages the retry state.
type Retryer struct {
	cfg     Config
	attempt int
	rng     *rand.Rand
	timer   Timer
}

// Option is a functional option for configuring a Retryer.
type Option func(*Config)

// WithDelayBeforeAttempt configures the retryer to start with a delay instead of
// immediately calling the operation. Use this when you've already tried once.
func WithDelayBeforeAttempt() Option {
	return func(c *Config) { c.DelayBeforeAttempt = true }
}

// withDisableJitter disables jitter for deterministic testing.
// Not exported - only used by test helpers.
func withDisableJitter() Option {
	return func(c *Config) { c.disableJitter = true }
}

// New creates a new Retryer with the given minDelay and maxDelay, plus optional configuration.
// Panics if the parameters are invalid (represents a coding error).
func New(minDelay, maxDelay time.Duration, opts ...Option) *Retryer {
	// Validate required parameters (panic on coding errors)
	if minDelay <= 0 {
		panic("retry: MinDelay must be positive")
	}
	if maxDelay <= 0 {
		panic("retry: MaxDelay must be positive")
	}
	if minDelay > maxDelay {
		panic("retry: MinDelay cannot be greater than MaxDelay")
	}

	// Build config
	cfg := Config{
		MinDelay: minDelay,
		MaxDelay: maxDelay,
	}

	// Apply optional configuration
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Retryer{
		cfg:   cfg,
		rng:   rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
		timer: realTimer{},
	}
}

// Do executes the operation with exponential backoff.
// The operation function receives the attempt number (0-indexed).
// Returns nil if operation succeeds, or the last error if all attempts fail.
func (r *Retryer) Do(ctx context.Context, operation func(attempt int) error) error {
	for r.attempt = 0; ; r.attempt++ {
		// Calculate delay with exponential backoff
		delay := r.calculateDelay()

		if r.cfg.DelayBeforeAttempt {
			// Wait for the delay or context cancellation
			select {
			case <-r.timer.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Execute the operation
		err := operation(r.attempt)
		if err == nil {
			return nil // Success!
		}

		// Check if the error signals that we should stop retries
		var nre *nonRetryableError
		if errors.As(err, &nre) {
			return nre.err
		}

		if !r.cfg.DelayBeforeAttempt {
			// Wait for the delay or context cancellation
			select {
			case <-r.timer.After(delay):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// calculateDelay computes the next delay with exponential backoff and Full Jitter.
//
// This implements the "Full Jitter" algorithm recommended by AWS:
// sleep = random_between(0, min(cap, base * 2^attempt))
//
// Full Jitter provides maximum randomization to prevent thundering herd problems
// where multiple clients retry at the same time, causing synchronized load spikes.
//
// Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// TODO: Consider supporting multiple jitter strategies in the future:
//
//  1. Full Jitter (current implementation):
//     Formula: sleep = random_between(0, min(cap, base * 2^attempt))
//     Best for: Most distributed systems, especially when multiple clients may retry simultaneously
//     Examples:
//     - Retrying failed API calls in microservices
//     - Reconnecting to shared databases or message queues after connection loss
//     - Polling for job completion when many workers compete for resources
//     - Waiting for backend services to become healthy after deployment
//     Pros: Maximum load spreading, best thundering herd protection
//     Cons: Can produce very short delays (close to 0), which may cause rapid retries
//     Recommendation: Use this as the default unless you have specific latency requirements
//
//  2. Decorrelated Jitter:
//     Formula: sleep = min(cap, random_between(base, prev_sleep * 3))
//     Best for: When you want some randomization but prefer smoother retry patterns
//     Examples:
//     - User-facing retries where UX benefits from more predictable timing
//     - Scenarios where previous delay provides useful signal about system state
//     Pros: Maintains some dependency on previous delay, can feel more "natural"
//     Cons: Has clamping issues that reduce jitter effectiveness over time
//     Recommendation: Use with caution; AWS article notes this has flaws
//
//  3. Fractional Jitter (also called "Equal Jitter" when fraction=0.5):
//     Formula: sleep = delay * (1-fraction) + random_between(0, delay * fraction)
//     Examples: fraction=0.5: sleep = delay/2 + random_between(0, delay/2)
//     fraction=0.2: sleep = 80% of delay + random_between(0, 20% of delay)
//     Best for: Latency-sensitive scenarios where delays must never get too short
//     Examples:
//     - OLTP query retries where sub-millisecond retries could overwhelm the database
//     - Rate limiting scenarios where you need guaranteed minimum spacing between requests
//     - Retrying latency-sensitive operations where very short delays are counterproductive
//     - Infinite polling loops where minimum delay prevents resource exhaustion
//     Pros: Configurable minimum delay (1-fraction), more predictable latency bounds
//     Cons: Less randomization than Full Jitter, less effective at preventing thundering herd
//     Recommendation: Use when you need guaranteed minimum delays (e.g., fraction=0.5 ensures
//     delays are never less than 50% of the computed exponential backoff)
//
// Proposed API for future extension:
//
//	type JitterStrategy interface {
//	    Apply(delay time.Duration, rng *rand.Rand, prevDelay time.Duration) time.Duration
//	}
//
//	// Built-in strategies:
//	func FullJitter() JitterStrategy { ... }
//	func DecorrelatedJitter() JitterStrategy { ... }
//	func FractionalJitter(fraction float64) JitterStrategy { ... }
//
//	func WithJitterStrategy(strategy JitterStrategy) Option {
//	    return func(c *Config) { c.jitterStrategy = strategy }
//	}
//
// This would allow users to opt into alternative strategies while keeping
// Full Jitter as the recommended default, and make fraction configurable for
// Fractional Jitter.
func (r *Retryer) calculateDelay() time.Duration {
	// Exponential backoff: minDelay * 2^attempts
	// Use bit shifting for precise integer math and overflow protection
	attempts := r.attempt
	if attempts > 62 {
		// Cap to prevent overflow (shifting more than 62 bits would overflow int64)
		attempts = 62
	}

	// Calculate delay = MinDelay * (1 << attempts)
	// time.Duration is int64, so we can work with it directly
	multiplier := int64(1 << attempts)
	minDelay := int64(r.cfg.MinDelay)

	var delay time.Duration
	if minDelay > 0 && multiplier > math.MaxInt64/minDelay {
		// Would overflow, use MaxDelay
		delay = r.cfg.MaxDelay
	} else {
		delay = time.Duration(minDelay * multiplier)
		// Apply max delay cap
		if delay > r.cfg.MaxDelay {
			delay = r.cfg.MaxDelay
		}
	}

	// Apply Full Jitter: randomize between 0 and computed delay
	// This prevents synchronized retries by spreading retries across time
	if !r.cfg.disableJitter {
		// random_between(0, delay)
		// r.rng.Float64() returns [0.0, 1.0)
		delay = time.Duration(float64(delay) * r.rng.Float64())
	}

	return delay
}

// Attempt returns the current attempt number (0-indexed).
func (r *Retryer) Attempt() int {
	return r.attempt
}

// Example usage:
//
// // Basic usage with Full Jitter (automatic):
// r := retry.New(
//     500 * time.Millisecond,  // minDelay
//     30 * time.Second,         // maxDelay
// )
// err := r.Do(ctx, func(attempt int) error {
//     result, err := makeAPICall()
//     if err != nil {
//         // Any error triggers retry by default
//         return err
//     }
//     return nil
// })
//
// // Stopping retries for permanent errors:
// err := r.Do(ctx, func(attempt int) error {
//     result, err := makeAPICall()
//     if err != nil {
//         // Stop immediately on auth errors - no point retrying
//         if errors.Is(err, ErrUnauthorized) {
//             return retry.NonRetryableError(err)
//         }
//         // Any other error triggers retry
//         return err
//     }
//     return nil
// })
//
// // Time-bounded retry with context timeout (recommended pattern):
// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// defer cancel()
// r := retry.New(
//     100 * time.Millisecond,  // minDelay
//     5 * time.Second,          // maxDelay
// )
// err := r.Do(ctx, func(attempt int) error {
//     return checkServiceReady()
// })
//
// Note: Full Jitter is always enabled to prevent thundering herd problems.
// See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
