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
//	    return backoff.NonRetryableError(err)
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

	// JitterFraction is the fraction of the delay to add as random jitter (0.0 to 1.0).
	// For example, 0.1 means add up to 10% random jitter.
	// Default: 0 (no jitter)
	JitterFraction float64

	// DelayBeforeAttempt starts with the first backoff delay instead of immediately
	// calling the operation. Useful when you've already tried once before calling Do().
	// Default: false (call operation immediately)
	DelayBeforeAttempt bool
}

// Retryer manages the retry state.
type Retryer struct {
	cfg      Config
	attempts int
	rng      *rand.Rand
	timer    Timer
}

// Option is a functional option for configuring a Retryer.
type Option func(*Config)

// WithJitter sets the jitter fraction (0.0 to 1.0).
func WithJitter(fraction float64) Option {
	return func(c *Config) { c.JitterFraction = fraction }
}

// WithDelayBeforeAttempt configures the retryer to start with a delay instead of
// immediately calling the operation. Use this when you've already tried once.
func WithDelayBeforeAttempt() Option {
	return func(c *Config) { c.DelayBeforeAttempt = true }
}

// New creates a new Retryer with the given minDelay and maxDelay, plus optional configuration.
// Panics if the parameters are invalid (represents a coding error).
func New(minDelay, maxDelay time.Duration, opts ...Option) *Retryer {
	// Validate required parameters (panic on coding errors)
	if minDelay <= 0 {
		panic("backoff: MinDelay must be positive")
	}
	if maxDelay <= 0 {
		panic("backoff: MaxDelay must be positive")
	}
	if minDelay > maxDelay {
		panic("backoff: MinDelay cannot be greater than MaxDelay")
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

	// Validate optional configuration
	if cfg.JitterFraction < 0 || cfg.JitterFraction > 1.0 {
		panic("backoff: JitterFraction must be between 0.0 and 1.0")
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
	for r.attempts = 0; ; r.attempts++ {
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
		err := operation(r.attempts)
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

// calculateDelay computes the next delay with exponential backoff and jitter.
func (r *Retryer) calculateDelay() time.Duration {
	// Exponential backoff: minDelay * 2^attempts
	// Use bit shifting for precise integer math and overflow protection
	attempts := r.attempts
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

	// Add jitter (applied before final cap to ensure we never exceed MaxDelay)
	if r.cfg.JitterFraction > 0 {
		jitter := time.Duration(float64(delay) * r.cfg.JitterFraction * r.rng.Float64())
		delay += jitter
		// Cap again in case jitter pushed us over
		if delay > r.cfg.MaxDelay {
			delay = r.cfg.MaxDelay
		}
	}

	return delay
}

// Attempts returns the current attempt number (0-indexed).
func (r *Retryer) Attempts() int {
	return r.attempts
}

// Example usage:
//
// // Basic usage:
// retryer := backoff.New(
//     500 * time.Millisecond,  // minDelay
//     30 * time.Second,         // maxDelay
//     backoff.WithJitter(0.1),
// )
// err := retryer.Do(ctx, func(attempt int) error {
//     result, err := makeAPICall()
//     if err != nil {
//         // Any error triggers retry by default
//         return err
//     }
//     return nil
// })
//
// // Stopping retries for permanent errors:
// err := retryer.Do(ctx, func(attempt int) error {
//     result, err := makeAPICall()
//     if err != nil {
//         // Stop immediately on auth errors - no point retrying
//         if errors.Is(err, ErrUnauthorized) {
//             return backoff.NonRetryableError(err)
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
// retryer := backoff.New(
//     100 * time.Millisecond,  // minDelay
//     5 * time.Second,          // maxDelay
// )
// err := retryer.Do(ctx, func(attempt int) error {
//     return checkServiceReady()
// })
