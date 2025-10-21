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

// Config holds the configuration for retry behavior.
type Config struct {
	// BaseDelay is the base delay for exponential backoff (delay = baseDelay × 2^attempt).
	// With Full Jitter, actual delays range from 0 to the computed delay.
	// Each retry doubles the delay up to MaxDelay.
	// Required.
	BaseDelay time.Duration

	// MaxDelay is the maximum delay between retry attempts.
	// Computed delays exceeding this value will be capped.
	// Required.
	MaxDelay time.Duration

	// InitialDelay adds a delay before the first attempt (attempt 0).
	// Useful when you've already tried once before calling Do().
	// Default: false (call operation immediately)
	InitialDelay bool

	// backoff strategy for calculating delays between retries.
	// Defaults to exponential backoff with full jitter.
	// Not exported in public API, but accessible for testing.
	backoff backoff
}

// Retryer manages the retry state and executes operations with backoff.
type Retryer struct {
	cfg     Config
	attempt int
	timer   Timer
}

// Option is a functional option for configuring a Retryer.
type Option func(*Config)

// WithInitialDelay configures the retryer to add a delay before the first attempt.
// Use this when you've already tried once before calling Do().
func WithInitialDelay() Option {
	return func(c *Config) { c.InitialDelay = true }
}

// New creates a new Retryer with the given baseDelay and maxDelay, plus optional configuration.
// Panics if the parameters are invalid (represents a coding error).
//
// The default backoff strategy is exponential backoff with full jitter, which provides
// maximum randomization to prevent thundering herd problems.
//
// Parameters:
//   - baseDelay: Base delay for exponential backoff (delay = baseDelay × 2^attempt)
//   - maxDelay: Maximum delay cap to prevent unbounded growth
func New(baseDelay, maxDelay time.Duration, opts ...Option) *Retryer {
	// Validate required parameters (panic on coding errors)
	if baseDelay <= 0 {
		panic("retry: BaseDelay must be positive")
	}
	if maxDelay <= 0 {
		panic("retry: MaxDelay must be positive")
	}
	if baseDelay > maxDelay {
		panic("retry: BaseDelay cannot be greater than MaxDelay")
	}

	// Build config with defaults
	cfg := Config{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		backoff:   newExponentialFullJitterBackoff(baseDelay, maxDelay),
	}

	// Apply optional configuration
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Retryer{
		cfg:   cfg,
		timer: realTimer{},
	}
}

// Do executes the operation with exponential backoff.
// The operation function receives the attempt number (0-indexed).
// Returns nil if operation succeeds, or the last error if all attempts fail.
//
// The retry loop continues indefinitely until one of the following occurs:
//   - The operation succeeds (returns nil)
//   - The operation returns a NonRetryableError
//   - The context is cancelled or times out
//
// Example usage:
//
//	r := retry.New(500*time.Millisecond, 30*time.Second)
//	err := r.Do(ctx, func(attempt int) error {
//	    result, err := makeAPICall()
//	    if err != nil {
//	        if errors.Is(err, ErrUnauthorized) {
//	            return retry.NonRetryableError(err)
//	        }
//	        return err // Retry on other errors
//	    }
//	    return nil
//	})
func (r *Retryer) Do(ctx context.Context, operation func(attempt int) error) error {
	for r.attempt = 0; ; r.attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Calculate delay with backoff strategy
		delay := r.cfg.backoff.nextDelay(r.attempt)

		if r.cfg.InitialDelay {
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

		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Check if the error signals that we should stop retries
		var nre *nonRetryableError
		if errors.As(err, &nre) {
			return nre.err
		}

		if !r.cfg.InitialDelay {
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

// Attempt returns the current attempt number (0-indexed).
func (r *Retryer) Attempt() int {
	return r.attempt
}
