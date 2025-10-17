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
	"math"
	"math/rand/v2"
	"time"
)

// backoff calculates retry delays based on attempt numbers.
// Implementations determine the backoff strategy (exponential, linear, constant, etc.).
// Each strategy manages its own configuration internally.
type backoff interface {
	// nextDelay calculates the delay for a given attempt number (0-indexed).
	nextDelay(attempt int) time.Duration
}

// exponentialFullJitterBackoff implements exponential backoff with Full Jitter.
//
// This implements the "Full Jitter" algorithm recommended by AWS:
// sleep = random_between(0, min(cap, base * 2^attempt))
//
// Full Jitter provides maximum randomization to prevent thundering herd problems
// where multiple clients retry at the same time, causing synchronized load spikes.
//
// Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// The algorithm works as follows:
//  1. Calculate exponential delay: minDelay * 2^attempt
//  2. Cap at maxDelay to prevent unbounded growth
//  3. Apply Full Jitter: randomize between 0 and computed delay
//
// Best for: Most distributed systems, especially when multiple clients may retry simultaneously
// Examples:
// - Retrying failed API calls in microservices
// - Reconnecting to shared databases or message queues after connection loss
// - Polling for job completion when many workers compete for resources
// - Waiting for backend services to become healthy after deployment
//
// Pros: Maximum load spreading, best thundering herd protection
// Cons: Can produce very short delays (close to 0), which may cause rapid retries
//
// Note: For use cases requiring guaranteed minimum delays, consider implementing
// a fractionalJitter strategy in the future.
type exponentialFullJitterBackoff struct {
	minDelay      time.Duration
	maxDelay      time.Duration
	rng           *rand.Rand
	disableJitter bool // For deterministic testing
}

// newExponentialFullJitterBackoff creates a new exponential backoff with full jitter.
func newExponentialFullJitterBackoff(minDelay, maxDelay time.Duration) *exponentialFullJitterBackoff {
	return &exponentialFullJitterBackoff{
		minDelay: minDelay,
		maxDelay: maxDelay,
		rng:      rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}
}

// newExponentialFullJitterBackoffWithRNG creates a backoff with a specific RNG (for testing).
func newExponentialFullJitterBackoffWithRNG(minDelay, maxDelay time.Duration, rng *rand.Rand) *exponentialFullJitterBackoff {
	return &exponentialFullJitterBackoff{
		minDelay: minDelay,
		maxDelay: maxDelay,
		rng:      rng,
	}
}

// newExponentialBackoffNoJitter creates a backoff without jitter (for testing).
func newExponentialBackoffNoJitter(minDelay, maxDelay time.Duration) *exponentialFullJitterBackoff {
	return &exponentialFullJitterBackoff{
		minDelay:      minDelay,
		maxDelay:      maxDelay,
		disableJitter: true,
	}
}

// nextDelay calculates the next delay using exponential backoff with full jitter.
func (e *exponentialFullJitterBackoff) nextDelay(attempt int) time.Duration {
	// Exponential backoff: minDelay * 2^attempt
	// Use bit shifting for precise integer math and overflow protection
	attempts := attempt
	if attempts > 62 {
		// Cap to prevent overflow (shifting more than 62 bits would overflow int64)
		attempts = 62
	}

	// Calculate delay = minDelay * (1 << attempts)
	// time.Duration is int64, so we can work with it directly
	multiplier := int64(1 << attempts)
	minDelayInt := int64(e.minDelay)

	var delay time.Duration
	if minDelayInt > 0 && multiplier > math.MaxInt64/minDelayInt {
		// Would overflow, use maxDelay
		delay = e.maxDelay
	} else {
		delay = time.Duration(minDelayInt * multiplier)
		// Apply max delay cap
		if delay > e.maxDelay {
			delay = e.maxDelay
		}
	}

	// Apply Full Jitter: randomize between 0 and computed delay
	// This prevents synchronized retries by spreading retries across time
	if !e.disableJitter {
		// random_between(0, delay)
		// rng.Float64() returns [0.0, 1.0)
		delay = time.Duration(float64(delay) * e.rng.Float64())
	}

	return delay
}

// Future backoff strategies to consider implementing:
//
// 1. decorrelatedJitterBackoff:
//    Formula: sleep = min(cap, random_between(base, prev_sleep * 3))
//    Best for: When you want some randomization but prefer smoother retry patterns
//    Examples:
//    - User-facing retries where UX benefits from more predictable timing
//    - Scenarios where previous delay provides useful signal about system state
//    Pros: Maintains some dependency on previous delay, can feel more "natural"
//    Cons: Has clamping issues that reduce jitter effectiveness over time
//    Note: Would require a stateful interface or passing prevDelay parameter
//
// 2. fractionalJitterBackoff (also called "Equal Jitter" when fraction=0.5):
//    Formula: sleep = delay * (1-fraction) + random_between(0, delay * fraction)
//    Examples: fraction=0.5: sleep = delay/2 + random_between(0, delay/2)
//              fraction=0.2: sleep = 80% of delay + random_between(0, 20% of delay)
//    Best for: Latency-sensitive scenarios where delays must never get too short
//    Examples:
//    - OLTP query retries where sub-millisecond retries could overwhelm the database
//    - Rate limiting scenarios where you need guaranteed minimum spacing between requests
//    - Retrying latency-sensitive operations where very short delays are counterproductive
//    Pros: Configurable minimum delay (1-fraction), more predictable latency bounds
//    Cons: Less randomization than Full Jitter, less effective at preventing thundering herd
//
// 3. constantBackoff:
//    Formula: sleep = minDelay (always)
//    Best for: Simple scenarios where exponential growth isn't needed
//
// 4. linearBackoff:
//    Formula: sleep = min(cap, base * attempt)
//    Best for: Gradual increase without exponential growth
