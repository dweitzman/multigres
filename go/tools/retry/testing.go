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

import "time"

// Shared test infrastructure used across backoff_test.go and retry_test.go

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

// fakeBackoff returns predetermined delays for testing retry logic in isolation.
// This allows retry tests to focus on orchestration without depending on
// specific backoff calculations.
type fakeBackoff struct {
	delays []time.Duration
}

func (f *fakeBackoff) nextDelay(attempt int) time.Duration {
	if attempt < len(f.delays) {
		return f.delays[attempt]
	}
	// Return the last delay for attempts beyond the predetermined list
	if len(f.delays) > 0 {
		return f.delays[len(f.delays)-1]
	}
	return 1 * time.Second // Default fallback
}

// withBackoff is a test-only option to set a custom backoff strategy.
func withBackoff(b backoff) Option {
	return func(c *Config) { c.backoff = b }
}
