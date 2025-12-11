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

package executil

import (
	"context"
	"time"
)

// DefaultGracePeriod is the default grace period before SIGKILL (5 seconds).
var DefaultGracePeriod = WithGracePeriod(5 * time.Second)

// GraceOption specifies how long to wait between SIGTERM and SIGKILL.
type GraceOption struct {
	d   time.Duration
	ctx context.Context
}

// WithGracePeriod creates a GraceOption with a fixed duration.
// After context cancellation triggers SIGTERM, wait this long before SIGKILL.
func WithGracePeriod(d time.Duration) GraceOption {
	return GraceOption{d: d}
}

// WithGraceContext creates a GraceOption with a context-based grace period.
// After context cancellation triggers SIGTERM, wait until this context is done before SIGKILL.
// This is useful for condition-based grace periods (e.g., wait for queries to drain).
func WithGraceContext(ctx context.Context) GraceOption {
	return GraceOption{ctx: ctx}
}

// duration returns the grace period duration.
// Returns 0 for context-based grace (requires custom handling in execution methods).
func (g GraceOption) duration() time.Duration {
	return g.d
}

// isContextBased returns true if this is a context-based grace option.
func (g GraceOption) isContextBased() bool {
	return g.ctx != nil
}

// graceContext returns the grace context for context-based grace options.
func (g GraceOption) graceContext() context.Context {
	return g.ctx
}
