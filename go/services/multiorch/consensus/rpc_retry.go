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

package consensus

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/retry"
)

// retryRPC calls fn, retrying with a short bounded backoff only when the
// returned error is transient (mterrors.IsRetryable — UNAVAILABLE or
// DEADLINE_EXCEEDED). Any other error, including ABORTED, is returned
// immediately: those signal a concurrency conflict that requires an explicit
// redo with different input (e.g. a fresh Recruit at a higher term), which is
// the recovery engine's job, not something a blind resend here can fix.
//
// fn's error must already be mterrors-coded — if it crossed a gRPC boundary,
// the caller (rpcclient.Client) is responsible for running it through
// mterrors.FromGRPC first, since a raw gRPC status error would otherwise be
// misclassified as UNKNOWN and never retried.
func retryRPC(ctx context.Context, fn func() error) error {
	r := retry.New(store.DefaultRPCRetryBaseDelay, store.DefaultRPCRetryMaxDelay)
	var lastErr error
	for attempt, waitErr := range r.Attempts(ctx) {
		if waitErr != nil {
			// Context was cancelled/timed out while waiting for backoff.
			return waitErr
		}
		lastErr = fn()
		if lastErr == nil || !mterrors.IsRetryable(lastErr) {
			return lastErr
		}
		if attempt >= store.DefaultRPCRetryMaxAttempts {
			return lastErr
		}
	}
	return lastErr
}
