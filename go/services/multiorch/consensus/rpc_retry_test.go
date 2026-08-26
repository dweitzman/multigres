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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

func TestRetryRPC_RetriesUntilSuccess(t *testing.T) {
	attempts := 0
	err := retryRPC(t.Context(), func() error {
		attempts++
		if attempts < 3 {
			return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not ready yet")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, attempts, "should retry until success")
}

func TestRetryRPC_DeadlineExceededRetries(t *testing.T) {
	attempts := 0
	err := retryRPC(t.Context(), func() error {
		attempts++
		if attempts < 2 {
			return mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timed out")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func TestRetryRPC_AbortedNeverRetries(t *testing.T) {
	attempts := 0
	err := retryRPC(t.Context(), func() error {
		attempts++
		return mterrors.New(mtrpcpb.Code_ABORTED, "term superseded")
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	assert.Equal(t, 1, attempts, "ABORTED must not be retried — the identical request cannot succeed")
}

func TestRetryRPC_FailedPreconditionNeverRetries(t *testing.T) {
	attempts := 0
	err := retryRPC(t.Context(), func() error {
		attempts++
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "wrong role")
	})
	require.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetryRPC_GivesUpAfterMaxAttempts(t *testing.T) {
	attempts := 0
	err := retryRPC(t.Context(), func() error {
		attempts++
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "still not ready")
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
	assert.Equal(t, retryRPCMaxAttempts, attempts, "should give up after the bounded attempt count")
}

func TestRetryRPC_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	attempts := 0
	err := retryRPC(ctx, func() error {
		attempts++
		if attempts == 1 {
			cancel()
		}
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "still not ready")
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, attempts, "should stop retrying once the context is cancelled")
}

func TestRetryRPC_SucceedsFirstTryNoDelay(t *testing.T) {
	start := time.Now()
	attempts := 0
	err := retryRPC(t.Context(), func() error {
		attempts++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, attempts)
	assert.Less(t, time.Since(start), 20*time.Millisecond, "first attempt must not incur a backoff wait")
}
