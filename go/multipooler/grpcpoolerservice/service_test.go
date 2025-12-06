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

package grpcpoolerservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// TestGetAuthCredentials_NilPooler verifies that GetAuthCredentials
// returns UNAVAILABLE when called without an initialized pooler.
func TestGetAuthCredentials_NilPooler(t *testing.T) {
	// Create a service without a pooler (nil)
	svc := &poolerService{
		pooler: nil,
	}

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: "testuser",
	}

	resp, err := svc.GetAuthCredentials(context.Background(), req)

	// Should return an error since pooler is nil
	require.Error(t, err)
	assert.Nil(t, resp)

	// Verify it's an UNAVAILABLE error
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	assert.Equal(t, codes.Unavailable, st.Code())
	assert.Contains(t, st.Message(), "pooler not initialized")
}

// TestGetAuthCredentials_ValidatesRequest verifies request validation.
func TestGetAuthCredentials_ValidatesRequest(t *testing.T) {
	svc := &poolerService{
		pooler: nil,
	}

	tests := []struct {
		name     string
		req      *multipoolerpb.GetAuthCredentialsRequest
		wantCode codes.Code
	}{
		{
			name: "empty username",
			req: &multipoolerpb.GetAuthCredentialsRequest{
				Database: "postgres",
				Username: "",
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "empty database",
			req: &multipoolerpb.GetAuthCredentialsRequest{
				Database: "",
				Username: "testuser",
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := svc.GetAuthCredentials(context.Background(), tt.req)

			// Should return an error for invalid requests
			require.Error(t, err)
			assert.Nil(t, resp)

			st, ok := status.FromError(err)
			require.True(t, ok, "expected gRPC status error")
			assert.Equal(t, tt.wantCode, st.Code())
		})
	}
}
