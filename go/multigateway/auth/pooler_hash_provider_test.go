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

package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	pgauth "github.com/multigres/multigres/go/pgprotocol/auth"
)

// mockMultiPoolerClient is a mock implementation of the MultiPoolerServiceClient.
type mockMultiPoolerClient struct {
	getAuthCredentialsFunc func(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error)
}

func (m *mockMultiPoolerClient) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	if m.getAuthCredentialsFunc != nil {
		return m.getAuthCredentialsFunc(ctx, req)
	}
	return nil, errors.New("not implemented")
}

// TestPoolerHashProvider_ExistingUser tests that GetPasswordHash returns
// the correct hash for an existing user.
func TestPoolerHashProvider_ExistingUser(t *testing.T) {
	// Valid SCRAM-SHA-256 hash with proper base64-encoded components
	scramHash := "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Oln6rNiYzlYY42lUpMtdJ7U90=:HKZfkuYXDxJboM9DFNR0yFNHpRx/rbdVdNOTk/V0v0Q="

	mockClient := &mockMultiPoolerClient{
		getAuthCredentialsFunc: func(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
			assert.Equal(t, "testuser", req.Username)
			assert.Equal(t, "postgres", req.Database)
			return &multipoolerpb.GetAuthCredentialsResponse{
				UserExists:  true,
				ScramHash:   scramHash,
				HashVersion: 1,
			}, nil
		},
	}

	provider := NewPoolerHashProvider(mockClient)

	hash, err := provider.GetPasswordHash(t.Context(), "testuser", "postgres")
	require.NoError(t, err)
	require.NotNil(t, hash)

	// Verify the hash was parsed correctly
	assert.Equal(t, 4096, hash.Iterations)
}

// TestPoolerHashProvider_NonExistentUser tests that GetPasswordHash returns
// ErrUserNotFound for a non-existent user.
func TestPoolerHashProvider_NonExistentUser(t *testing.T) {
	mockClient := &mockMultiPoolerClient{
		getAuthCredentialsFunc: func(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
			return &multipoolerpb.GetAuthCredentialsResponse{
				UserExists:  false,
				ScramHash:   "",
				HashVersion: 0,
			}, nil
		},
	}

	provider := NewPoolerHashProvider(mockClient)

	hash, err := provider.GetPasswordHash(t.Context(), "nonexistent", "postgres")
	assert.ErrorIs(t, err, pgauth.ErrUserNotFound)
	assert.Nil(t, hash)
}

// TestPoolerHashProvider_UserWithoutPassword tests that GetPasswordHash returns
// ErrUserNotFound for a user that exists but has no password set.
func TestPoolerHashProvider_UserWithoutPassword(t *testing.T) {
	mockClient := &mockMultiPoolerClient{
		getAuthCredentialsFunc: func(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
			return &multipoolerpb.GetAuthCredentialsResponse{
				UserExists:  true,
				ScramHash:   "", // Empty - no password set
				HashVersion: 1,
			}, nil
		},
	}

	provider := NewPoolerHashProvider(mockClient)

	hash, err := provider.GetPasswordHash(t.Context(), "nopassword", "postgres")
	assert.ErrorIs(t, err, pgauth.ErrUserNotFound)
	assert.Nil(t, hash)
}

// TestPoolerHashProvider_GRPCError tests that GetPasswordHash propagates gRPC errors.
func TestPoolerHashProvider_GRPCError(t *testing.T) {
	mockClient := &mockMultiPoolerClient{
		getAuthCredentialsFunc: func(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
			return nil, errors.New("connection refused")
		},
	}

	provider := NewPoolerHashProvider(mockClient)

	hash, err := provider.GetPasswordHash(t.Context(), "testuser", "postgres")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.Nil(t, hash)
}

// TestPoolerHashProvider_InvalidHashFormat tests that GetPasswordHash returns
// an error when the hash format is invalid.
func TestPoolerHashProvider_InvalidHashFormat(t *testing.T) {
	mockClient := &mockMultiPoolerClient{
		getAuthCredentialsFunc: func(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
			return &multipoolerpb.GetAuthCredentialsResponse{
				UserExists:  true,
				ScramHash:   "invalid-hash-format",
				HashVersion: 1,
			}, nil
		},
	}

	provider := NewPoolerHashProvider(mockClient)

	hash, err := provider.GetPasswordHash(t.Context(), "testuser", "postgres")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
	assert.Nil(t, hash)
}

// TestPoolerHashProvider_ImplementsInterface verifies that PoolerHashProvider
// implements the PasswordHashProvider interface.
func TestPoolerHashProvider_ImplementsInterface(t *testing.T) {
	var _ pgauth.PasswordHashProvider = (*PoolerHashProvider)(nil)
}
