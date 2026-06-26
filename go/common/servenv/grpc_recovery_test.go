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

package servenv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryUnaryInterceptor_RecoversPanic(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	handler := func(context.Context, any) (any, error) {
		panic("secret-panic-detail")
	}

	var resp any
	var err error
	require.NotPanics(t, func() {
		resp, err = recoveryUnaryInterceptor(t.Context(), "req", info, handler)
	})

	assert.Nil(t, resp)
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	// The client-facing error must not leak the panic value.
	assert.NotContains(t, err.Error(), "secret-panic-detail")
}

func TestRecoveryUnaryInterceptor_PassesThrough(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	handler := func(context.Context, any) (any, error) {
		return "ok", nil
	}

	resp, err := recoveryUnaryInterceptor(t.Context(), "req", info, handler)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
}

// fakeServerStream is a minimal grpc.ServerStream that only needs to answer
// Context() for the recovery interceptor.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeServerStream) Context() context.Context { return s.ctx }

func TestRecoveryStreamInterceptor_RecoversPanic(t *testing.T) {
	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}
	handler := func(any, grpc.ServerStream) error {
		panic("secret-stream-detail")
	}

	var err error
	require.NotPanics(t, func() {
		err = recoveryStreamInterceptor(nil, &fakeServerStream{ctx: t.Context()}, info, handler)
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.NotContains(t, err.Error(), "secret-stream-detail")
}

func TestRecoveryStreamInterceptor_PassesThrough(t *testing.T) {
	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}
	handler := func(any, grpc.ServerStream) error {
		return nil
	}

	err := recoveryStreamInterceptor(nil, &fakeServerStream{ctx: t.Context()}, info, handler)
	require.NoError(t, err)
}
