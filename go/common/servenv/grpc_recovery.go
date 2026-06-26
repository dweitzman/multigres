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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/tools/safego"
)

// A panicking gRPC handler is a request-local failure: there is a caller to
// return an error to, so the right response is to recover and fail that one
// request, not to crash the whole process (an unrecovered handler panic would).
// These interceptors recover any panic, route it through safego (which logs the
// value + stack and counts it), and return an opaque Internal error so no panic
// detail leaks to the client. They are installed first in the chain so they also
// cover panics raised inside other interceptors.

// recoveryUnaryInterceptor recovers panics from unary handlers.
func recoveryUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			_ = safego.Recovered(ctx, info.FullMethod, r)
			resp = nil
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}

// recoveryStreamInterceptor recovers panics from streaming handlers.
func recoveryStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			_ = safego.Recovered(ss.Context(), info.FullMethod, r)
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(srv, ss)
}
