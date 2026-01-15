// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcfaultproxy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// director is the StreamDirector function that routes gRPC calls to backends.
// It extracts metadata, logs the request, and returns the appropriate backend connection.
func (p *Proxy) director(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
	// Extract request metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "missing metadata in incoming context")
	}

	// Extract request information
	req := ExtractRequestInfo(md, fullMethodName)

	// Log the proxied request
	p.logger.DebugContext(ctx, "proxying gRPC call",
		"method", req.Method,
		"source", req.Source,
		"target", req.Target)

	// Validate we have a target
	if req.Target == "" {
		return ctx, nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"missing :authority header in request metadata")
	}

	// Get or create backend connection
	conn, err := p.getBackendConn(ctx, req.Target)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to get backend connection",
			"target", req.Target,
			"error", err)
		return ctx, nil, mterrors.Wrapf(err, "failed to connect to backend %s", req.Target)
	}

	// Return context and connection
	// The proxy will forward the RPC to this connection
	return ctx, conn, nil
}

// getBackendConn gets or creates a gRPC connection to the specified backend.
// Connections are cached and reused.
func (p *Proxy) getBackendConn(ctx context.Context, target string) (*grpc.ClientConn, error) {
	p.connMu.RLock()
	conn, exists := p.conns[target]
	p.connMu.RUnlock()

	if exists {
		return conn, nil
	}

	// Create new connection
	p.connMu.Lock()
	defer p.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := p.conns[target]; exists {
		return conn, nil
	}

	p.logger.DebugContext(ctx, "creating new backend connection", "target", target)

	// Create connection with insecure credentials
	// TODO: Support TLS in future
	conn, err := grpccommon.NewClient(target,
		grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to dial backend")
	}

	// Cache the connection
	p.conns[target] = conn

	return conn, nil
}

// closeBackendConns closes all cached backend connections.
func (p *Proxy) closeBackendConns() {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	for target, conn := range p.conns {
		if err := conn.Close(); err != nil {
			p.logger.Error("failed to close backend connection",
				"target", target,
				"error", err)
		}
	}

	p.conns = make(map[string]*grpc.ClientConn)
}
