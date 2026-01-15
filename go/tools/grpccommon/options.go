// Copyright 2019 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package grpccommon

import (
	"context"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var (
	// maxMessageSize is the maximum message size which the gRPC server will
	// accept. Larger messages will be rejected.
	// Note: We're using 16 MiB as default value because that's the default in MySQL
	maxMessageSize = 16 * 1024 * 1024
	// enablePrometheus sets a flag to enable grpc client/server grpc monitoring.
	enablePrometheus bool
)

// RegisterFlags installs grpccommon flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.IntVar(&maxMessageSize, "grpc-max-message-size", maxMessageSize, "Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'.")
	fs.BoolVar(&grpc.EnableTracing, "grpc-enable-tracing", grpc.EnableTracing, "Enable gRPC tracing.")
	fs.BoolVar(&enablePrometheus, "grpc-prometheus", enablePrometheus, "Enable gRPC monitoring with Prometheus.")
}

// EnableGRPCPrometheus returns the value of the --grpc-prometheus flag.
func EnableGRPCPrometheus() bool {
	return enablePrometheus
}

// MaxMessageSize returns the value of the --grpc-max-message-size flag.
func MaxMessageSize() int {
	return maxMessageSize
}

// LocalClientDialOptions returns a slice of grpc.DialOption to be used when creating a gRPC client.
// These options are used for local clients connecting to the gRPC server.
// They are not intended to be used for production environments.
// The WithDisableServiceConfig is a workaround for a known issue
// in MacOS where localhost host takes too long to resolve.
// See the following PR for more details: https://github.com/multigres/multigres/pull/152
func LocalClientDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableServiceConfig(),
	}
}

// ClientOption configures OpenTelemetry instrumentation for the gRPC client.
// These options extend the stats handler that NewClient creates.
type ClientOption interface {
	apply(*clientConfig)
}

type clientConfig struct {
	otelOptions []otelgrpc.Option
	dialOptions []grpc.DialOption
}

// WithAttributes adds custom OpenTelemetry attributes to gRPC client spans.
// This is a generic helper that can be used by domain-specific code to add
// custom span attributes without making grpccommon domain-aware.
func WithAttributes(attrs ...attribute.KeyValue) ClientOption {
	return funcOption(func(c *clientConfig) {
		c.otelOptions = append(c.otelOptions,
			otelgrpc.WithSpanAttributes(attrs...),
		)
	})
}

// WithDialOptions adds standard gRPC dial options to the client.
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return funcOption(func(c *clientConfig) {
		c.dialOptions = append(c.dialOptions, opts...)
	})
}

type funcOption func(*clientConfig)

func (f funcOption) apply(c *clientConfig) {
	f(c)
}

// noopOption is a ClientOption that does nothing.
type noopOption struct{}

func (noopOption) apply(*clientConfig) {}

// WithSourceID adds x-multigres-source metadata when GRPC_PROXY_SOURCE_ID is set.
// This is only used for proxy testing - when the environment variable is not set,
// this option does nothing.
//
// The source ID identifies which service is making the gRPC request, allowing
// the fault injection proxy to apply rules based on the source service.
func WithSourceID() ClientOption {
	sourceID := os.Getenv("GRPC_PROXY_SOURCE_ID")
	if sourceID == "" {
		return noopOption{}
	}

	return funcOption(func(c *clientConfig) {
		c.dialOptions = append(c.dialOptions,
			grpc.WithUnaryInterceptor(sourceIDUnaryInterceptor(sourceID)),
			grpc.WithStreamInterceptor(sourceIDStreamInterceptor(sourceID)),
		)
	})
}

// sourceIDUnaryInterceptor injects x-multigres-source metadata for unary RPCs.
func sourceIDUnaryInterceptor(sourceID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-multigres-source", sourceID)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// sourceIDStreamInterceptor injects x-multigres-source metadata for streaming RPCs.
func sourceIDStreamInterceptor(sourceID string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-multigres-source", sourceID)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// NewClient creates a gRPC client with OpenTelemetry instrumentation.
// Use WithPeerService to set the remote service identifier in traces.
// Use WithDialOptions to pass standard gRPC dial options.
//
// All ClientOptions are used to configure a single stats handler, preventing
// duplication and ensuring consistent telemetry across the application.
//
// The client automatically includes WithSourceID() to support proxy testing
// when the GRPC_PROXY_SOURCE_ID environment variable is set.
func NewClient(target string, opts ...ClientOption) (*grpc.ClientConn, error) {
	// Prepend WithSourceID so it's always applied
	opts = append([]ClientOption{WithSourceID()}, opts...)

	cfg := &clientConfig{}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	// Create single stats handler with all OTel options
	allOpts := append([]grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(cfg.otelOptions...)),
	}, cfg.dialOptions...)

	return grpc.NewClient(target, allOpts...)
}
