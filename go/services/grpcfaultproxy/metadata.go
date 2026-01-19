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
	"google.golang.org/grpc/metadata"
)

const (
	// SourceMetadataKey is the metadata key for identifying the source service.
	// Clients should inject this using the GRPC_PROXY_SOURCE_ID environment variable.
	SourceMetadataKey = "x-multigres-source"

	// AuthorityMetadataKey is the HTTP/2 :authority pseudo-header that contains
	// the target address (host:port) the client wants to connect to.
	AuthorityMetadataKey = ":authority"
)

// RequestInfo contains extracted information about a request (gRPC or PostgreSQL).
type RequestInfo struct {
	// Source is the source service identifier
	// - gRPC: from x-multigres-source metadata
	// - PostgreSQL: from application_name connection parameter
	Source string

	// Target is the target address (host:port)
	// - gRPC: from :authority pseudo-header
	// - PostgreSQL: from proxy_target in options parameter
	Target string

	// Method is the method/operation identifier
	// - gRPC: full method name (e.g., "/service.Service/Method")
	// - PostgreSQL: "postgres:startup" or "postgres:*"
	Method string

	// Protocol identifies the protocol type: "grpc" or "postgres"
	Protocol string
}

// extractSource extracts the source service identifier from metadata.
// Returns empty string if not present.
func extractSource(md metadata.MD) string {
	values := md.Get(SourceMetadataKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// extractAuthority extracts the target authority from metadata.
// The :authority pseudo-header contains the target address (host:port).
// Returns empty string if not present.
func extractAuthority(md metadata.MD) string {
	values := md.Get(AuthorityMetadataKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// ExtractRequestInfo extracts request information from gRPC metadata.
func ExtractRequestInfo(md metadata.MD, fullMethodName string) RequestInfo {
	return RequestInfo{
		Source:   extractSource(md),
		Target:   extractAuthority(md),
		Method:   fullMethodName,
		Protocol: "grpc",
	}
}
