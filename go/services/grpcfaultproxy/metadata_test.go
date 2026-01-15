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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestExtractSource(t *testing.T) {
	tests := []struct {
		name     string
		md       metadata.MD
		expected string
	}{
		{
			name: "source present",
			md: metadata.New(map[string]string{
				SourceMetadataKey: "multipooler-zone1-0",
			}),
			expected: "multipooler-zone1-0",
		},
		{
			name:     "source absent",
			md:       metadata.New(map[string]string{}),
			expected: "",
		},
		{
			name:     "empty metadata",
			md:       metadata.MD{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSource(tt.md)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractAuthority(t *testing.T) {
	tests := []struct {
		name     string
		md       metadata.MD
		expected string
	}{
		{
			name: "authority present",
			md: metadata.New(map[string]string{
				AuthorityMetadataKey: "localhost:16100",
			}),
			expected: "localhost:16100",
		},
		{
			name:     "authority absent",
			md:       metadata.New(map[string]string{}),
			expected: "",
		},
		{
			name: "multiple services",
			md: metadata.New(map[string]string{
				AuthorityMetadataKey: "multipooler-zone1-0:16100",
			}),
			expected: "multipooler-zone1-0:16100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractAuthority(tt.md)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractRequestInfo(t *testing.T) {
	tests := []struct {
		name           string
		md             metadata.MD
		fullMethodName string
		expected       RequestInfo
	}{
		{
			name: "complete request info",
			md: metadata.New(map[string]string{
				SourceMetadataKey:    "multiorch",
				AuthorityMetadataKey: "multipooler-zone1-0:16100",
			}),
			fullMethodName: "/multipoolermanager.MultiPoolerManager/GetPoolerStatus",
			expected: RequestInfo{
				Source: "multiorch",
				Target: "multipooler-zone1-0:16100",
				Method: "/multipoolermanager.MultiPoolerManager/GetPoolerStatus",
			},
		},
		{
			name: "missing source",
			md: metadata.New(map[string]string{
				AuthorityMetadataKey: "localhost:16100",
			}),
			fullMethodName: "/grpc.health.v1.Health/Check",
			expected: RequestInfo{
				Source: "",
				Target: "localhost:16100",
				Method: "/grpc.health.v1.Health/Check",
			},
		},
		{
			name: "missing target",
			md: metadata.New(map[string]string{
				SourceMetadataKey: "test-client",
			}),
			fullMethodName: "/service.Service/Method",
			expected: RequestInfo{
				Source: "test-client",
				Target: "",
				Method: "/service.Service/Method",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractRequestInfo(tt.md, tt.fullMethodName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
