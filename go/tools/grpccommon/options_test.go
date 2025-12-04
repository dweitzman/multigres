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

package grpccommon

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxMessageSize_Default(t *testing.T) {
	// Default should be 16 MiB (matching MySQL default)
	expected := 16 * 1024 * 1024
	assert.Equal(t, expected, MaxMessageSize())
}

func TestRegisterFlags(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)

	// Verify flags are registered
	maxMsgFlag := fs.Lookup("grpc-max-message-size")
	require.NotNil(t, maxMsgFlag, "grpc-max-message-size flag should be registered")
	assert.Equal(t, "16777216", maxMsgFlag.DefValue)

	tracingFlag := fs.Lookup("grpc-enable-tracing")
	require.NotNil(t, tracingFlag, "grpc-enable-tracing flag should be registered")
}

func TestLocalClientDialOptions(t *testing.T) {
	opts := LocalClientDialOptions()

	// Should return 3 options: credentials, disable service config, stats handler
	require.Len(t, opts, 3, "LocalClientDialOptions should return 3 dial options")

	// Options are opaque, but we can verify they're non-nil
	for i, opt := range opts {
		assert.NotNil(t, opt, "option %d should not be nil", i)
	}
}
