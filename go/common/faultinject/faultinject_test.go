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

package faultinject

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	faults, err := Parse(`
# comment
promote.before-pg-promote delay=10s
promote.after-pg-promote delay=1s fail
`)
	require.NoError(t, err)
	assert.Equal(t, map[Point]Action{
		PromoteBeforePgPromote: {Delay: 10 * time.Second},
		PromoteAfterPgPromote:  {Delay: time.Second, Fail: true},
	}, faults)

	for _, bad := range []string{
		"no.such-point fail",
		"promote.before-pg-promote",
		"promote.before-pg-promote explode",
		"promote.before-pg-promote delay=soon",
	} {
		_, err := Parse(bad)
		assert.Error(t, err, bad)
	}
}

func TestCheck(t *testing.T) {
	var e Engine
	require.NoError(t, e.Check(t.Context(), PromoteBeforePgPromote), "nothing configured")

	e.Set(map[Point]Action{PromoteBeforePgPromote: {Fail: true}})
	assert.ErrorContains(t, e.Check(t.Context(), PromoteBeforePgPromote), "injected fault at promote.before-pg-promote")
	assert.NoError(t, e.Check(t.Context(), PromoteAfterPgPromote), "other points are unaffected")

	e.Set(map[Point]Action{PromoteBeforePgPromote: {Delay: time.Hour}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.ErrorIs(t, e.Check(ctx, PromoteBeforePgPromote), context.Canceled, "a delay ends with its context")

	e.Set(nil)
	assert.NoError(t, e.Check(t.Context(), PromoteBeforePgPromote))
}

func TestCheck_FollowsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "faults")
	e := Engine{file: path}
	require.NoError(t, e.Check(t.Context(), PromoteBeforePgPromote), "a missing file means no faults")

	require.NoError(t, os.WriteFile(path, []byte("promote.before-pg-promote fail\n"), 0o600))
	assert.Error(t, e.Check(t.Context(), PromoteBeforePgPromote))

	require.NoError(t, os.WriteFile(path, []byte("not a valid line\n"), 0o600))
	assert.Error(t, e.Check(t.Context(), PromoteBeforePgPromote), "an unparsable file keeps the previous faults")

	require.NoError(t, os.Remove(path))
	assert.NoError(t, e.Check(t.Context(), PromoteBeforePgPromote), "removing the file clears faults")
}
