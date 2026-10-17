// Copyright 2026 Supabase, Inc.
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

package recovery

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

func TestAlertTransitionLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	var l alertTransitionLogger
	now := time.Now()

	alert := types.Problem{
		Code:           types.ProblemShardAtRisk,
		Scope:          types.ScopePooler,
		RecoveryAction: actions.NewAlertOnlyAction(),
		Description:    "first description",
	}

	observe := func(offset time.Duration, problems ...types.Problem) string {
		buf.Reset()
		l.observe(context.Background(), logger, problems, now.Add(offset))
		return buf.String()
	}

	out := observe(0, alert)
	assert.Contains(t, out, "alert-only problem detected")
	assert.Contains(t, out, "first description")

	alert.Description = "changed description"
	assert.Empty(t, observe(time.Second, alert), "a persisting problem is not logged again, even if its description changes")

	out = observe(5 * time.Second)
	assert.Contains(t, out, "alert-only problem cleared")
	assert.Contains(t, out, "duration=5s")

	assert.Empty(t, observe(6*time.Second), "nothing to report once cleared")
	assert.Contains(t, observe(7*time.Second, alert), "alert-only problem detected", "a recurrence is a new transition")
}

func TestAlertTransitionLogger_IgnoresActionableProblems(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	var l alertTransitionLogger

	l.observe(context.Background(), logger, []types.Problem{{
		Code:           types.ProblemLeaderUnhealthy,
		RecoveryAction: actions.NewReconnectRecruitAbandonedAction(nil, logger),
	}}, time.Now())
	assert.Empty(t, buf.String())
}
