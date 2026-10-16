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
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

type alertKey struct {
	code     types.ProblemCode
	entityID string
}

// alertTransitionLogger logs alert-only problems when they appear and when they
// clear, rather than every cycle they persist. The zero value is ready to use.
type alertTransitionLogger struct {
	mu     sync.Mutex
	active map[alertKey]time.Time
}

// observe takes the full set of problems detected this cycle, once per cycle.
func (l *alertTransitionLogger) observe(ctx context.Context, logger *slog.Logger, problems []types.Problem, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	current := make(map[alertKey]time.Time)
	for _, p := range problems {
		if p.RecoveryAction.Metadata().Name != actions.AlertOnlyActionName {
			continue
		}
		key := alertKey{code: p.Code, entityID: p.EntityID()}
		since, known := l.active[key]
		if !known {
			since = now
			logger.InfoContext(ctx, "alert-only problem detected",
				"problem_code", p.Code, "entity_id", key.entityID, "description", p.Description)
		}
		current[key] = since
	}
	for key, since := range l.active {
		if _, still := current[key]; !still {
			logger.InfoContext(ctx, "alert-only problem cleared",
				"problem_code", key.code, "entity_id", key.entityID, "duration", now.Sub(since))
		}
	}
	l.active = current
}
