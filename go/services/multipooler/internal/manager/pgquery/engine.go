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

// Package pgquery is the multipooler's local-Postgres SQL mechanism layer: a
// home for the pre-packaged queries that read and change the state of this
// pooler's running PostgreSQL instance — replication status and LSNs, WAL-replay
// pause/resume, primary_conninfo get/set/reset, config reload, and schema/LSN
// checks. It is mechanism ("how to act on this instance"), never HA policy
// ("when/why", which stays in consensus/).
//
// It is a leaf package: it depends only on the executor query service and a
// logger, and is imported by the manager (as field pm.pg) and by consensus/.
// Process control (start/stop/restart) and on-disk config are a separate
// concern, not part of this query layer. Construct via NewEngine.
package pgquery

import (
	"log/slog"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Engine runs the pre-packaged queries that read and change local-Postgres
// state for a single multipooler: replication status, LSNs, WAL replay, conn
// info, and config reloads.
type Engine struct {
	logger *slog.Logger
	qs     executor.InternalQueryService
}

// NewEngine constructs a pgquery engine from the given fixed dependencies. The
// query service is captured once and is stable for the manager's lifetime; the
// connection pool underneath it may churn, but the reference does not change.
func NewEngine(logger *slog.Logger, qs executor.InternalQueryService) *Engine {
	return &Engine{logger: logger, qs: qs}
}
