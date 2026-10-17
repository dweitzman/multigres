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

package actions

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// Compile-time assertion that AlertOnlyAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*AlertOnlyAction)(nil)

// AlertOnlyActionName is AlertOnlyAction's RecoveryMetadata().Name, exported
// so callers can identify it without string-matching a literal (e.g. to skip
// treating it like a real recovery attempt in logs/metrics).
const AlertOnlyActionName = "AlertOnly"

// AlertOnlyAction is the recovery action for non-actionable problems — ones an
// orchestrator has detected but cannot fix on its own (e.g. ShardStuck,
// ShardAtRisk). It performs no remediation; surfacing the problem is the whole
// point. The engine drops these problems before prioritizing them, so Execute
// is never expected to run; the detected-problems metric and the engine's alert transition
// logs are the signal.
type AlertOnlyAction struct{}

// NewAlertOnlyAction creates an alert-only recovery action.
func NewAlertOnlyAction() *AlertOnlyAction {
	return &AlertOnlyAction{}
}

// Execute reports a bug: the engine never runs alert-only problems, so reaching
// it means that filtering broke. It returns an error rather than panicking so a
// slip here cannot take down the failover coordinator, and it changes nothing.
func (a *AlertOnlyAction) Execute(context.Context, types.RecheckedProblem) error {
	return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "alert-only problems are never executed; the engine should have filtered this one")
}

func (a *AlertOnlyAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        AlertOnlyActionName,
		Description: "Records a non-actionable problem for alerting; performs no remediation",
		Timeout:     5 * time.Second,
	}
}

// RequiresHealthyLeader is false: alerting never touches the leader.
func (a *AlertOnlyAction) RequiresHealthyLeader() bool {
	return false
}

// GracePeriod is nil: there is nothing to defer for an alert.
func (a *AlertOnlyAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
