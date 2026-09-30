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
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// alwaysDetectAnalyzer re-detects a shard-wide failover problem on every run, so
// the recheck in attemptRecovery always confirms it and only the timing gate,
// applied after the recheck, decides whether the action runs.
type alwaysDetectAnalyzer struct {
	action types.RecoveryAction
}

func (a *alwaysDetectAnalyzer) Name() types.CheckName { return "MockAlwaysDetect" }

func (a *alwaysDetectAnalyzer) RecoveryAction() types.RecoveryAction { return a.action }

func (a *alwaysDetectAnalyzer) Analyze(sa *analysis.ShardAnalysis) ([]types.Problem, error) {
	return []types.Problem{{
		Code:           types.ProblemLeaderUnhealthy,
		CheckName:      a.Name(),
		ShardKey:       sa.ShardKey,
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		RecoveryAction: a.action,
		DetectedAt:     time.Now(),
	}}, nil
}

// newRecheckGateEngine builds an engine over one pooler whose cached state shows
// no revocation. The fake RPC client answers that pooler's live Status with
// liveRevocation, which is how another orchestrator's recruitment becomes
// visible only after a refresh.
func newRecheckGateEngine(t *testing.T, action types.RecoveryAction, liveRevocation *clustermetadatapb.TermRevocation) (*Engine, types.Problem) {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	decided := &clustermetadatapb.ConsensusStatus{
		Id: poolerID,
		CurrentPosition: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}},
		}},
	}

	fakeClient := rpcclient.NewFakeClient()
	live := decided
	if liveRevocation != nil {
		live = &clustermetadatapb.ConsensusStatus{
			Id:              poolerID,
			CurrentPosition: decided.CurrentPosition,
			TermRevocation:  liveRevocation,
		}
	}
	fakeClient.SetStatusResponse("multipooler-cell1-p1", &multipoolermanagerdatapb.StatusResponse{
		Status:          &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY},
		ConsensusStatus: live,
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))
	analysis.SetTestAnalyzers([]analysis.Analyzer{&alwaysDetectAnalyzer{action: action}})
	t.Cleanup(analysis.ResetAnalyzers)

	store.SeedCache(t, engine.poolerCache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: poolerID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		LastSeen:    timestamppb.Now(),
		Status:      &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY},
		// The cached state shows no revocation: the failover looks ready.
		ConsensusStatus: decided,
	}, nil))

	problems := detectProblems(t, engine)
	require.Len(t, problems, 1)
	_, ready := engine.readyToExecute(problems[0])
	require.True(t, ready, "with no revocation in the cache the failover must look ready")
	return engine, problems[0]
}

func recentRevocation() *clustermetadatapb.TermRevocation {
	return &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		CoordinatorInitiatedAt: timestamppb.Now(),
		RecruitIntent: &clustermetadatapb.RecruitIntent{
			ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
			Attempt:         1,
		},
	}
}

// appointAction is a stand-in failover action. A zero refreshTimeout means it
// does not ask for a refresh before the recheck.
func appointAction(refreshTimeout time.Duration) *mockRecoveryAction {
	return &mockRecoveryAction{
		metadata: types.RecoveryMetadata{Name: "Appoint", Timeout: 30 * time.Second, RefreshBeforeRecheckTimeout: refreshTimeout},
	}
}

func TestAttemptRecovery_ReappliesTheGateAfterRefreshingEvidence(t *testing.T) {
	t.Run("defers when the refresh reveals another orchestrator's recent recruitment", func(t *testing.T) {
		action := appointAction(time.Second)
		engine, problem := newRecheckGateEngine(t, action, recentRevocation())

		engine.attemptRecovery(t.Context(), problem)

		assert.False(t, action.executed.Load(),
			"a revocation visible only after the refresh must push the backoff out, so the failover waits")
	})

	t.Run("runs when the refresh reveals nothing that delays it", func(t *testing.T) {
		action := appointAction(time.Second)
		engine, problem := newRecheckGateEngine(t, action, nil)

		engine.attemptRecovery(t.Context(), problem)

		assert.True(t, action.executed.Load())
	})

	t.Run("does not refresh for an action that did not ask for it", func(t *testing.T) {
		// Same live revocation, but the cache is never refreshed, so the gate still
		// sees none: a positive RefreshBeforeRecheckTimeout is what drives the refresh.
		action := appointAction(0)
		engine, problem := newRecheckGateEngine(t, action, recentRevocation())

		engine.attemptRecovery(t.Context(), problem)

		assert.True(t, action.executed.Load())
	})
}
