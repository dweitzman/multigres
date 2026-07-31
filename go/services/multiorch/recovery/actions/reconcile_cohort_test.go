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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestReconcileCohortAction_Metadata(t *testing.T) {
	action := NewReconcileCohortAction(nil, nil, nil, nil, slog.Default())
	md := action.Metadata()
	assert.Equal(t, "ReconcileCohort", md.Name)
	assert.True(t, md.Retryable)
	assert.True(t, action.RequiresHealthyLeader())
	assert.Nil(t, action.GracePeriod())
}

func TestReconcileCohortAction_Execute(t *testing.T) {
	ctx := context.Background()
	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}
	replica2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica2"}
	replica3ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica3"}
	replica4ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica4"}
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}

	setupStore := func(t *testing.T, fakeClient *rpcclient.FakeClient) (*store.PoolerCache, func()) {
		t.Helper()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		ps := store.NewTestCache(t)
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       primaryID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:             primaryID,
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 7},
						LeaderId:      primaryID,
						CohortMembers: []*clustermetadatapb.ID{primaryID, replica2ID},
						DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
							QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
							RequiredCount: 1,
						},
					}},
				},
			},
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       replicaID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			},
			// Healthy and streaming by default — ReconcileCohortAction now
			// re-verifies eligibility.Joinable/Evaluate against fresh state
			// before applying, so an ADD target must actually pass it.
			IsLastCheckValid: true,
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
					WalReceiverStatus: "streaming",
					LastReceiveLsn:    "0/3000000",
				},
			},
		}, nil))
		return ps, func() { _ = ts.Close() }
	}

	t.Run("ProblemPoolerNotInCohort issues UpdateConsensusRule with ADD", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.NoError(t, err)
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD, req.Operation)
		require.Len(t, req.StandbyIds, 1)
		assert.Equal(t, replicaID.Name, req.StandbyIds[0].Name)
		require.NotNil(t, req.ExpectedOutgoingRule, "CAS guard must be set")
		assert.Equal(t, int64(3), req.ExpectedOutgoingRule.CoordinatorTerm)
		assert.Equal(t, int64(7), req.ExpectedOutgoingRule.LeaderSubterm)

		// After recording membership on the leader, the action re-issues
		// SetPrimary to the joining member so it clears restore_command
		// synchronously — a member that joins an already-established cohort
		// out-of-band never went through Recruit's clear. The rule relayed is
		// the leader's post-ADD rule (re-read from the leader's status).
		assert.Contains(t, fakeClient.CallLog, "Status(multipooler-cell1-primary)")
		assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-replica1)")
		setReq := fakeClient.SetPrimaryRequests["multipooler-cell1-replica1"]
		require.NotNil(t, setReq)
		assert.Equal(t, primaryID.Name, setReq.GetReplicationPrimary().GetPrimary().GetId().GetName())
	})

	t.Run("ProblemCohortMemberIneligible issues UpdateConsensusRule with REMOVE", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID, primaryID, replicaID, replica2ID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()
		// Execute now re-verifies eligibility fresh rather than trusting
		// problem.Code, so replicaID must actually be a cohort member and
		// actually excluded (self-reported INELIGIBLE, overriding the shared
		// healthy default). The cohort must also be large enough that
		// removing replicaID AND losing the leader still leaves a majority —
		// IsCohortMemberRemovalSafe requires strict majority, not just the
		// policy's raw count, so this needs 5 members (2 survivors of 4
		// isn't a majority), not the 3-member shape used elsewhere.
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id: primaryID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com", PortMap: map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:             primaryID,
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 7},
						LeaderId:   primaryID,
						CohortMembers: []*clustermetadatapb.ID{
							primaryID, replicaID, replica2ID, replica3ID, replica4ID,
						},
						DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
							QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
							RequiredCount: 2,
						},
					}},
				},
			},
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: replicaID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
			AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			},
		}, nil))

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemCohortMemberIneligible,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.NoError(t, err)
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE, req.Operation)
	})

	t.Run("target pooler not in store resolves to a no-op, not an error", func(t *testing.T) {
		// Execute no longer trusts problem.Code — with no rider for replicaID
		// at all (and it's not in the leader's cohort or a tombstone either),
		// eligibility.DecideAll correctly says there's nothing to do: the ADD
		// this Problem originally proposed is moot.
		fakeClient := &rpcclient.FakeClient{}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "ghost"},
		})

		require.NoError(t, err)
		assert.NotContains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
	})

	t.Run("returns error when no consensus leader is known for the shard", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{}
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		ps := store.NewTestCache(t)
		// Add only the target replica; the shard search uses the
		// (database, table_group, shard) tuple, so an unrelated shard tuple
		// finds no poolers and therefore no leader.
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       replicaID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
		}, nil))

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code: types.ProblemPoolerNotInCohort,
			ShardKey: &clustermetadatapb.ShardKey{
				Database: "otherdb", TableGroup: "default", Shard: "0",
			},
			PoolerID: replicaID,
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no consensus leader known")
	})

	t.Run("surfaces an UpdateConsensusRule failure (e.g. stale-leader CAS rejection)", func(t *testing.T) {
		// The action does not pre-verify the leader's health: it issues the
		// CAS-fenced UpdateConsensusRule against the cached leader and lets the RPC
		// reject a stale write. A failure is surfaced so the engine retries next
		// cycle with a fresh view.
		fakeClient := &rpcclient.FakeClient{
			Errors: map[topoclient.ComponentID]error{
				"multipooler-cell1-primary": errors.New("rule CAS rejected"),
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "UpdateConsensusRule failed")
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
	})

	t.Run("rejects when the leader's highest known position has an undecided proposal", func(t *testing.T) {
		// Propagation isn't supported yet: the cohort must not be updated
		// against an outgoing rule that isn't decided. Seed the cache
		// directly (bypassing setupStore's decided-only primary) with a
		// proposal beyond the decision.
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		ps := store.NewTestCache(t)
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       primaryID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:             primaryID,
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{
						Decision: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 7},
							LeaderId:   primaryID,
						},
						Proposal: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 8},
							LeaderId:   primaryID,
						},
					},
				},
			},
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       replicaID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			},
		}, nil))

		fakeClient := &rpcclient.FakeClient{}
		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update its cohort while it has an undecided proposal")
		assert.Empty(t, fakeClient.CallLog, "no RPC should be dispatched")
	})

	t.Run("batches multiple ADD candidates into a single UpdateConsensusRule call", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()
		// A second healthy, joinable non-member alongside setupStore's default
		// replicaID: both should ADD in the same RPC instead of trickling out
		// one per recovery cycle.
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: replica3ID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
			IsLastCheckValid: true,
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
					WalReceiverStatus: "streaming",
					LastReceiveLsn:    "0/3000000",
				},
			},
		}, nil))

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.NoError(t, err)
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD, req.Operation)
		var gotNames []string
		for _, id := range req.StandbyIds {
			gotNames = append(gotNames, id.Name)
		}
		assert.ElementsMatch(t, []string{replicaID.Name, replica3ID.Name}, gotNames)
	})

	t.Run("batches multiple Urgent REMOVE candidates into a single UpdateConsensusRule call", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID, primaryID, replicaID, replica2ID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()
		// setupStore's default cohort is {primaryID, replica2ID}; extend it to
		// also cover replicaID, then mark both replicaID and replica2ID
		// quarantined (Urgent — bypasses the durability-safety gate entirely)
		// so both should REMOVE in one call even though a 3-member AT_LEAST_1
		// cohort couldn't safely lose both under the non-urgent path.
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id: primaryID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com", PortMap: map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:             primaryID,
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 7},
						LeaderId:      primaryID,
						CohortMembers: []*clustermetadatapb.ID{primaryID, replicaID, replica2ID},
						DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
							QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
							RequiredCount: 1,
						},
					}},
				},
			},
		}, nil))
		quarantined := func(id *clustermetadatapb.ID) *store.Pooler {
			return store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{
					Id: id, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA,
					LifecycleStatus: &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED},
				},
			}, nil)
		}
		store.SeedCache(t, ps, quarantined(replicaID))
		store.SeedCache(t, ps, quarantined(replica2ID))

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemCohortMemberQuarantined,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.NoError(t, err)
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE, req.Operation)
		var gotNames []string
		for _, id := range req.StandbyIds {
			gotNames = append(gotNames, id.Name)
		}
		assert.ElementsMatch(t, []string{replicaID.Name, replica2ID.Name}, gotNames)
	})

	t.Run("problem.Code plays no role in the decision", func(t *testing.T) {
		// Execute re-derives the operation from fresh state via
		// eligibility.DecideAll; a nonsense/mismatched Code (here,
		// ProblemReplicaNotReplicating on a genuine ADD candidate) doesn't
		// stop it from applying the ADD that current state actually calls for.
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.Problem{
			Code:     types.ProblemReplicaNotReplicating,
			ShardKey: shardKey,
			PoolerID: replicaID,
		})

		require.NoError(t, err)
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD, req.Operation)
	})
}

// selfLeaderConsensus builds a consensus status in which the pooler names itself
// as the consensus leader (so commonconsensus.HighestKnownRule/IsLeader identify
// it) without a recorded rule number. If cohort is non-empty, an AT_LEAST_1
// durability policy is attached too, so IsCohortMemberRemovalSafe has enough
// to work with.
func selfLeaderConsensus(id *clustermetadatapb.ID, cohort ...*clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
	rule := &clustermetadatapb.ShardRule{LeaderId: id}
	if len(cohort) > 0 {
		rule.CohortMembers = cohort
		rule.DurabilityPolicy = &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 1,
		}
	}
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: rule},
		},
	}
}
