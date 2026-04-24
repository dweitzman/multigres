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

package consensus

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/prototest"
)

// createMockNode creates a mock node for testing using FakeClient.
// Sets up RecruitResponse with ConsensusStatus (no Rule — callers override for end-to-end tests).
func createMockNode(fakeClient *rpcclient.FakeClient, name string, term int64, walLsn string, healthy bool, _ consensusdatapb.PostgresRole) *multiorchdatapb.PoolerHealthState {
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      name,
	}

	pooler := &clustermetadatapb.MultiPooler{
		Id:       poolerID,
		Hostname: "localhost",
		PortMap: map[string]int32{
			"grpc": 9000,
		},
	}

	poolerKey := topoclient.MultiPoolerIDString(poolerID)

	termRevocation := &consensusdatapb.TermRevocation{RevokedBelowTerm: term}

	fakeClient.RecruitResponses[poolerKey] = &consensusdatapb.RecruitResponse{
		ConsensusStatus: &consensusdatapb.ConsensusStatus{
			Id:              poolerID,
			TermRevocation:  termRevocation,
			CurrentPosition: &consensusdatapb.PoolerPosition{Lsn: walLsn},
		},
	}

	return &multiorchdatapb.PoolerHealthState{
		MultiPooler:      pooler,
		IsLastCheckValid: healthy,
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized:  true,
			TermRevocation: termRevocation,
		},
	}
}

func TestDiscoverMaxTerm(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("success - finds max term from cohort", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			createMockNode(fakeClient, "mp2", 3, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			createMockNode(fakeClient, "mp3", 7, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
		}

		maxTerm, err := c.discoverMaxTerm(cohort)
		require.NoError(t, err)
		require.Equal(t, int64(7), maxTerm)
	})

	t.Run("error - returns error when all nodes have term 0", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		// Unhealthy nodes: IsLastCheckValid=false skips the nil-TermRevocation invariant.
		// TermRevocation.RevokedBelowTerm=0 so maxTerm stays 0, triggering the error.
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 0, "0/1000000", false, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			createMockNode(fakeClient, "mp2", 0, "0/1000000", false, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
		}

		_, err := c.discoverMaxTerm(cohort)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no poolers in cohort have initialized consensus term")
	})

	t.Run("success - ignores failed nodes", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}

		pooler2ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp2",
		}
		pooler2 := &clustermetadatapb.MultiPooler{
			Id:       pooler2ID,
			Hostname: "localhost",
			PortMap:  map[string]int32{"grpc": 9000},
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(pooler2ID)] = context.DeadlineExceeded

		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			{MultiPooler: pooler2},
			createMockNode(fakeClient, "mp3", 3, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
		}

		maxTerm, err := c.discoverMaxTerm(cohort)
		require.NoError(t, err)
		require.Equal(t, int64(5), maxTerm)
	})
}

func TestRecruitNodes(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}
	termRevocation := &consensusdatapb.TermRevocation{
		RevokedBelowTerm:      6,
		AcceptedCoordinatorId: coordID,
	}

	t.Run("success - all nodes accept", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
		}

		recruited, err := c.recruitNodes(ctx, cohort, termRevocation)
		require.NoError(t, err)
		require.Len(t, recruited, 3)
	})

	t.Run("success - excludes nodes that return errors", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			createMockNode(fakeClient, "mp3", 5, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
		}

		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp3ID)] = errors.New("rejected: term too low")

		recruited, err := c.recruitNodes(ctx, cohort, termRevocation)
		require.NoError(t, err)
		require.Len(t, recruited, 2)
	})

	t.Run("success - excludes nodes with RPC errors", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		c := &Coordinator{
			coordinatorID: coordID,
			logger:        logger,
			rpcClient:     fakeClient,
		}
		cohort := []*multiorchdatapb.PoolerHealthState{
			createMockNode(fakeClient, "mp1", 5, "0/3000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY),
			createMockNode(fakeClient, "mp2", 5, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
			createMockNode(fakeClient, "mp3", 5, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA),
		}

		mp3ID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "mp3",
		}
		fakeClient.Errors[topoclient.MultiPoolerIDString(mp3ID)] = context.DeadlineExceeded

		recruited, err := c.recruitNodes(ctx, cohort, termRevocation)
		require.NoError(t, err)
		require.Len(t, recruited, 2)
	})
}

// defaultDurabilityPolicy returns an AT_LEAST_N policy for use in test rules.
func defaultDurabilityPolicy(required int32) *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_N",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: required,
	}
}

// setNodeRule overrides the recruit response for a node to include a ShardRule
// in its CurrentPosition. Required for tests that exercise CheckSufficientRecruitment.
func setNodeRule(fakeClient *rpcclient.FakeClient, poolerID *clustermetadatapb.ID, walLsn string, rule *consensusdatapb.ShardRule) {
	key := topoclient.MultiPoolerIDString(poolerID)
	fakeClient.RecruitResponses[key] = &consensusdatapb.RecruitResponse{
		ConsensusStatus: &consensusdatapb.ConsensusStatus{
			Id:             poolerID,
			TermRevocation: &consensusdatapb.TermRevocation{RevokedBelowTerm: 5},
			CurrentPosition: &consensusdatapb.PoolerPosition{
				Lsn:  walLsn,
				Rule: rule,
			},
		},
	}
}

func TestAppointLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	t.Run("propose sent to recruited nodes when some nodes reject", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		policy := defaultDurabilityPolicy(2)
		require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
			Name:                      "testdb",
			BootstrapDurabilityPolicy: policy,
		}))

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		mp1 := createMockNode(fakeClient, "mp1", 5, "0/3000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		mp1.Status.PostgresReady = true
		mp2 := createMockNode(fakeClient, "mp2", 5, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		mp2.Status.PostgresReady = true
		mp3 := createMockNode(fakeClient, "mp3", 5, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		mp3.Status.PostgresReady = true

		require.NoError(t, ts.CreateMultiPooler(ctx, mp1.MultiPooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, mp2.MultiPooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, mp3.MultiPooler))

		// mp3 rejects recruitment; mp1 and mp2 are recruited.
		mp3Key := topoclient.MultiPoolerIDString(mp3.MultiPooler.Id)
		fakeClient.Errors[mp3Key] = errors.New("rejected: WAL freeze failed")

		// Set up proper Rules on mp1 and mp2 so CheckSufficientRecruitment succeeds.
		existingRule := &consensusdatapb.ShardRule{
			RuleNumber:       &consensusdatapb.RuleNumber{CoordinatorTerm: 5},
			CohortMembers:    []*clustermetadatapb.ID{mp1.MultiPooler.Id, mp2.MultiPooler.Id},
			DurabilityPolicy: policy,
		}
		setNodeRule(fakeClient, mp1.MultiPooler.Id, "0/3000000", existingRule)
		setNodeRule(fakeClient, mp2.MultiPooler.Id, "0/2000000", existingRule)

		cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2, mp3}
		err := c.AppointLeader(ctx, "shard0", cohort, "testdb", "test_primary_lost")
		require.NoError(t, err)

		// Propose must be sent to both recruited nodes (mp1, mp2).
		callLog := fakeClient.GetCallLog()
		require.Contains(t, callLog, "Propose(multipooler-zone1-mp1)")
		require.Contains(t, callLog, "Propose(multipooler-zone1-mp2)")

		// Both nodes receive the same proposal; check via whichever key is present.
		mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
		req, ok := fakeClient.ProposeRequests[mp1Key]
		require.True(t, ok, "ProposeRequest should be recorded for mp1")

		// Term must be maxKnown+1 = 6.
		require.Equal(t, int64(6), req.GetProposal().GetTermRevocation().GetRevokedBelowTerm())

		// Cohort must be exactly the recruited nodes (not the rejected mp3).
		prototest.RequireElementsMatch(t, []*clustermetadatapb.ID{
			mp1.MultiPooler.Id,
			mp2.MultiPooler.Id,
		}, req.GetProposal().GetProposedRule().GetCohortMembers(),
			"proposed cohort should be the recruited nodes only")

		// Leader must be one of the recruited nodes.
		leaderName := req.GetProposal().GetProposalLeader().GetId().GetName()
		require.Contains(t, []string{"mp1", "mp2"}, leaderName)
	})
}

func TestAppointInitialLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	setupDatabase := func(t *testing.T, ts topoclient.Store, policy *clustermetadatapb.DurabilityPolicy) {
		t.Helper()
		require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
			Name:                      "testdb",
			BootstrapDurabilityPolicy: policy,
		}))
	}

	t.Run("success with term 1", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		policy := defaultDurabilityPolicy(2)
		setupDatabase(t, ts, policy)
		c := NewCoordinator(coordID, ts, fakeClient, logger)

		// Fresh standbys at term 0 (brand new nodes, just restored from backup).
		// TermRevocation must be non-nil (even for term=0) so preVote counts them as eligible.
		mp1 := createMockNode(fakeClient, "mp1", 0, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		mp1.Status.PostgresReady = true

		mp2 := createMockNode(fakeClient, "mp2", 0, "0/1000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		mp2.Status.PostgresReady = true

		require.NoError(t, ts.CreateMultiPooler(ctx, mp1.MultiPooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, mp2.MultiPooler))

		// Set up proper Rules on both nodes so CheckSufficientRecruitment succeeds.
		existingRule := &consensusdatapb.ShardRule{
			RuleNumber:       &consensusdatapb.RuleNumber{CoordinatorTerm: 0},
			CohortMembers:    []*clustermetadatapb.ID{mp1.MultiPooler.Id, mp2.MultiPooler.Id},
			DurabilityPolicy: policy,
		}
		setNodeRule(fakeClient, mp1.MultiPooler.Id, "0/2000000", existingRule)
		setNodeRule(fakeClient, mp2.MultiPooler.Id, "0/1000000", existingRule)

		cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}
		err := c.AppointInitialLeader(ctx, "shard0", cohort, "testdb")
		require.NoError(t, err)

		// Term=1 must be used (not discovered from nodes, which are all at term 0).
		callLog := fakeClient.GetCallLog()
		require.Contains(t, callLog, "Propose(multipooler-zone1-mp1)")

		mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
		req, ok := fakeClient.ProposeRequests[mp1Key]
		require.True(t, ok, "candidate should receive ProposeRequest")
		require.Equal(t, int64(1), req.GetProposal().GetTermRevocation().GetRevokedBelowTerm(),
			"initial leader should use term 1")
	})

	t.Run("empty cohort returns error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		err := c.AppointInitialLeader(ctx, "shard0", nil, "testdb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cohort is empty")
	})

	t.Run("missing bootstrap policy returns error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
			Name: "testdb",
		}))

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		mp1 := createMockNode(fakeClient, "mp1", 0, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		cohort := []*multiorchdatapb.PoolerHealthState{mp1}

		err := c.AppointInitialLeader(ctx, "shard0", cohort, "testdb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no bootstrap_durability_policy configured")
	})

	t.Run("pre-vote failure returns error", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Policy requires 3, but only 1 node — preVote will fail.
		setupDatabase(t, ts, defaultDurabilityPolicy(3))

		c := NewCoordinator(coordID, ts, fakeClient, logger)

		mp1 := createMockNode(fakeClient, "mp1", 0, "0/2000000", true, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA)
		cohort := []*multiorchdatapb.PoolerHealthState{mp1}

		err := c.AppointInitialLeader(ctx, "shard0", cohort, "testdb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "pre-vote failed")
	})
}
