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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// These tests exercise the retry wiring end-to-end through FakeClient — a
// transient (UNAVAILABLE) failure on the first RPC attempt, success on the
// second — as opposed to rpc_retry_test.go, which unit-tests retryRPC's
// backoff/classification logic directly against a synthetic fn.

func TestRecruit_RetriesTransientFailureThenSucceeds(t *testing.T) {
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)
	rc := c.newRuleChange("test", nopTryBuildProposal, nopCheckProposalPossible)

	mp1 := makePoolerState("zone1", "mp1")
	mp1Key := topoclient.ComponentIDString(mp1.Multipooler.Id)
	setRecruitOK(fc, mp1)
	fc.ErrorOnce[mp1Key] = mterrors.New(mtrpcpb.Code_UNAVAILABLE, "postgres not ready yet")

	cs := rc.recruit(ctx, mp1, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1})
	require.NotNil(t, cs, "recruit should succeed once the transient failure clears on retry")
	wantCall := fmt.Sprintf("Recruit(%s)", mp1Key)
	assert.Equal(t, []string{wantCall, wantCall}, fc.GetCallLog(),
		"Recruit should have been called twice: the failed attempt and the retry")
}

func TestPromote_LeaderRetriesTransientFailureThenSucceeds(t *testing.T) {
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)
	rc := c.newRuleChange("test", nopTryBuildProposal, nopCheckProposalPossible)

	mp1 := makePoolerState("zone1", "mp1")
	leaderID := mp1.Multipooler.Id
	mp1Key := topoclient.ComponentIDString(leaderID)
	fc.ErrorOnce[mp1Key] = mterrors.New(mtrpcpb.Code_UNAVAILABLE, "postgres not ready yet")

	req := &consensusdatapb.PromoteRequest{
		Proposal: &consensusdatapb.CoordinatorProposal{
			ProposalLeader: &clustermetadatapb.PoolerAddress{Id: leaderID, Host: "localhost", PostgresPort: 5432},
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				LeaderId: leaderID,
			}},
		},
	}

	err := rc.promote(ctx, mp1, req, true /* isLeader */)
	require.NoError(t, err, "promote should succeed once the transient failure clears on retry")
	wantCall := fmt.Sprintf("Promote(%s)", mp1Key)
	assert.Equal(t, []string{wantCall, wantCall}, fc.GetCallLog(),
		"Promote should have been called twice: the failed attempt and the retry")
}

func TestPromote_LeaderDoesNotRetryAbortedFailure(t *testing.T) {
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)
	rc := c.newRuleChange("test", nopTryBuildProposal, nopCheckProposalPossible)

	mp1 := makePoolerState("zone1", "mp1")
	leaderID := mp1.Multipooler.Id
	mp1Key := topoclient.ComponentIDString(leaderID)
	// Persistent (not ErrorOnce) ABORTED failure: if promote() retried this,
	// the call log would show more than one attempt.
	fc.Errors[mp1Key] = mterrors.New(mtrpcpb.Code_ABORTED, "term superseded by a more recent Recruit")

	req := &consensusdatapb.PromoteRequest{
		Proposal: &consensusdatapb.CoordinatorProposal{
			ProposalLeader: &clustermetadatapb.PoolerAddress{Id: leaderID, Host: "localhost", PostgresPort: 5432},
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				LeaderId: leaderID,
			}},
		},
	}

	err := rc.promote(ctx, mp1, req, true /* isLeader */)
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	assert.Equal(t, []string{fmt.Sprintf("Promote(%s)", mp1Key)}, fc.GetCallLog(),
		"ABORTED must fail fast — the identical request can never succeed on retry")
}

func TestPromote_NonLeaderSetPrimaryRetriesTransientFailureThenSucceeds(t *testing.T) {
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)
	rc := c.newRuleChange("test", nopTryBuildProposal, nopCheckProposalPossible)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	leaderID := mp1.Multipooler.Id
	mp2Key := topoclient.ComponentIDString(mp2.Multipooler.Id)
	fc.ErrorOnce[mp2Key] = mterrors.New(mtrpcpb.Code_UNAVAILABLE, "postgres not ready yet")

	req := &consensusdatapb.PromoteRequest{
		Proposal: &consensusdatapb.CoordinatorProposal{
			ProposalLeader: &clustermetadatapb.PoolerAddress{Id: leaderID, Host: "localhost", PostgresPort: 5432},
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				LeaderId: leaderID,
			}},
		},
	}

	err := rc.promote(ctx, mp2, req, false /* isLeader */)
	require.NoError(t, err, "SetPrimary should succeed once the transient failure clears on retry")
	wantCall := fmt.Sprintf("SetPrimary(%s)", mp2Key)
	assert.Equal(t, []string{wantCall, wantCall}, fc.GetCallLog(),
		"SetPrimary should have been called twice: the failed attempt and the retry")
}

func nopTryBuildProposal(_ *clustermetadatapb.TermRevocation, _ []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
	return nil, nil
}
