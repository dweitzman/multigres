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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

func TestAlertOnlyAction(t *testing.T) {
	a := NewAlertOnlyAction()
	problem := types.Problem{
		Code:        types.ProblemShardStuck,
		ShardKey:    &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		Description: "shard needs a new leader but no recruitment quorum is reachable",
	}

	// The engine never executes alert-only problems; if one slips through, it fails loudly.
	err := a.Execute(t.Context(), types.RecheckedProblem{Problem: problem})
	require.Error(t, err)
	require.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))

	require.Equal(t, "AlertOnly", a.Metadata().Name)
	require.False(t, a.RequiresHealthyLeader())
	require.Nil(t, a.GracePeriod(), "an alert has nothing to defer")
}
