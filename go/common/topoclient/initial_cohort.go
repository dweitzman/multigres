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

package topoclient

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/multigres/multigres/go/common/types"
)

// initialCohortPath returns the global topology path for the initial cohort
// record of a shard. The file is stored alongside other shard-level topology
// data under the databases hierarchy.
func initialCohortPath(shardKey types.ShardKey) string {
	return path.Join(DatabasesPath, shardKey.Database, shardKey.TableGroup, shardKey.Shard, InitialCohortFile)
}

// ClaimInitialCohort atomically records the initial cohort pooler IDs for a
// shard using a compare-and-swap write to the global topology.
//
// The first caller creates the record (etcd Create returns success only when
// the key does not yet exist) and receives back its proposed IDs. Any
// subsequent caller — including after a crash and retry — reads the
// already-committed record and receives those IDs instead of its own proposal.
//
// Callers must use the returned committedIDs, not their proposedIDs, to
// decide which poolers to include in the initial cohort.
func (ts *store) ClaimInitialCohort(ctx context.Context, shardKey types.ShardKey, proposedIDs []string) ([]string, error) {
	sorted := make([]string, len(proposedIDs))
	copy(sorted, proposedIDs)
	sort.Strings(sorted)

	contents := []byte(strings.Join(sorted, "\n"))
	filePath := initialCohortPath(shardKey)

	_, err := ts.globalTopo.Create(ctx, filePath, contents)
	if err == nil {
		// We won the race — our proposal is now the committed cohort.
		return sorted, nil
	}

	if !errors.Is(err, &TopoError{Code: NodeExists}) {
		return nil, fmt.Errorf("failed to claim initial cohort for shard %s: %w", shardKey, err)
	}

	// Another orch already committed the cohort. Read and use their record.
	data, _, err := ts.globalTopo.Get(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read committed initial cohort for shard %s: %w", shardKey, err)
	}

	var committed []string
	for line := range strings.SplitSeq(string(data), "\n") {
		if line != "" {
			committed = append(committed, line)
		}
	}
	return committed, nil
}
