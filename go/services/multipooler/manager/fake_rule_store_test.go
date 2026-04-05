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

package manager

import (
	"context"
	"sync"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// fakeRuleStore is a test double for ruleStorer that returns a preset position
// without hitting postgres. Both observePosition and updateRule return pos
// (or observeErr/updateErr when set). updateRule records all calls in updates.
type fakeRuleStore struct {
	mu         sync.Mutex
	pos        *clustermetadatapb.NodePosition
	observeErr error
	updateErr  error
	updates    []*ruleUpdateBuilder
}

func (f *fakeRuleStore) observePosition(_ context.Context) (*clustermetadatapb.NodePosition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pos, f.observeErr
}

func (f *fakeRuleStore) updateRule(_ context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.NodePosition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, update)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return f.pos, nil
}
