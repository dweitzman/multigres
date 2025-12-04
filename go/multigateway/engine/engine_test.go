// Copyright 2025 Supabase, Inc.
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

package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRoute(t *testing.T) {
	route := NewRoute("tg1", "shard-0", "SELECT 1")

	assert.Equal(t, "tg1", route.TableGroup)
	assert.Equal(t, "shard-0", route.Shard)
	assert.Equal(t, "SELECT 1", route.Query)
}

func TestRoute_Getters(t *testing.T) {
	route := NewRoute("mygroup", "myshard", "SELECT * FROM users")

	assert.Equal(t, "mygroup", route.GetTableGroup())
	assert.Equal(t, "SELECT * FROM users", route.GetQuery())
	assert.Contains(t, route.String(), "mygroup")
	assert.Contains(t, route.String(), "SELECT * FROM users")
}

func TestRoute_VariousConfigs(t *testing.T) {
	// Test Route with various configurations
	// Note: Route.StreamExecute requires server.Conn which is hard to mock,
	// so we only test the getters here. StreamExecute is covered by integration tests.
	tests := []struct {
		name       string
		tableGroup string
		shard      string
		query      string
	}{
		{
			name:       "with shard",
			tableGroup: "tg1",
			shard:      "shard-0",
			query:      "SELECT 1",
		},
		{
			name:       "empty shard for unsharded",
			tableGroup: "unsharded_tg",
			shard:      "",
			query:      "SELECT * FROM config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := NewRoute(tt.tableGroup, tt.shard, tt.query)

			assert.Equal(t, tt.tableGroup, route.GetTableGroup())
			assert.Equal(t, tt.query, route.GetQuery())
			assert.Equal(t, tt.shard, route.Shard)
		})
	}
}

func TestNewPlan(t *testing.T) {
	route := NewRoute("tg1", "", "SELECT 1")
	plan := NewPlan("SELECT 1", route)

	assert.Equal(t, "SELECT 1", plan.Original)
	assert.Equal(t, route, plan.Primitive)
}

func TestPlan_GetTableGroup(t *testing.T) {
	route := NewRoute("target_group", "", "SELECT 1")
	plan := NewPlan("SELECT 1", route)

	assert.Equal(t, "target_group", plan.GetTableGroup())
}

func TestPlan_String(t *testing.T) {
	route := NewRoute("tg1", "", "SELECT 1")
	plan := NewPlan("SELECT 1", route)

	str := plan.String()
	assert.Contains(t, str, "Plan{")
	assert.Contains(t, str, "SELECT 1")
}

// Verify Route implements Primitive at compile time
var _ Primitive = (*Route)(nil)
