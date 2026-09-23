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

package logattr

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestAttrKeysAndValues(t *testing.T) {
	someErr := errors.New("boom")
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "pooler1",
	}
	shardKey := &clustermetadatapb.ShardKey{
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
	}

	cases := []struct {
		name    string
		attr    func() (key string, val any)
		wantKey string
	}{
		{"Err", func() (string, any) { a := Err(someErr); return a.Key, a.Value.Any() }, "error"},
		{"PoolerID", func() (string, any) { a := PoolerID(poolerID); return a.Key, a.Value.Any() }, "pooler_id"},
		{"PoolerIDString", func() (string, any) { a := PoolerIDString("zone1-pooler1"); return a.Key, a.Value.Any() }, "pooler_id"},
		{"ConnectionID", func() (string, any) { a := ConnectionID(7); return a.Key, a.Value.Any() }, "connection_id"},
		{"ReservedConnectionID", func() (string, any) { a := ReservedConnectionID(42); return a.Key, a.Value.Any() }, "reserved_connection_id"},
		{"TableGroup", func() (string, any) { a := TableGroup("tg1"); return a.Key, a.Value.Any() }, "table_group"},
		{"Cell", func() (string, any) { a := Cell("zone1"); return a.Key, a.Value.Any() }, "cell"},
		{"ShardKey", func() (string, any) { a := ShardKey(shardKey); return a.Key, a.Value.Any() }, "shard_key"},
		{"User", func() (string, any) { a := User("alice"); return a.Key, a.Value.Any() }, "user"},
		{"Database", func() (string, any) { a := Database("db1"); return a.Key, a.Value.Any() }, "database"},
		{"Query", func() (string, any) { a := Query("SELECT 1"); return a.Key, a.Value.Any() }, "query"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, val := tc.attr()
			require.Equal(t, tc.wantKey, key)
			require.NotEmpty(t, val)
		})
	}
}

func TestPoolerIDFormatsFromID(t *testing.T) {
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "pooler1",
	}
	require.Equal(t, "zone1-pooler1", PoolerID(id).Value.String())
}

func TestPoolerIDStringTrimsComponentPrefix(t *testing.T) {
	require.Equal(t, "zone1-pooler1", PoolerIDString("multipooler-zone1-pooler1").Value.String())
	require.Equal(t, "zone1-pooler1", PoolerIDString("zone1-pooler1").Value.String())
}
