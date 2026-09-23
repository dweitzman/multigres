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

// Package logattr provides typed constructors for recurring structured-logging
// attributes, so the same concept isn't spelled or typed differently across
// call sites (e.g. "tablegroup" vs "table_group", or a raw proto ID instead of
// its formatted string).
package logattr

import (
	"log/slog"
	"strings"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Err returns the canonical "error" attribute.
func Err(err error) slog.Attr {
	return slog.Any("error", err)
}

// poolerIDPrefix is the component-type prefix topoclient.ComponentIDString
// adds ahead of "cell-name" for a pooler. It's redundant under the "pooler_id"
// key (the key name already says it's a pooler), so PoolerID/PoolerIDString
// trim it to keep this frequently-logged field short.
const poolerIDPrefix = "multipooler-"

// PoolerID returns the "pooler_id" attribute for a raw pooler ID.
func PoolerID(id *clustermetadatapb.ID) slog.Attr {
	return PoolerIDString(string(topoclient.ComponentIDString(id)))
}

// PoolerIDString returns the "pooler_id" attribute from a value already
// formatted by topoclient.ComponentIDString (or equivalent), trimming the
// redundant component-type prefix for brevity. Intended for call sites that
// cache a topoclient.ComponentIDString-formatted value for reuse as a
// correlation/cache key elsewhere — trimming only affects what's logged, so
// it's safe to call without touching how that value is computed or stored.
func PoolerIDString(id string) slog.Attr {
	return slog.String("pooler_id", strings.TrimPrefix(id, poolerIDPrefix))
}

// ConnectionID returns the "connection_id" attribute for a client-facing
// (e.g. multigateway) connection identifier.
func ConnectionID(id uint32) slog.Attr {
	return slog.Uint64("connection_id", uint64(id))
}

// ReservedConnectionID returns the "reserved_connection_id" attribute for a
// multipooler reserved-connection identifier.
func ReservedConnectionID(id uint64) slog.Attr {
	return slog.Uint64("reserved_connection_id", id)
}

// TableGroup returns the "table_group" attribute.
func TableGroup(tableGroup string) slog.Attr {
	return slog.String("table_group", tableGroup)
}

// Cell returns the "cell" attribute.
func Cell(cell string) slog.Attr {
	return slog.String("cell", cell)
}

// ShardKey returns the "shard_key" attribute, formatted via commontypes.FormatShardKey.
func ShardKey(sk *clustermetadatapb.ShardKey) slog.Attr {
	return slog.String("shard_key", string(commontypes.FormatShardKey(sk)))
}

// User returns the "user" attribute.
func User(user string) slog.Attr {
	return slog.String("user", user)
}

// Database returns the "database" attribute.
func Database(database string) slog.Attr {
	return slog.String("database", database)
}

// Query returns the "query" attribute for a raw SQL query string. A single
// helper gives one place to later add truncation or redaction — query text
// can be large and may carry literal bind values.
func Query(sql string) slog.Attr {
	return slog.String("query", sql)
}
