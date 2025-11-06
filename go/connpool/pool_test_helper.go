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

package connpool

import "database/sql"

// NewPoolFromDB creates a new connection pool from an existing sql.DB connection.
//
// WARNING: This function is ONLY for testing purposes to allow wrapping mock databases.
// DO NOT use this in production code. Production code should use NewPool() which
// properly initializes the connection pool from a DSN.
func NewPoolFromDB(db *sql.DB) *Pool {
	return &Pool{db: db}
}
