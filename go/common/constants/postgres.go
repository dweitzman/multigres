// Copyright 2025 Supabase, Inc.
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

package constants

// DefaultAdminUser is the default PostgreSQL superuser created during initdb.
// This user is used by multigres internally for administrative operations.
// The "postgres" user is created separately as a demoted user for operator/customer use.
const DefaultAdminUser = "multigres_admin"
