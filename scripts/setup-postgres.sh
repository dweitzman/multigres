#!/bin/bash
# Copyright 2025 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Find PostgreSQL binaries dynamically (same as GitHub Actions workflow)
# shellcheck disable=SC2012
PGBIN="/usr/lib/postgresql/$(ls /usr/lib/postgresql/ | head -1)/bin"
export PATH="$PGBIN:$PATH"

echo "Starting PostgreSQL..."
# Start PostgreSQL in background using the image's standard data directory
su - postgres -c "PATH=$PATH pg_ctl -D /etc/postgresql -l /var/log/postgresql/postgresql.log start" || true
sleep 5

echo "Setting PostgreSQL password..."
su - postgres -c "PATH=$PATH psql -c \"ALTER USER postgres PASSWORD 'postgres';\"" || true

echo "PostgreSQL is ready"
