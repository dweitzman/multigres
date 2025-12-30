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

# Helper script for managing the observability stack (Jaeger, Prometheus, Grafana)

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose-observability.yml"

case "${1:-}" in
up)
  docker compose -f "$COMPOSE_FILE" up -d
  echo ""
  echo "Observability stack started:"
  echo "  Grafana:    http://localhost:3000"
  echo "  Jaeger:     http://localhost:16686"
  echo "  Prometheus: http://localhost:9090"
  ;;
down)
  docker compose -f "$COMPOSE_FILE" down
  ;;
ps)
  docker compose -f "$COMPOSE_FILE" ps
  ;;
restart)
  docker compose -f "$COMPOSE_FILE" restart
  ;;
logs)
  docker compose -f "$COMPOSE_FILE" logs "${@:2}"
  ;;
*)
  echo "Usage: $0 {up|down|ps|restart|logs}"
  exit 1
  ;;
esac
