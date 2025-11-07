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

# Start Jaeger all-in-one for local OpenTelemetry testing
#
# This script starts Jaeger in a Docker container with all necessary ports exposed:
# - 16686: Jaeger UI
# - 4318: OTLP HTTP endpoint
# - 4317: OTLP gRPC endpoint
#
# Usage:
#   ./scripts/start-jaeger.sh        # Start Jaeger in foreground (Ctrl-C to stop)
#   ./scripts/start-jaeger.sh -d     # Start Jaeger in background (daemon mode)
#   ./scripts/start-jaeger.sh stop   # Stop background Jaeger

set -euo pipefail

CONTAINER_NAME="jaeger-multigres"
IMAGE="jaegertracing/jaeger:2.3.0"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to check if Docker is available
check_docker() {
  if ! command -v docker &>/dev/null; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    echo "Please install Docker or use the binary method from docs/observability/local-testing-guide.md"
    exit 1
  fi
}

# Function to check if container is already running
is_running() {
  docker ps --filter "name=${CONTAINER_NAME}" --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"
}

# Function to start Jaeger in foreground
start_foreground() {
  if is_running; then
    echo -e "${YELLOW}Jaeger is already running. Stop it first with: $0 stop${NC}"
    exit 1
  fi

  echo -e "${GREEN}Starting Jaeger all-in-one...${NC}"
  echo "  Jaeger UI:         http://localhost:16686"
  echo "  OTLP HTTP:         http://localhost:4318"
  echo "  OTLP gRPC:         http://localhost:4317"
  echo ""
  echo "Press Ctrl-C to stop Jaeger"
  echo ""

  # Use --rm to auto-cleanup when stopped
  docker run --rm --name "${CONTAINER_NAME}" \
    -p 16686:16686 \
    -p 4318:4318 \
    -p 4317:4317 \
    "${IMAGE}"
}

# Function to start Jaeger in background
start_background() {
  if is_running; then
    echo -e "${YELLOW}Jaeger is already running.${NC}"
    show_status
    exit 0
  fi

  echo -e "${GREEN}Starting Jaeger all-in-one in background...${NC}"
  docker run -d --name "${CONTAINER_NAME}" \
    -p 16686:16686 \
    -p 4318:4318 \
    -p 4317:4317 \
    "${IMAGE}" >/dev/null

  echo -e "${GREEN}✓ Jaeger started successfully${NC}"
  show_status
}

# Function to stop Jaeger
stop() {
  if ! is_running; then
    echo -e "${YELLOW}Jaeger is not running${NC}"
    exit 0
  fi

  echo -e "${GREEN}Stopping Jaeger...${NC}"
  docker stop "${CONTAINER_NAME}" >/dev/null
  docker rm "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  echo -e "${GREEN}✓ Jaeger stopped${NC}"
}

# Function to show status
show_status() {
  echo ""
  echo "Jaeger endpoints:"
  echo "  UI:         http://localhost:16686"
  echo "  OTLP HTTP:  http://localhost:4318"
  echo "  OTLP gRPC:  http://localhost:4317"
  echo ""
  echo "View logs with: docker logs -f ${CONTAINER_NAME}"
  echo "Stop with:      $0 stop"
}

# Main script logic
check_docker

case "${1:-}" in
-d | --daemon)
  start_background
  ;;
stop)
  stop
  ;;
status)
  if is_running; then
    echo -e "${GREEN}Jaeger is running${NC}"
    show_status
  else
    echo -e "${YELLOW}Jaeger is not running${NC}"
    echo "Start with: $0"
  fi
  ;;
-h | --help)
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Start Jaeger all-in-one for OpenTelemetry testing"
  echo ""
  echo "Options:"
  echo "  (no args)     Start Jaeger in foreground (Ctrl-C to stop)"
  echo "  -d, --daemon  Start Jaeger in background"
  echo "  stop          Stop background Jaeger"
  echo "  status        Check if Jaeger is running"
  echo "  -h, --help    Show this help message"
  ;;
"")
  start_foreground
  ;;
*)
  echo -e "${RED}Unknown option: $1${NC}"
  echo "Use -h or --help for usage information"
  exit 1
  ;;
esac
