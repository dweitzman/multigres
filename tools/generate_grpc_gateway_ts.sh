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

set -euo pipefail

MTROOT="${MTROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PROTO_DIR="$MTROOT/proto"
WEB_DIR="$MTROOT/web/multiadmin"
OUT_DIR="$WEB_DIR/lib/api/generated"

# Ensure output directory exists
mkdir -p "$OUT_DIR"

# Find protoc-gen-grpc-gateway-ts in $MTROOT/bin (installed by make tools)
PLUGIN_PATH="$MTROOT/bin/protoc-gen-grpc-gateway-ts"
if [ ! -f "$PLUGIN_PATH" ]; then
  echo "Error: protoc-gen-grpc-gateway-ts not found at $PLUGIN_PATH"
  echo "Run: make tools"
  exit 1
fi

# PROTOC_VER must be set by Makefile
if [ -z "${PROTOC_VER:-}" ]; then
  echo "Error: PROTOC_VER environment variable not set"
  echo "This script should be called via 'make proto-ts'"
  exit 1
fi

echo "Generating TypeScript gRPC-Gateway client from proto files..."

# Generate TypeScript client from proto files
"$MTROOT/dist/protoc-$PROTOC_VER/bin/protoc" \
  --plugin="$PLUGIN_PATH" \
  --grpc-gateway-ts_out="$OUT_DIR" \
  --grpc-gateway-ts_opt=use_proto_names=false \
  --grpc-gateway-ts_opt=fetch_module_path=./fetch \
  --proto_path="$PROTO_DIR" \
  "$PROTO_DIR/multiadminservice.proto" \
  "$PROTO_DIR/clustermetadata.proto" \
  "$PROTO_DIR/multipoolermanagerdata.proto"

echo "✓ TypeScript gRPC-Gateway client generated: $OUT_DIR"
