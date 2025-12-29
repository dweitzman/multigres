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

# Wrapper to run multigres commands with OpenTelemetry telemetry enabled
#
# This script configures OTLP export of traces and metrics to:
# - Jaeger (traces): http://localhost:4318
# - Prometheus (metrics): http://localhost:9090/api/v1/otlp/v1/metrics
#
# Prerequisites (from the repository root):
#   docker compose -f docker-compose-observability.yml up -d
#
# Usage:
#   ./observability/multigres-with-telemetry.sh cluster start
#   ./observability/multigres-with-telemetry.sh cluster stop
#   ./observability/multigres-with-telemetry.sh cluster restart
#
# For faster metric updates during demos (default 60s), set:
#   OTEL_METRIC_EXPORT_INTERVAL=5000 ./observability/multigres-with-telemetry.sh cluster start

set -e

# Default to 5s for demos if not set
export OTEL_METRIC_EXPORT_INTERVAL="${OTEL_METRIC_EXPORT_INTERVAL:-5000}"

# Configure OTLP export
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://localhost:4318/v1/traces"
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://localhost:9090/api/v1/otlp/v1/metrics"
export OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE="cumulative"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_METRICS_EXPORTER="otlp"
export OTEL_TRACES_SAMPLER="always_on"

# Use exponential histograms for native histogram support in Prometheus
export OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION="base2_exponential_bucket_histogram"

echo "Running multigres with OpenTelemetry enabled:"
echo "  Traces  -> ${OTEL_EXPORTER_OTLP_TRACES_ENDPOINT}"
echo "  Metrics -> ${OTEL_EXPORTER_OTLP_METRICS_ENDPOINT}"
echo "  Metric export interval: ${OTEL_METRIC_EXPORT_INTERVAL}ms"
echo "  Histogram aggregation: exponential (native histograms)"
echo ""

./bin/multigres "$@"
