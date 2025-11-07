# OpenTelemetry Implementation Plan

## Overview

This document outlines the plan for adding comprehensive observability to Multigres services using OpenTelemetry. The implementation will provide:

- **Distributed Tracing**: End-to-end request tracking across all services
- **Metrics with Exemplars**: Time series metrics linked to example traces
- **Structured Logging**: Automatic trace context injection into slog logs
- **Auto-instrumentation**: Minimal-effort instrumentation for gRPC and HTTP handlers

## Architecture Context

### Current State

- **6 Go Services**: multiadmin, multigateway, multipooler, multiorch, pgctld, multigres CLI
- **Centralized Infrastructure**: All services use `servenv.ServEnv` and `servenv.GrpcServer`
- **Logging**: slog already implemented with structured JSON logging
- **gRPC**: Interceptor chain in place with TODO for tracing at line 375
- **HTTP**: Standard ServeMux pattern with `/live`, `/ready`, `/config` endpoints
- **Service Discovery**: etcd-based topology with health checking

### Integration Points

1. `go/servenv/telemetry.go` - New file for OTel setup
2. `go/servenv/grpc_server.go:375` - gRPC interceptor insertion point
3. `go/servenv/http.go` - HTTP middleware at ServeMux level
4. `go/servenv/logging.go` - slog bridge for trace context injection
5. `go/provisioner/local/local.go` - Startup trace propagation
6. `go/provisioner/local/healthcheck.go` - Health check instrumentation

## Implementation Phases

### Phase 1: Core OTel Infrastructure

#### 1.1 Dependencies

The following dependencies will be added automatically via imports and `go mod tidy`:

- `go.opentelemetry.io/otel` - Core OTel API
- `go.opentelemetry.io/otel/sdk/trace` - Tracing SDK
- `go.opentelemetry.io/otel/sdk/metric` - Metrics SDK
- `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp` - OTLP trace exporter
- `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp` - OTLP metric exporter
- `go.opentelemetry.io/otel/exporters/prometheus` - Prometheus exporter
- `go.opentelemetry.io/contrib/bridges/otelslog` - slog integration
- `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` - gRPC instrumentation
- `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` - HTTP instrumentation

After implementing the code, run: `go mod tidy`

#### 1.2 Create `go/servenv/telemetry.go`

**Responsibilities**:

- Initialize `TracerProvider` with OTLP HTTP exporter
- Initialize `MeterProvider` with dual exporters:
  - OTLP HTTP exporter (push-based, for production collection)
  - Prometheus exporter (pull-based, for local debugging)
- Enable exemplar support to link metrics to traces
- Provide slog handler wrapper for automatic trace context injection
- Support standard OTel environment variables:
  - `OTEL_EXPORTER_OTLP_ENDPOINT`
  - `OTEL_TRACES_SAMPLER` (always_on, always_off, traceidratio, parentbased_always_on, etc.)
  - `OTEL_SERVICE_NAME`
  - `OTEL_RESOURCE_ATTRIBUTES`
- Add CLI flags:
  - `--otel-enabled` (bool, default: false)
  - `--otel-service-name` (string, overrides OTEL_SERVICE_NAME)
  - `--otel-endpoint` (string, overrides OTEL_EXPORTER_OTLP_ENDPOINT)
- Parse `STARTUP_TRACEPARENT` environment variable for parent trace context
- Register shutdown hooks using `servenv.OnClose()`

**Key Functions**:

- `InitTelemetry(serviceName string) error` - Initialize all providers
- `GetPrometheusHandler() http.Handler` - Return handler for /metrics endpoint
- `ShutdownTelemetry(ctx context.Context) error` - Flush and cleanup
- `GetTracerProvider() trace.TracerProvider`
- `GetMeterProvider() metric.MeterProvider`

#### 1.3 Integrate gRPC Interceptors (`go/servenv/grpc_server.go`)

**Location**: Line 375 (TODO comment)

**Changes**:

```go
// Add OTel interceptors to the chain
if otelEnabled {
    builder.addUnary(otelgrpc.UnaryServerInterceptor())
    builder.addStream(otelgrpc.StreamServerInterceptor())
}
```

**Features**:

- Automatic span creation for each gRPC method
- Trace context propagation via gRPC metadata
- Error recording in spans
- Method name, status code as span attributes

#### 1.4 Instrument HTTP ServeMux (`go/servenv/http.go`)

**Changes**:

1. Wrap `NewServerMux()` creation with `otelhttp` middleware
2. Add Prometheus `/metrics` endpoint as optional debug handler
3. Add `--prometheus-http` flag (similar to existing `--pprof-http`)

**Features**:

- Automatic span creation for HTTP requests
- HTTP method, path, status code as span attributes
- Prometheus endpoint exposes:
  - gRPC method latencies (with exemplars)
  - HTTP endpoint latencies (with exemplars)
  - Custom application metrics
  - Go runtime metrics

#### 1.5 Update Logging (`go/servenv/logging.go`)

**Changes**:

- Wrap slog handler with OTel bridge
- Automatically inject `trace_id` and `span_id` into all log entries

**Result**:

```json
{
  "time": "2025-01-07T10:30:45Z",
  "level": "INFO",
  "msg": "Processing request",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "service": "multipooler",
  "database": "db1"
}
```

### Phase 2: Startup Trace Propagation

#### 2.1 Modify Local Provisioner (`go/provisioner/local/local.go`)

**Goal**: Create a distributed trace that spans cluster bootstrap

**Implementation**:

1. In `Bootstrap()`, create a root span: `cluster_startup`
2. Create child spans for major operations:
   - `start_etcd`
   - `initialize_topology`
   - `start_multiadmin`
   - `start_database_services` (per database)
     - `start_multigateway`
     - `start_multipooler`
     - `start_pgctld`
     - `start_multiorch`
3. For each subprocess start:
   - Extract trace context to W3C Trace Context format
   - Pass via `STARTUP_TRACEPARENT` environment variable
   - Example: `STARTUP_TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`

**Result**: Single trace showing entire cluster startup timeline with all services

#### 2.2 Parse Startup Trace Context (`go/servenv/telemetry.go`)

**Implementation**:

- Check for `STARTUP_TRACEPARENT` env var during `InitTelemetry()`
- Parse using W3C Trace Context format
- Set as parent context for service's root span
- Service's initialization spans (health checks, gRPC setup, etc.) become children

#### 2.3 Instrument Health Checks (`go/provisioner/local/healthcheck.go`)

**Changes**:

- Wrap `waitForServiceReady()` in span
- Create child spans for each check type:
  - `tcp_connectivity_check`
  - `http_health_check` (/live endpoint)
  - `grpc_health_check` (pgctld Status)
  - `etcd_health_check` (/health endpoint)
- Record attributes:
  - `service.name`
  - `service.port`
  - `check.type`
  - `check.duration_ms`
  - `check.success`
  - `check.retry_count`

**Result**: Visibility into health check latencies and failures during startup

### Phase 3: Testing & Verification

#### 3.1 Local Testing Setup

**Option 1: Jaeger All-in-One (Recommended for Development)**

```bash
# Download Jaeger binary (no Docker required)
# https://www.jaegertracing.io/download/

# Start Jaeger (includes collector, query, UI)
./jaeger-all-in-one

# Access UI: http://localhost:16686
# OTLP endpoint: http://localhost:4318 (HTTP) or 4317 (gRPC)
```

**Option 2: Prometheus + Jaeger via Docker Compose**

```yaml
# docs/observability/docker-compose.yml
version: "3"
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686" # UI
      - "4318:4318" # OTLP HTTP
      - "4317:4317" # OTLP gRPC

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--enable-feature=exemplar-storage" # Enable exemplars
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
```

**Option 3: Console Exporter (Stdout Debugging)**

```bash
# Set env var to output traces/metrics to stdout
export OTEL_TRACES_EXPORTER=console
export OTEL_METRICS_EXPORTER=console
```

#### 3.2 Example Test Scenario

**Setup**:

```bash
# Start Jaeger
./jaeger-all-in-one &

# Configure OTel for services
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_SAMPLER=always_on
export OTEL_SERVICE_NAME=multigres-local

# Start local cluster with OTel enabled
./bin/multigres cluster start --otel-enabled --prometheus-http
```

**Verification Steps**:

1. **Check Startup Trace**:
   - Open Jaeger UI: http://localhost:16686
   - Search for service: `multigres-local`
   - Find trace with operation: `cluster_startup`
   - Verify child spans for all services
   - Check span durations and attributes

2. **Check Trace IDs in Logs**:

   ```bash
   tail -f logs/multipooler_*.log | jq '.trace_id'
   ```

   - Verify trace_id and span_id present in all log entries

3. **Check Prometheus Metrics**:

   ```bash
   curl http://localhost:8080/metrics
   ```

   - Look for `rpc_server_duration_*` (gRPC metrics)
   - Look for `http_server_duration_*` (HTTP metrics)
   - Verify exemplars present (commented trace IDs)

4. **Send Test Request**:

   ```bash
   # Trigger a gRPC call that crosses services
   # Check Jaeger for distributed trace across multigateway → multipooler
   ```

5. **Navigate from Metrics to Traces**:
   - Find a high-latency metric in Prometheus
   - Copy exemplar trace ID
   - Search for that trace ID in Jaeger
   - Investigate the slow request

#### 3.3 Expected Outcomes

✅ **Distributed Tracing**:

- Single trace spans from provisioner through all service startups
- gRPC calls show client-server span relationships
- HTTP requests traced end-to-end

✅ **Logs with Trace Context**:

- All slog entries include `trace_id` and `span_id`
- Can correlate logs to traces in Jaeger
- Filtered log viewing by trace ID

✅ **Metrics with Exemplars**:

- Prometheus `/metrics` endpoint shows histograms
- Exemplars link high-latency requests to trace IDs
- Can investigate outliers via exemplar traces

✅ **Automatic Instrumentation**:

- No code changes required in gRPC method implementations
- HTTP handlers automatically traced
- Health checks show up in startup trace

## Configuration Reference

### Environment Variables (Standard OTel)

| Variable                      | Description                    | Example                         |
| ----------------------------- | ------------------------------ | ------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint        | `http://localhost:4318`         |
| `OTEL_TRACES_SAMPLER`         | Sampling strategy              | `always_on`, `traceidratio=0.1` |
| `OTEL_SERVICE_NAME`           | Service name in traces         | `multipooler`                   |
| `OTEL_RESOURCE_ATTRIBUTES`    | Additional resource attributes | `deployment.environment=local`  |

### Custom Environment Variables

| Variable              | Description                      | Example                          |
| --------------------- | -------------------------------- | -------------------------------- |
| `STARTUP_TRACEPARENT` | Parent trace context for startup | `00-4bf92f...736-00f06...2b7-01` |

### CLI Flags

| Flag                  | Description                         | Default                 |
| --------------------- | ----------------------------------- | ----------------------- |
| `--otel-enabled`      | Enable OpenTelemetry                | `false`                 |
| `--otel-service-name` | Override service name               | (auto-detected)         |
| `--otel-endpoint`     | Override OTLP endpoint              | `http://localhost:4318` |
| `--prometheus-http`   | Enable Prometheus /metrics endpoint | `false`                 |

## Implementation Notes

### Exemplar Configuration

Exemplars require:

1. Metric SDK configured with exemplar reservoir
2. Trace context available when recording metric
3. Prometheus exporter with exemplar support enabled

```go
// In telemetry.go
reader := prometheus.New(
    prometheus.WithoutScopeInfo(),
    prometheus.WithResourceAsConstantLabels(true),
)

// Metrics automatically record exemplars when traces are active
```

### Shutdown Hooks

Proper shutdown ensures all telemetry is flushed:

```go
servenv.OnClose(func() {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := telemetry.ShutdownTelemetry(ctx); err != nil {
        logger.Error("failed to shutdown telemetry", "error", err)
    }
})
```

### Performance Considerations

- **Sampling**: Use `parentbased_traceidratio=0.01` for production (1% sampling)
- **Batch Export**: OTLP exporter batches spans/metrics to reduce overhead
- **Exemplar Limit**: Default 1 exemplar per metric bucket (configurable)
- **Overhead**: ~1-5% CPU overhead with 1% sampling, ~10-20% with always_on

## Future Enhancements

### Phase 4: Custom Metrics

- Query latency histograms per database
- Connection pool metrics (active, idle, waiting)
- Replication lag metrics
- Consensus view change metrics

### Phase 5: Database Query Tracing

- Instrument PostgreSQL queries in multipooler
- Add SQL as span attributes (sanitized)
- Track query plans for slow queries

### Phase 6: Production Deployment

- Configure OTLP exporter for production collector
- Set up sampling strategy (head-based or tail-based)
- Configure metric aggregation intervals
- Set up alerting on SLIs

## References

- [OpenTelemetry Go Documentation](https://opentelemetry.io/docs/instrumentation/go/)
- [OTel gRPC Instrumentation](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc)
- [OTel HTTP Instrumentation](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp)
- [OTel slog Bridge](https://pkg.go.dev/go.opentelemetry.io/contrib/bridges/otelslog)
- [Prometheus Exemplars](https://prometheus.io/docs/prometheus/latest/feature_flags/#exemplars-storage)
- [Jaeger Deployment](https://www.jaegertracing.io/docs/deployment/)
