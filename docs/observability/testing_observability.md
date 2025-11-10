# Observability Guide

This guide explains how to configure OpenTelemetry and test traces and metrics locally.

## OpenTelemetry Configuration

Multigres uses standard OpenTelemetry environment variables for configuration. Telemetry is always initialized - when exporters are not configured, noop exporters are used with minimal overhead.

For complete documentation, see the [official OTel SDK environment variables reference](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).

### Common Environment Variables

| Variable                      | Description                    | Example                          |
| ----------------------------- | ------------------------------ | -------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint        | `http://localhost:4318`          |
| `OTEL_TRACES_EXPORTER`        | Traces exporter type           | `otlp`, `none` (default: `otlp`) |
| `OTEL_METRICS_EXPORTER`       | Metrics exporter type          | `otlp`, `none` (default: `otlp`) |
| `OTEL_TRACES_SAMPLER`         | Sampling strategy              | `always_on`, `traceidratio`      |
| `OTEL_TRACES_SAMPLER_ARG`     | Sampler ratio for traceidratio | `0.01` (1% sampling)             |
| `OTEL_SERVICE_NAME`           | Service name                   | Auto-detected from binary name   |

### CLI-Specific Sampling

Multigres adds one non-standard variable: **`COMMAND_OTEL_TRACES_SAMPLER`**

This sets trace sampling only for CLI commands without affecting subprocesses they start. Useful for debugging CLI operations with full tracing while keeping services at lower sampling rates.

## Local Testing with Docker Compose

### 1. Start the Observability Stack

```bash
docker-compose -f docker-compose-observability.yml up -d
```

This starts:

- **Jaeger** - Distributed tracing backend
- **Prometheus** - Metrics scraper and storage with exemplar support
- **Grafana** - Visualization with pre-configured dashboards

### 2. Start Multigres with Full Tracing

```bash
# Enable traces and metrics export
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_SAMPLER=always_on
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp

./bin/multigres cluster start
```

**Note:** Cluster startup automatically generates `multigres_local/observability/prometheus.yml` with the correct service ports (which change on each cluster start).

### 3. Access the UIs

| Service        | URL                    | Description                                   |
| -------------- | ---------------------- | --------------------------------------------- |
| **Grafana**    | http://localhost:3000  | Pre-configured dashboards with exemplar links |
| **Prometheus** | http://localhost:9090  | Query metrics and view exemplars              |
| **Jaeger**     | http://localhost:16686 | Search and view distributed traces            |

Grafana has anonymous access enabled - the dashboard loads automatically.

### 4. View Metrics Directly

All services expose Prometheus metrics at `/metrics`:

```bash
# OpenMetrics format with exemplars
curl -H "Accept: application/openmetrics-text" http://localhost:15100/metrics
```

The `/metrics` handler isn't necessarily intended to be used in production. Ideally
production would use environment variables to configure export, however:

- Having `/metrics` is helpful for quick debugging, since it can be pulled on demand at a predicable path
- It doesn't look like env-based autoexport supports EnableOpenMetrics yet, so it may be necessary to contribute code upstream

### 5. Cleanup

```bash
# Stop observability stack
docker-compose -f docker-compose-observability.yml down

# Stop Multigres cluster
./bin/multigres cluster stop
```

## Troubleshooting

### No traces in Jaeger

- Verify Jaeger is running: `curl http://localhost:16686`
- Check OTLP endpoint: `curl http://localhost:4318/v1/traces` (should return 405)
- Verify `OTEL_EXPORTER_OTLP_ENDPOINT` is set
- Check `OTEL_TRACES_SAMPLER` is not `always_off`

### No metrics in Prometheus

- Check Prometheus targets: http://localhost:9090/targets (should be "UP")
- Verify `prometheus.yml` exists: `cat multigres_local/observability/prometheus.yml`
- Restart Prometheus: `docker-compose -f docker-compose-observability.yml restart prometheus`

### No exemplars in Grafana

- Check OpenMetrics format: `curl -H "Accept: application/openmetrics-text" http://localhost:15100/metrics | grep -c '# {trace_id'`
- Verify traces exist in Jaeger (exemplars link metrics to traces)
- Check Prometheus is scraping: http://localhost:9090/targets

### Port conflicts

If ports 3000, 9090, 16686, or 4318 are in use:

```bash
lsof -i :3000 :9090 :16686 :4318
```

## References

- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)
- [OpenTelemetry Go SDK](https://opentelemetry.io/docs/languages/go/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [Prometheus Exemplars](https://prometheus.io/docs/prometheus/latest/feature_flags/#exemplars-storage)
