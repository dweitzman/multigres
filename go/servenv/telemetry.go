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

package servenv

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	// EnvOTELTracesSampler is the standard OpenTelemetry environment variable for trace sampling
	EnvOTELTracesSampler = "OTEL_TRACES_SAMPLER"

	// EnvCommandOTELTracesSampler is a custom environment variable for CLI commands
	// If set and OTEL_TRACES_SAMPLER is not set, it will be used as the sampler for the CLI command only
	// This allows CLI commands to have different sampling (e.g., always_on) than subprocesses
	EnvCommandOTELTracesSampler = "COMMAND_OTEL_TRACES_SAMPLER"
)

// Telemetry holds OpenTelemetry configuration and state
type Telemetry struct {
	// State
	mu               sync.Mutex
	tracerProvider   *sdktrace.TracerProvider
	meterProvider    *sdkmetric.MeterProvider
	startupParentCtx context.Context
	initialized      bool
}

// NewTelemetry creates a new Telemetry instance
func NewTelemetry() *Telemetry {
	return &Telemetry{}
}

// InitTelemetry initializes OpenTelemetry providers and exporters
// This should be called early in the service lifecycle, typically in OnInit or OnRun hooks
// Configuration is done via standard OpenTelemetry environment variables:
// - OTEL_SERVICE_NAME: Service name (defaults to defaultServiceName)
// - OTEL_EXPORTER_OTLP_ENDPOINT: OTLP endpoint URL
// - OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf" or "grpc"
// - OTEL_TRACES_EXPORTER: "otlp", "console", or "none"
// - OTEL_METRICS_EXPORTER: "otlp", "console", or "none"
// - OTEL_TRACES_SAMPLER: "always_on", "always_off", "traceidratio", "parentbased_always_on", etc.
// - COMMAND_OTEL_TRACES_SAMPLER: CLI-specific sampler (used when OTEL_TRACES_SAMPLER is unset)
func (t *Telemetry) InitTelemetry(ctx context.Context, defaultServiceName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.initialized {
		return nil
	}

	// Determine service name (env var > default)
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	// Create resource with service name and standard attributes
	// Note: We don't merge with resource.Default() to avoid schema version conflicts
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	// Initialize tracing
	if err := t.initTracing(ctx, res); err != nil {
		return fmt.Errorf("failed to initialize tracing: %w", err)
	}

	// Initialize metrics
	if err := t.initMetrics(ctx, res); err != nil {
		return fmt.Errorf("failed to initialize metrics: %w", err)
	}

	// Set up trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Parse startup trace parent if present
	t.parseStartupTraceParent()

	t.initialized = true

	slog.InfoContext(ctx, "OpenTelemetry initialized", "service", serviceName)

	return nil
}

// initTracing initializes the TracerProvider using autoexport
// The exporter is automatically configured based on OTEL_TRACES_EXPORTER and OTEL_EXPORTER_OTLP_PROTOCOL
func (t *Telemetry) initTracing(ctx context.Context, res *resource.Resource) error {
	// Support COMMAND_OTEL_TRACES_SAMPLER for CLI commands
	// This allows CLI commands to have different sampling behavior than subprocesses
	// If COMMAND_OTEL_TRACES_SAMPLER is set and OTEL_TRACES_SAMPLER is not, temporarily use it
	var originalSampler string
	var restoreSampler bool
	if commandSampler := os.Getenv(EnvCommandOTELTracesSampler); commandSampler != "" {
		if os.Getenv(EnvOTELTracesSampler) == "" {
			originalSampler = os.Getenv(EnvOTELTracesSampler)
			os.Setenv(EnvOTELTracesSampler, commandSampler)
			restoreSampler = true
		}
	}
	defer func() {
		if restoreSampler {
			if originalSampler == "" {
				os.Unsetenv(EnvOTELTracesSampler)
			} else {
				os.Setenv(EnvOTELTracesSampler, originalSampler)
			}
		}
	}()

	// Create trace exporter using autoexport
	// This automatically selects the right exporter based on environment variables:
	// - OTEL_TRACES_EXPORTER: "otlp" (default), "console", or "none"
	// - OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf" (default) or "grpc"
	// - OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
	traceExporter, err := autoexport.NewSpanExporter(ctx)
	if err != nil {
		return fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Create TracerProvider with batch span processor
	// Batch processing reduces overhead by grouping spans before export
	// Sampler is automatically configured from OTEL_TRACES_SAMPLER (or COMMAND_OTEL_TRACES_SAMPLER)
	t.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	// Set global tracer provider
	otel.SetTracerProvider(t.tracerProvider)

	return nil
}

// initMetrics initializes the MeterProvider with dual exporters (autoexport + Prometheus)
func (t *Telemetry) initMetrics(ctx context.Context, res *resource.Resource) error {
	// Create Prometheus exporter for pull-based metrics (local debugging)
	// This is always created so we can serve metrics at /metrics endpoint
	promExporter, err := otelprom.New()
	if err != nil {
		return fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	// Create metric reader using autoexport
	// This automatically selects the right exporter based on environment variables:
	// - OTEL_METRICS_EXPORTER: "otlp" (default), "prometheus", "console", or "none"
	// - OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf" (default) or "grpc"
	// - OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
	autoMetricReader, err := autoexport.NewMetricReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to create metric reader: %w", err)
	}

	// Create MeterProvider with both readers
	// Prometheus reader is pull-based (scraped via /metrics endpoint)
	// Auto reader can be OTLP push-based, console, or noop depending on env vars
	t.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),     // Pull-based for local debugging
		sdkmetric.WithReader(autoMetricReader), // Configured via env vars
	)

	// Set global meter provider
	otel.SetMeterProvider(t.meterProvider)

	return nil
}

// parseStartupTraceParent parses the STARTUP_TRACEPARENT environment variable
// and creates a context with the parent span context for service startup tracing
func (t *Telemetry) parseStartupTraceParent() {
	traceparent := os.Getenv("STARTUP_TRACEPARENT")
	if traceparent == "" {
		return
	}

	// Parse W3C Trace Context format: version-trace_id-span_id-flags
	// Example: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
	carrier := propagation.MapCarrier{
		"traceparent": traceparent,
	}

	propagator := otel.GetTextMapPropagator()
	ctx := propagator.Extract(context.Background(), carrier)

	// Check if we successfully extracted a span context
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		t.startupParentCtx = ctx
		slog.Info("Parsed startup trace parent",
			"trace_id", span.SpanContext().TraceID().String(),
			"span_id", span.SpanContext().SpanID().String(),
		)
	} else {
		slog.Warn("Failed to parse STARTUP_TRACEPARENT", "value", traceparent)
	}
}

// GetStartupContext returns a context with the startup trace parent if available
// This allows services to create spans that are children of the provisioner's startup span
func (t *Telemetry) GetStartupContext() context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.startupParentCtx != nil {
		return t.startupParentCtx
	}
	return context.Background()
}

// GetPrometheusHandler returns the HTTP handler for the Prometheus metrics endpoint
// This handler should be registered at /metrics for local debugging
func (t *Telemetry) GetPrometheusHandler() http.Handler {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		// Return a handler that returns 503 if telemetry is not initialized
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("OpenTelemetry not initialized"))
		})
	}

	// The prometheus exporter is automatically registered as a collector with the default registry
	// Use promhttp.HandlerFor() with OpenMetrics enabled to support exemplars
	// Exemplars link metrics to traces by embedding trace_id and span_id in metric samples
	return promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	)
}

// GetTracerProvider returns the configured TracerProvider
func (t *Telemetry) GetTracerProvider() trace.TracerProvider {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tracerProvider == nil {
		return otel.GetTracerProvider()
	}
	return t.tracerProvider
}

// GetTracer returns a named tracer for creating spans
func (t *Telemetry) GetTracer(name string) trace.Tracer {
	return t.GetTracerProvider().Tracer(name)
}

// ShutdownTelemetry gracefully shuts down all telemetry providers
// This ensures all pending spans and metrics are flushed before the service exits
func (t *Telemetry) ShutdownTelemetry(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		return nil
	}

	slog.InfoContext(ctx, "Shutting down OpenTelemetry")

	var errs []error

	// Shutdown tracer provider
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown tracer provider: %w", err))
		}
	}

	// Shutdown meter provider
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown meter provider: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during telemetry shutdown: %v", errs)
	}

	slog.InfoContext(ctx, "OpenTelemetry shutdown complete")
	return nil
}

// WrapSlogHandler wraps an slog.Handler to inject trace context
func (t *Telemetry) WrapSlogHandler(handler slog.Handler) slog.Handler {
	return &traceHandler{wrapped: handler}
}

// traceHandler wraps an slog.Handler to inject trace_id and span_id from context
type traceHandler struct {
	wrapped slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.wrapped.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		r.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	return h.wrapped.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{wrapped: h.wrapped.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{wrapped: h.wrapped.WithGroup(name)}
}

// Global telemetry instance for convenience
var (
	globalTelemetry     *Telemetry
	globalTelemetryOnce sync.Once
)

// GetGlobalTelemetry returns the global telemetry instance
func GetGlobalTelemetry() *Telemetry {
	globalTelemetryOnce.Do(func() {
		globalTelemetry = NewTelemetry()
	})
	return globalTelemetry
}
