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

	"github.com/multigres/multigres/go/viperutil"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/spf13/pflag"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry holds OpenTelemetry configuration and state
type Telemetry struct {
	// Configuration
	enabled     viperutil.Value[bool]
	serviceName viperutil.Value[string]
	endpoint    viperutil.Value[string]

	// State
	mu               sync.Mutex
	tracerProvider   *sdktrace.TracerProvider
	meterProvider    *sdkmetric.MeterProvider
	startupParentCtx context.Context
	initialized      bool
}

// NewTelemetry creates a new Telemetry instance with default configuration
func NewTelemetry() *Telemetry {
	return &Telemetry{
		enabled: viperutil.Configure("otel-enabled", viperutil.Options[bool]{
			Default:  false,
			FlagName: "otel-enabled",
			Dynamic:  false,
		}),
		serviceName: viperutil.Configure("otel-service-name", viperutil.Options[string]{
			Default:  "",
			FlagName: "otel-service-name",
			Dynamic:  false,
		}),
		endpoint: viperutil.Configure("otel-endpoint", viperutil.Options[string]{
			Default:  "http://localhost:4318",
			FlagName: "otel-endpoint",
			Dynamic:  false,
		}),
	}
}

// RegisterFlags registers telemetry-related command line flags
func (t *Telemetry) RegisterFlags(fs *pflag.FlagSet) {
	fs.Bool("otel-enabled", t.enabled.Default(), "Enable OpenTelemetry instrumentation")
	fs.String("otel-service-name", t.serviceName.Default(), "Service name for OpenTelemetry (overrides OTEL_SERVICE_NAME)")
	fs.String("otel-endpoint", t.endpoint.Default(), "OTLP endpoint for OpenTelemetry (overrides OTEL_EXPORTER_OTLP_ENDPOINT)")
	viperutil.BindFlags(fs, t.enabled, t.serviceName, t.endpoint)
}

// IsEnabled returns whether OpenTelemetry is enabled
// OTel is enabled if either:
// 1. The --otel-enabled flag is explicitly set to true, OR
// 2. The OTEL_EXPORTER_OTLP_ENDPOINT environment variable is set (unless --otel-enabled=false)
//
// This allows the environment variable to implicitly enable telemetry while still allowing
// explicit disabling via the flag.
func (t *Telemetry) IsEnabled() bool {
	// If flag was explicitly set (true or false), respect it
	if t.enabled.Get() {
		return true
	}

	// Auto-enable if OTEL_EXPORTER_OTLP_ENDPOINT is set
	// This follows OpenTelemetry best practices where setting the exporter endpoint
	// implicitly enables telemetry
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
}

// InitTelemetry initializes OpenTelemetry providers and exporters
// This should be called early in the service lifecycle, typically in OnInit or OnRun hooks
func (t *Telemetry) InitTelemetry(ctx context.Context, defaultServiceName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.initialized {
		return nil
	}

	// Use IsEnabled() instead of t.enabled.Get() to check if OTel should be initialized
	// IsEnabled() also checks for OTEL_EXPORTER_OTLP_ENDPOINT environment variable
	if !t.IsEnabled() {
		slog.DebugContext(ctx, "OpenTelemetry is disabled, skipping initialization")
		return nil
	}

	// Determine service name (flag > env var > default)
	serviceName := t.serviceName.Get()
	if serviceName == "" {
		serviceName = os.Getenv("OTEL_SERVICE_NAME")
	}
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	// Determine OTLP endpoint (flag > env var)
	endpoint := t.endpoint.Get()
	if envEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); envEndpoint != "" {
		endpoint = envEndpoint
	}

	// Create resource with service name and standard attributes
	// Note: We don't merge with resource.Default() to avoid schema version conflicts
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	// Initialize tracing
	if err := t.initTracing(ctx, endpoint, res); err != nil {
		return fmt.Errorf("failed to initialize tracing: %w", err)
	}

	// Initialize metrics
	if err := t.initMetrics(ctx, endpoint, res); err != nil {
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

	slog.InfoContext(ctx, "OpenTelemetry initialized",
		"service", serviceName,
		"endpoint", endpoint,
	)

	return nil
}

// initTracing initializes the TracerProvider with OTLP HTTP exporter
func (t *Telemetry) initTracing(ctx context.Context, endpoint string, res *resource.Resource) error {
	// Create OTLP HTTP trace exporter
	// WithEndpointURL accepts the full URL including protocol
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Create TracerProvider with batch span processor
	// Batch processing reduces overhead by grouping spans before export
	t.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		// Sampler is configured via OTEL_TRACES_SAMPLER env var
		// Defaults to parentbased_always_on if not set
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)

	// Set global tracer provider
	otel.SetTracerProvider(t.tracerProvider)

	return nil
}

// initMetrics initializes the MeterProvider with dual exporters (OTLP + Prometheus)
func (t *Telemetry) initMetrics(ctx context.Context, endpoint string, res *resource.Resource) error {
	// Create Prometheus exporter for pull-based metrics (local debugging)
	promExporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	// Create OTLP HTTP metric exporter for push-based metrics (production)
	// WithEndpointURL accepts the full URL including protocol
	otlpExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	// Create MeterProvider with both readers
	// Prometheus reader is pull-based (scraped via /metrics endpoint)
	// OTLP reader is push-based (periodically sends to collector)
	t.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),                              // Pull-based for local debugging
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExporter)), // Push-based for production
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
	// Use promhttp.Handler() to serve metrics from the default prometheus registry
	return promhttp.Handler()
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
	if !t.enabled.Get() {
		return handler
	}
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

// RegisterTelemetryFlags registers telemetry flags on the global instance
func RegisterTelemetryFlags(fs *pflag.FlagSet) {
	GetGlobalTelemetry().RegisterFlags(fs)
}
