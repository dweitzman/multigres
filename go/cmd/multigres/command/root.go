// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// MultigresCommand holds the configuration for multigres commands
type MultigresCommand struct {
	vc             *viperutil.ViperConfig
	tracerProvider *sdktrace.TracerProvider
}

// GetRootCommand creates and returns the root command for multigres with all subcommands
func GetRootCommand() *cobra.Command {
	mc := &MultigresCommand{
		vc: viperutil.NewViperConfig(),
	}

	root := &cobra.Command{
		Use:   "multigres",
		Short: "The command-line companion for managing and developing with Multigres clusters",
		Long: `The Multigres CLI makes distributed Postgres feel as easy as running Postgres locally.

A single binary that gives developers confidence when experimenting,
and operators the tools to keep clusters healthy at scale.

Get started with:
  multigres cluster init    # Create a local cluster configuration
  multigres cluster up      # Start your local cluster

Configuration:
  Multigres automatically searches for configuration files in this order:
  1. File specified by --config-file flag (if provided)
  2. Files named 'multigres' with supported extensions (.yaml, .yml, .json, .toml)
     in directories specified by --config-path flags
  3. Current working directory (default search path)

  Environment variable MT_CONFIG_NAME can override the config filename.
  Use --config-file-not-found-handling to control behavior when no config is found.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Silence usage for application errors, but allow it for flag errors
			// This gets called after flag parsing, so flag errors will still show usage
			cmd.SilenceUsage = true

			// Set multigres-specific config name
			viper.SetConfigName("multigres")

			// Load config (without the full servenv setup)
			_, err := mc.vc.LoadConfig()
			if err != nil {
				return err
			}

			// Initialize OpenTelemetry if OTEL_EXPORTER_OTLP_ENDPOINT is set
			// For CLI commands, we only need traces (no metrics/Prometheus endpoint)
			if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
				if err := mc.initTelemetry(endpoint); err != nil {
					fmt.Fprintf(os.Stderr, "Warning: Failed to initialize OpenTelemetry: %v\n", err)
				}
			}

			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			// Shutdown OpenTelemetry to flush all pending spans
			// This is critical for CLI commands to export traces before process exit
			if mc.tracerProvider != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := mc.tracerProvider.Shutdown(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "Warning: Failed to shutdown OpenTelemetry: %v\n", err)
				}
			}
			return nil
		},
	}

	// Add any other servenv flags
	mc.vc.RegisterFlags(root.PersistentFlags())

	// Override the default display value for multigres
	if flag := root.PersistentFlags().Lookup("config-name"); flag != nil {
		flag.DefValue = "multigres"
	}

	// Add all subcommands
	AddClusterCommand(root, mc)
	AddTopoCommands(root, mc)

	return root
}

// initTelemetry sets up OpenTelemetry tracing for CLI commands
// Unlike services, CLI commands only need traces (no metrics or Prometheus endpoint)
func (mc *MultigresCommand) initTelemetry(endpoint string) error {
	ctx := context.Background()

	// Create resource with service name
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("multigres-cli"),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

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
	mc.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		// Sampler is configured via OTEL_TRACES_SAMPLER env var
		// Defaults to parentbased_always_on if not set
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)

	// Set global tracer provider so provisioner can create spans
	otel.SetTracerProvider(mc.tracerProvider)

	// Set up W3C Trace Context propagation
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return nil
}
