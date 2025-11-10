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
	"net/http"
	"time"

	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// MultigresCommand holds the configuration for multigres commands
type MultigresCommand struct {
	vc        *viperutil.ViperConfig
	telemetry *servenv.Telemetry
}

// GetRootCommand creates and returns the root command for multigres with all subcommands
func GetRootCommand() *cobra.Command {
	mc := &MultigresCommand{
		vc:        viperutil.NewViperConfig(),
		telemetry: servenv.NewTelemetry(),
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

			// Initialize OpenTelemetry
			// Configuration is done via standard OTEL environment variables
			// For CLI commands, you can set COMMAND_OTEL_TRACES_SAMPLER=always_on for better debugging
			// unless explicitly overridden by OTEL_TRACES_SAMPLER
			if err := mc.telemetry.InitTelemetry(context.Background(), "multigres-cli"); err != nil {
				return fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
			}

			// Wrap the default HTTP client transport with OTel instrumentation
			// This ensures all HTTP requests made by the CLI and provisioner propagate trace context
			http.DefaultClient.Transport = otelhttp.NewTransport(http.DefaultTransport)

			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			// Shutdown OpenTelemetry to flush all pending spans
			// This is critical for CLI commands to export traces before process exit
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mc.telemetry.ShutdownTelemetry(ctx); err != nil {
				return fmt.Errorf("failed to shutdown OpenTelemetry: %w", err)
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
