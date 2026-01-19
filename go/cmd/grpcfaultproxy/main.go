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

// grpcfaultproxy provides a transparent gRPC proxy with fault injection capabilities
// for testing failure scenarios in multigres clusters.
package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/services/grpcfaultproxy"
)

func main() {
	cmd := &cobra.Command{
		Use:   "grpcfaultproxy",
		Short: "Transparent proxy with fault injection for testing (gRPC and PostgreSQL)",
		Long: `grpcfaultproxy is a transparent proxy that intercepts gRPC and PostgreSQL traffic
and allows fault injection (latency, errors, drops) for testing failure scenarios.

gRPC services configure the proxy using the HTTPS_PROXY environment variable.
PostgreSQL replicas configure the proxy using the FORCE_POSTGRES_PROXY environment variable.`,
		Args: cobra.NoArgs,
		RunE: run,
	}

	// Configuration flags
	var config grpcfaultproxy.Config
	cmd.Flags().StringVar(&config.HTTPAddr, "http-addr", ":17000",
		"Address to listen on for HTTP CONNECT requests (gRPC proxy)")
	cmd.Flags().StringVar(&config.ManagementAddr, "management-addr", ":17001",
		"Address to listen on for management gRPC API (optional, empty to disable)")
	cmd.Flags().StringVar(&config.PostgresAddr, "postgres-addr", "",
		"Address to listen on for PostgreSQL wire protocol proxy (optional, empty to disable)")
	cmd.Flags().StringVar(&config.RulesFile, "rules-file", "",
		"Path to fault injection rules YAML file (optional)")

	if err := cmd.Execute(); err != nil {
		slog.Error("command failed", "error", err)
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Get config from flags
	httpAddr, _ := cmd.Flags().GetString("http-addr")
	managementAddr, _ := cmd.Flags().GetString("management-addr")
	postgresAddr, _ := cmd.Flags().GetString("postgres-addr")
	rulesFile, _ := cmd.Flags().GetString("rules-file")

	config := grpcfaultproxy.Config{
		HTTPAddr:       httpAddr,
		ManagementAddr: managementAddr,
		PostgresAddr:   postgresAddr,
		RulesFile:      rulesFile,
	}

	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create proxy
	proxy := grpcfaultproxy.New(config, logger)

	// Start proxy
	if err := proxy.Start(); err != nil {
		return err
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("proxy running, press Ctrl+C to stop")

	// Wait for signal
	<-sigChan
	logger.Info("received shutdown signal")

	// Stop proxy gracefully
	return proxy.Stop()
}
