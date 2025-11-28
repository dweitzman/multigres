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

// Package executil provides safe subprocess execution with graceful termination,
// explicit environment variable handling, and OpenTelemetry trace propagation.
//
// Use this package instead of exec.CommandContext directly. On context cancellation,
// commands receive SIGTERM first and escalate to SIGKILL after a grace period,
// allowing subprocesses to flush logs, send telemetry, and clean up.
//
// Environment variables are handled explicitly via AddEnv() and SetEnv() methods,
// avoiding the subtle pitfalls of exec.Cmd.Env (where nil means "inherit" but
// non-nil means "replace entirely").
//
// Trace context is automatically propagated to subprocesses via the TRACEPARENT
// environment variable if the context contains a valid span.
package executil

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// DefaultGracePeriod is the time to wait after SIGTERM before escalating to SIGKILL.
const DefaultGracePeriod = 10 * time.Second

const tracingServiceName = "multigres"

var tracer = otel.Tracer(tracingServiceName)

// Cmd wraps exec.Cmd with a builder pattern for safe configuration.
// Create with Command() or CommandWithGracePeriod().
type Cmd struct {
	*exec.Cmd
	ctx        context.Context
	extraEnv   []string
	clientSpan bool
}

// Command creates a new Cmd with graceful termination on context cancellation.
// On cancellation: SIGTERM is sent first, then SIGKILL after DefaultGracePeriod.
//
// By default, the command inherits the parent process environment.
// Use AddEnv() to add variables, or SetEnv() to replace the entire environment.
func Command(ctx context.Context, name string, args ...string) *Cmd {
	return CommandWithGracePeriod(ctx, DefaultGracePeriod, name, args...)
}

// CommandWithGracePeriod creates a Cmd with a custom grace period between
// SIGTERM and SIGKILL. Use a shorter grace period for commands that should
// terminate quickly (e.g., 100ms for simple queries).
func CommandWithGracePeriod(ctx context.Context, gracePeriod time.Duration, name string, args ...string) *Cmd {
	cmd := exec.CommandContext(ctx, name, args...)

	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = gracePeriod

	return &Cmd{Cmd: cmd, ctx: ctx}
}

// AddEnv adds environment variables to the command. Variables are specified
// as "KEY=value" strings. Safe to call multiple times - variables accumulate.
//
// Variables are added on top of the inherited environment (or the explicit
// base if SetEnv was called). The actual environment is finalized when
// Start/Run/Output/CombinedOutput is called.
func (c *Cmd) AddEnv(keyvals ...string) *Cmd {
	c.extraEnv = append(c.extraEnv, keyvals...)
	return c
}

// SetEnv replaces the entire environment with the provided variables.
// The command will NOT inherit any environment from the parent process.
//
// Call AddEnv() after SetEnv() to add additional variables on top of
// this explicit base.
func (c *Cmd) SetEnv(env []string) *Cmd {
	c.Cmd.Env = env
	return c
}

// SetDir sets the working directory for the command.
func (c *Cmd) SetDir(dir string) *Cmd {
	c.Cmd.Dir = dir
	return c
}

// SetStdin sets the stdin for the command.
func (c *Cmd) SetStdin(r *os.File) *Cmd {
	c.Cmd.Stdin = r
	return c
}

// SetStdout sets the stdout for the command.
func (c *Cmd) SetStdout(w *os.File) *Cmd {
	c.Cmd.Stdout = w
	return c
}

// SetStderr sets the stderr for the command.
func (c *Cmd) SetStderr(w *os.File) *Cmd {
	c.Cmd.Stderr = w
	return c
}

// WithClientSpan enables creating an OpenTelemetry client span around
// the command execution. The span is started when Start/Run is called
// and ended when the command completes.
func (c *Cmd) WithClientSpan() *Cmd {
	c.clientSpan = true
	return c
}

// finalizeEnv prepares cmd.Env before execution, including trace propagation.
func (c *Cmd) finalizeEnv() {
	// Add TRACEPARENT if context has a valid span
	if envVar := telemetry.TraceparentEnvVar(c.ctx); envVar != "" {
		c.extraEnv = append(c.extraEnv, envVar)
	}

	if len(c.extraEnv) == 0 {
		return
	}

	if c.Cmd.Env == nil {
		c.Cmd.Env = os.Environ()
	}
	c.Cmd.Env = append(c.Cmd.Env, c.extraEnv...)
}

// Start starts the command without waiting for it to complete.
// Note: WithClientSpan() has no effect on Start() since the span cannot be
// ended until Wait() is called. Use Run() for client span support.
func (c *Cmd) Start() error {
	c.finalizeEnv()
	return c.Cmd.Start()
}

// Run starts the command and waits for it to complete.
// If WithClientSpan() was called, an OpenTelemetry span is created around
// the command execution.
func (c *Cmd) Run() error {
	if c.clientSpan {
		_, span := tracer.Start(c.ctx, c.Cmd.Path)
		defer span.End()
	}
	c.finalizeEnv()
	return c.Cmd.Run()
}

// Output runs the command and returns its stdout.
// If WithClientSpan() was called, an OpenTelemetry span is created around
// the command execution.
func (c *Cmd) Output() ([]byte, error) {
	if c.clientSpan {
		_, span := tracer.Start(c.ctx, c.Cmd.Path)
		defer span.End()
	}
	c.finalizeEnv()
	return c.Cmd.Output()
}

// CombinedOutput runs the command and returns its combined stdout and stderr.
// If WithClientSpan() was called, an OpenTelemetry span is created around
// the command execution.
func (c *Cmd) CombinedOutput() ([]byte, error) {
	if c.clientSpan {
		_, span := tracer.Start(c.ctx, c.Cmd.Path)
		defer span.End()
	}
	c.finalizeEnv()
	return c.Cmd.CombinedOutput()
}
