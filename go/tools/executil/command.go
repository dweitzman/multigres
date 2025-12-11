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

// Package executil provides a wrapper around exec.Cmd with:
//   - Graceful termination (SIGTERM → grace period → SIGKILL)
//   - Clean environment variable handling
//   - Automatic OpenTelemetry trace propagation
package executil

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/multigres/multigres/go/tools/executil")

// Cmd wraps command configuration and provides safe execution methods.
// The underlying exec.Cmd is created at execution time, not construction time.
type Cmd struct {
	name string
	args []string
	dir  string

	// Environment handling
	env      []string // explicit base environment (nil = inherit)
	extraEnv []string // accumulated via AddEnv()

	// Stdin/Stdout/Stderr
	Stdin  any // io.Reader or *os.File
	Stdout any // io.Writer or *os.File
	Stderr any // io.Writer or *os.File

	// Runtime state (protected by mu)
	mu       sync.Mutex
	cmd      *exec.Cmd
	span     trace.Span
	startCh  chan struct{} // closed when Start() completes
	waitOnce sync.Once     // ensures Wait() only runs once
	waitErr  error         // result of first Wait()
}

// Command creates a new Cmd. Context is provided at execution time, not here.
func Command(name string, args ...string) *Cmd {
	return &Cmd{
		name: name,
		args: args,
	}
}

// Dir sets the working directory for the command.
func (c *Cmd) Dir(dir string) *Cmd {
	c.dir = dir
	return c
}

// AddEnv adds environment variable(s) to the command.
// These are added on top of inherited environment (or explicit base if SetEnv was called).
// Safe to call multiple times - vars accumulate.
func (c *Cmd) AddEnv(keyvals ...string) *Cmd {
	c.extraEnv = append(c.extraEnv, keyvals...)
	return c
}

// SetEnv replaces the entire environment (no inheritance from parent).
// Call AddEnv after this to add additional vars on top of the explicit base.
func (c *Cmd) SetEnv(env []string) *Cmd {
	c.env = env
	return c
}

// Path returns the command path/name.
func (c *Cmd) Path() string {
	return c.name
}

// Args returns the command arguments (excluding the command name).
func (c *Cmd) Args() []string {
	return c.args
}

// Process returns the underlying os.Process after Start() has been called.
// Returns nil if the command hasn't been started.
func (c *Cmd) Process() *os.Process {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil {
		return nil
	}
	return c.cmd.Process
}

// ProcessState returns the os.ProcessState after Wait() has been called.
// Returns nil if the command hasn't been started or hasn't finished.
func (c *Cmd) ProcessState() *os.ProcessState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil {
		return nil
	}
	return c.cmd.ProcessState
}

// Term sends SIGTERM to the process and waits for it to exit.
// The context deadline/timeout limits how long to wait before sending SIGKILL.
// Safe to call multiple times - subsequent calls return immediately with the first result.
// Safe to call even if process wasn't started - returns nil.
func (c *Cmd) Term(ctx context.Context) error {
	proc := c.Process()
	if proc == nil {
		return nil
	}
	// Send SIGTERM (ignore error - process may have already exited)
	_ = proc.Signal(syscall.SIGTERM)
	// Wait for exit with SIGKILL fallback on ctx timeout
	return c.Wait(ctx)
}

// buildEnv prepares the final environment for the command.
func (c *Cmd) buildEnv() []string {
	if len(c.extraEnv) == 0 && c.env == nil {
		return nil // use default inheritance
	}

	base := c.env
	if base == nil {
		base = os.Environ()
	}
	return append(base, c.extraEnv...)
}

// addTraceparentFromContext extracts trace context and adds TRACEPARENT env var.
func (c *Cmd) addTraceparentFromContext(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return
	}

	// Use the standard W3C trace context propagator
	carrier := propagation.MapCarrier{}
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, carrier)

	if traceparent, ok := carrier["traceparent"]; ok {
		c.AddEnv("TRACEPARENT=" + traceparent)
	}
}

// buildCmd creates the underlying exec.Cmd with proper configuration.
func (c *Cmd) buildCmd(ctx context.Context, grace GraceOption) *exec.Cmd {
	cmd := exec.CommandContext(ctx, c.name, c.args...)
	cmd.Dir = c.dir
	cmd.Env = c.buildEnv()

	// Set up stdin/stdout/stderr
	if c.Stdin != nil {
		switch v := c.Stdin.(type) {
		case *os.File:
			cmd.Stdin = v
		default:
			cmd.Stdin = c.Stdin.(interface{ Read([]byte) (int, error) })
		}
	}
	if c.Stdout != nil {
		switch v := c.Stdout.(type) {
		case *os.File:
			cmd.Stdout = v
		default:
			cmd.Stdout = c.Stdout.(interface{ Write([]byte) (int, error) })
		}
	}
	if c.Stderr != nil {
		switch v := c.Stderr.(type) {
		case *os.File:
			cmd.Stderr = v
		default:
			cmd.Stderr = c.Stderr.(interface{ Write([]byte) (int, error) })
		}
	}

	// Configure graceful termination: SIGTERM first, then SIGKILL after grace period
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = grace.duration()

	return cmd
}

// prepareExecution sets up tracing and environment for command execution.
// Returns the context to use (with span added).
func (c *Cmd) prepareExecution(ctx context.Context) context.Context {
	// Add traceparent to environment first (before potentially modifying ctx)
	c.addTraceparentFromContext(ctx)

	// Create client span for command execution
	var span trace.Span
	ctx, span = tracer.Start(ctx, c.name, trace.WithSpanKind(trace.SpanKindClient))
	c.mu.Lock()
	c.span = span
	c.mu.Unlock()

	return ctx
}

// endSpan ends the span if one was created, recording exit code and error status.
func (c *Cmd) endSpan(err error) {
	c.mu.Lock()
	span := c.span
	c.span = nil
	c.mu.Unlock()

	if span == nil {
		return
	}

	// Record exit code if available
	if c.cmd != nil && c.cmd.ProcessState != nil {
		exitCode := c.cmd.ProcessState.ExitCode()
		span.SetAttributes(attribute.Int("process.exit_code", exitCode))

		// Mark as error if non-zero exit code
		if exitCode != 0 {
			span.SetStatus(codes.Error, "process exited with non-zero code")
		}
	}

	// Record any Go-level error (context canceled, etc.)
	if err != nil {
		span.RecordError(err)
		// Only set error status if not already set by exit code
		if c.cmd == nil || c.cmd.ProcessState == nil || c.cmd.ProcessState.ExitCode() == 0 {
			span.SetStatus(codes.Error, err.Error())
		}
	}

	span.End()
}

// Run runs the command and waits for completion.
// The context controls when to trigger graceful termination (SIGTERM).
// The grace option controls how long to wait before SIGKILL.
func (c *Cmd) Run(ctx context.Context, grace GraceOption) error {
	ctx = c.prepareExecution(ctx)

	c.mu.Lock()
	c.cmd = c.buildCmd(ctx, grace)
	c.mu.Unlock()

	var err error
	if grace.isContextBased() {
		err = c.runWithGraceContext(grace.graceContext())
	} else {
		err = c.cmd.Run()
	}
	c.endSpan(err)
	return err
}

// runWithGraceContext runs the command with context-based grace period handling.
// SIGKILL is sent when the grace context is done (not based on a fixed duration).
func (c *Cmd) runWithGraceContext(graceCtx context.Context) error {
	if err := c.cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- c.cmd.Wait()
	}()

	select {
	case err := <-done:
		return err
	case <-graceCtx.Done():
		// Grace context expired, kill the process
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		return <-done
	}
}

// Output runs the command and returns its stdout.
func (c *Cmd) Output(ctx context.Context, grace GraceOption) ([]byte, error) {
	ctx = c.prepareExecution(ctx)

	c.mu.Lock()
	c.cmd = c.buildCmd(ctx, grace)
	c.mu.Unlock()

	var out []byte
	var err error
	if grace.isContextBased() {
		out, err = c.outputWithGraceContext(grace.graceContext(), false)
	} else {
		out, err = c.cmd.Output()
	}
	c.endSpan(err)
	return out, err
}

// CombinedOutput runs the command and returns its combined stdout and stderr.
func (c *Cmd) CombinedOutput(ctx context.Context, grace GraceOption) ([]byte, error) {
	ctx = c.prepareExecution(ctx)

	c.mu.Lock()
	c.cmd = c.buildCmd(ctx, grace)
	c.mu.Unlock()

	var out []byte
	var err error
	if grace.isContextBased() {
		out, err = c.outputWithGraceContext(grace.graceContext(), true)
	} else {
		out, err = c.cmd.CombinedOutput()
	}
	c.endSpan(err)
	return out, err
}

// outputWithGraceContext runs the command and captures output with context-based grace.
func (c *Cmd) outputWithGraceContext(graceCtx context.Context, combined bool) ([]byte, error) {
	// Set up output capture
	var buf bytes.Buffer
	if combined {
		c.cmd.Stdout = &buf
		c.cmd.Stderr = &buf
	} else {
		c.cmd.Stdout = &buf
	}

	if err := c.cmd.Start(); err != nil {
		return nil, err
	}

	done := make(chan error, 1)
	go func() {
		done <- c.cmd.Wait()
	}()

	select {
	case err := <-done:
		return buf.Bytes(), err
	case <-graceCtx.Done():
		// Grace context expired, kill the process
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		err := <-done
		return buf.Bytes(), err
	}
}

// Start starts the command but does not wait for it to complete.
// The context controls when to trigger graceful termination (SIGTERM).
// Call Wait() to wait for completion and specify the grace period before SIGKILL.
func (c *Cmd) Start(ctx context.Context) error {
	ctx = c.prepareExecution(ctx)

	c.mu.Lock()
	// For Start(), we use a minimal grace period initially - Wait() will handle the real grace
	c.cmd = c.buildCmd(ctx, WithGracePeriod(0))
	c.startCh = make(chan struct{})
	c.mu.Unlock()

	err := c.cmd.Start()

	c.mu.Lock()
	close(c.startCh)
	c.mu.Unlock()

	if err != nil {
		c.endSpan(err)
	}

	return err
}

// StartDaemon starts a long-running daemon process.
// The context is used only for trace propagation (TRACEPARENT) - the daemon's
// lifecycle is NOT tied to context cancellation. No client span is created.
// Use TerminatePID or TerminateProcess for graceful daemon shutdown.
func (c *Cmd) StartDaemon(ctx context.Context) error {
	// Propagate trace context from the passed context
	c.addTraceparentFromContext(ctx)

	c.mu.Lock()
	//nolint:gocritic // Daemon lifecycle must not be tied to context cancellation
	c.cmd = c.buildCmd(context.Background(), WithGracePeriod(0))
	c.startCh = make(chan struct{})
	c.mu.Unlock()

	err := c.cmd.Start()

	c.mu.Lock()
	close(c.startCh)
	c.mu.Unlock()

	return err
}

// Wait waits for the command to exit.
// The context deadline/timeout limits how long to wait before sending SIGKILL.
// If the context has no deadline, waits indefinitely for the process to exit.
// Safe to call multiple times - subsequent calls return immediately with the first result.
func (c *Cmd) Wait(ctx context.Context) error {
	c.mu.Lock()
	cmd := c.cmd
	startCh := c.startCh
	c.mu.Unlock()

	if cmd == nil {
		return nil
	}

	// Wait for Start() to complete
	if startCh != nil {
		<-startCh
	}

	// Use sync.Once to ensure we only wait once
	c.waitOnce.Do(func() {
		// Wait for process with context-based timeout for SIGKILL
		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		select {
		case c.waitErr = <-done:
			// Process exited
		case <-ctx.Done():
			// Context expired, kill the process
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			// Still wait for the process to actually exit
			c.waitErr = <-done
		}

		c.endSpan(c.waitErr)
	})

	return c.waitErr
}
