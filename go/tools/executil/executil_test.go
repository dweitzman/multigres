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

package executil

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestCommand_Run(t *testing.T) {
	ctx := context.Background()
	cmd := Command("echo", "hello")
	err := cmd.Run(ctx, DefaultGracePeriod)
	require.NoError(t, err)
}

func TestCommand_Output(t *testing.T) {
	ctx := context.Background()
	cmd := Command("echo", "hello")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", string(out))
}

func TestCommand_CombinedOutput(t *testing.T) {
	ctx := context.Background()
	// Use sh to write to both stdout and stderr
	cmd := Command("sh", "-c", "echo stdout; echo stderr >&2")
	out, err := cmd.CombinedOutput(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	assert.Contains(t, string(out), "stdout")
	assert.Contains(t, string(out), "stderr")
}

func TestCommand_StartWait(t *testing.T) {
	ctx := context.Background()
	cmd := Command("echo", "hello")
	err := cmd.Start(ctx)
	require.NoError(t, err)

	err = cmd.Wait(context.Background())
	require.NoError(t, err)
}

func TestCommand_AddEnv(t *testing.T) {
	ctx := context.Background()
	cmd := Command("sh", "-c", "echo $TEST_VAR").
		AddEnv("TEST_VAR=hello_from_env")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	assert.Equal(t, "hello_from_env\n", string(out))
}

func TestCommand_AddEnv_Multiple(t *testing.T) {
	ctx := context.Background()
	cmd := Command("sh", "-c", "echo $VAR1 $VAR2").
		AddEnv("VAR1=first").
		AddEnv("VAR2=second")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	assert.Equal(t, "first second\n", string(out))
}

func TestCommand_AddEnv_InheritsParent(t *testing.T) {
	// Set a var in parent environment
	t.Setenv("PARENT_VAR", "from_parent")

	ctx := context.Background()
	cmd := Command("sh", "-c", "echo $PARENT_VAR $CHILD_VAR").
		AddEnv("CHILD_VAR=from_child")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	assert.Equal(t, "from_parent from_child\n", string(out))
}

func TestCommand_SetEnv_NoInheritance(t *testing.T) {
	// Set a var in parent environment
	t.Setenv("PARENT_VAR", "from_parent")

	ctx := context.Background()
	// SetEnv replaces environment, so PARENT_VAR won't be available
	cmd := Command("sh", "-c", "echo \"PARENT=$PARENT_VAR\" \"CHILD=$CHILD_VAR\"").
		SetEnv([]string{"PATH=" + os.Getenv("PATH")}).
		AddEnv("CHILD_VAR=from_child")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	// PARENT_VAR should be empty since we didn't inherit
	assert.Equal(t, "PARENT= CHILD=from_child\n", string(out))
}

func TestCommand_Dir(t *testing.T) {
	ctx := context.Background()
	cmd := Command("pwd").Dir("/tmp")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	// /tmp might be a symlink on macOS to /private/tmp
	assert.True(t, strings.HasSuffix(strings.TrimSpace(string(out)), "tmp"))
}

func TestCommand_Stdout(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	cmd := Command("echo", "hello")
	cmd.Stdout = &buf
	err := cmd.Run(ctx, DefaultGracePeriod)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", buf.String())
}

func TestCommand_ExitCode(t *testing.T) {
	ctx := context.Background()
	cmd := Command("sh", "-c", "exit 42")
	err := cmd.Run(ctx, DefaultGracePeriod)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exit status 42")
}

func TestCommand_GracefulTermination(t *testing.T) {
	// Start a process that ignores SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	cmd := Command("sh", "-c", "trap '' TERM; sleep 10")
	err := cmd.Start(ctx)
	require.NoError(t, err)

	// Cancel context to send SIGTERM
	cancel()

	// Wait with a short grace period - should SIGKILL after grace expires
	graceCtx, graceCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer graceCancel()

	start := time.Now()
	err = cmd.Wait(graceCtx)
	elapsed := time.Since(start)

	// Should complete quickly (grace period + some overhead)
	assert.Less(t, elapsed, 500*time.Millisecond)
	// Process was killed
	require.Error(t, err)
}

func TestCommand_WithGracePeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := Command("sh", "-c", "trap '' TERM; sleep 10")

	// Use Run with a short grace period
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := cmd.Run(ctx, WithGracePeriod(100*time.Millisecond))
	elapsed := time.Since(start)

	// Should complete: 50ms (cancel) + 100ms (grace) + overhead
	assert.Less(t, elapsed, 500*time.Millisecond)
	require.Error(t, err)
}

func TestCommand_WithGraceContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	graceCtx, graceCancel := context.WithCancel(context.Background())

	cmd := Command("sh", "-c", "trap '' TERM; sleep 10")

	// Cancel main context to trigger SIGTERM
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Cancel grace context to trigger SIGKILL
	go func() {
		time.Sleep(150 * time.Millisecond)
		graceCancel()
	}()

	start := time.Now()
	err := cmd.Run(ctx, WithGraceContext(graceCtx))
	elapsed := time.Since(start)

	// Should complete around 150ms
	assert.Less(t, elapsed, 500*time.Millisecond)
	require.Error(t, err)
}

// setupTestTracer creates a test tracer provider and returns it along with a span recorder.
func setupTestTracer(t *testing.T) (*tracetest.SpanRecorder, func()) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	originalTP := otel.GetTracerProvider()
	originalProp := otel.GetTextMapPropagator()

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Reset the package-level tracer to use the new provider
	tracer = tp.Tracer("github.com/multigres/multigres/go/tools/executil")

	return sr, func() {
		otel.SetTracerProvider(originalTP)
		otel.SetTextMapPropagator(originalProp)
		tracer = otel.Tracer("github.com/multigres/multigres/go/tools/executil")
	}
}

func TestCommand_CreatesSpan(t *testing.T) {
	sr, cleanup := setupTestTracer(t)
	defer cleanup()

	// Use t.Context() (not context.Background()/DaemonContext) to get span creation.
	// In production, this would be a request context or operation context.
	cmd := Command("echo", "hello")
	err := cmd.Run(t.Context(), DefaultGracePeriod)
	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "echo", spans[0].Name())
	assert.Equal(t, trace.SpanKindClient, spans[0].SpanKind())
}

func TestCommand_SpanRecordsExitCode(t *testing.T) {
	sr, cleanup := setupTestTracer(t)
	defer cleanup()

	cmd := Command("sh", "-c", "exit 42")
	err := cmd.Run(t.Context(), DefaultGracePeriod)
	require.Error(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	// Check exit code attribute
	var exitCode int64 = -1
	for _, attr := range spans[0].Attributes() {
		if attr.Key == attribute.Key("process.exit_code") {
			exitCode = attr.Value.AsInt64()
			break
		}
	}
	assert.Equal(t, int64(42), exitCode)

	// Check error status
	assert.Equal(t, codes.Error, spans[0].Status().Code)
}

func TestCommand_SpanRecordsZeroExitCode(t *testing.T) {
	sr, cleanup := setupTestTracer(t)
	defer cleanup()

	cmd := Command("true")
	err := cmd.Run(t.Context(), DefaultGracePeriod)
	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	// Check exit code attribute
	var exitCode int64 = -1
	for _, attr := range spans[0].Attributes() {
		if attr.Key == attribute.Key("process.exit_code") {
			exitCode = attr.Value.AsInt64()
			break
		}
	}
	assert.Equal(t, int64(0), exitCode)

	// Status should not be error
	assert.NotEqual(t, codes.Error, spans[0].Status().Code)
}

func TestCommand_DaemonContext_NoSpan(t *testing.T) {
	sr, cleanup := setupTestTracer(t)
	defer cleanup()

	cmd := Command("echo", "hello")
	err := cmd.Start(DaemonContext)
	require.NoError(t, err)
	err = cmd.Wait(context.Background())
	require.NoError(t, err)

	// No span should be created for daemon context
	spans := sr.Ended()
	assert.Empty(t, spans)
}

func TestCommand_TraceparentPropagation(t *testing.T) {
	sr, cleanup := setupTestTracer(t)
	defer cleanup()

	// Create a parent span
	ctx, parentSpan := otel.Tracer("test").Start(context.Background(), "parent")
	defer parentSpan.End()

	// Run a command that prints TRACEPARENT
	cmd := Command("sh", "-c", "echo $TRACEPARENT")
	out, err := cmd.Output(ctx, DefaultGracePeriod)
	require.NoError(t, err)

	// TRACEPARENT should be set and contain the trace ID
	traceparent := strings.TrimSpace(string(out))
	assert.NotEmpty(t, traceparent)
	assert.True(t, strings.HasPrefix(traceparent, "00-"), "traceparent should start with version 00")

	// The trace ID in TRACEPARENT should match the parent span
	traceID := parentSpan.SpanContext().TraceID().String()
	assert.Contains(t, traceparent, traceID)

	spans := sr.Ended()
	// Should have the command span (parent span not ended yet)
	require.Len(t, spans, 1)
}

func TestCommand_Path(t *testing.T) {
	cmd := Command("echo", "hello", "world")
	assert.Equal(t, "echo", cmd.Path())
}

func TestCommand_Args(t *testing.T) {
	cmd := Command("echo", "hello", "world")
	assert.Equal(t, []string{"hello", "world"}, cmd.Args())
}

func TestCommand_Process(t *testing.T) {
	cmd := Command("sleep", "1")

	// Before start, process should be nil
	assert.Nil(t, cmd.Process())

	ctx := context.Background()
	err := cmd.Start(ctx)
	require.NoError(t, err)

	// After start, process should be set
	proc := cmd.Process()
	require.NotNil(t, proc)
	assert.Greater(t, proc.Pid, 0)

	// Clean up
	graceCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = cmd.Wait(graceCtx)
}
