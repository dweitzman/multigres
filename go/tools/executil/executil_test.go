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
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCommand_InheritsEnvironment(t *testing.T) {
	ctx := context.Background()
	cmd := Command(ctx, "env")

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("cmd.Output() failed: %v", err)
	}

	// Should contain PATH from parent environment
	if !strings.Contains(string(output), "PATH=") {
		t.Error("expected inherited PATH environment variable")
	}
}

func TestCommand_AddEnv(t *testing.T) {
	ctx := context.Background()
	cmd := Command(ctx, "env").
		AddEnv("TEST_VAR_1=value1").
		AddEnv("TEST_VAR_2=value2")

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("cmd.Output() failed: %v", err)
	}

	out := string(output)
	if !strings.Contains(out, "TEST_VAR_1=value1") {
		t.Error("expected TEST_VAR_1 in output")
	}
	if !strings.Contains(out, "TEST_VAR_2=value2") {
		t.Error("expected TEST_VAR_2 in output")
	}
	// Should still inherit parent environment
	if !strings.Contains(out, "PATH=") {
		t.Error("expected inherited PATH environment variable")
	}
}

func TestCommand_SetEnv_ReplacesEnvironment(t *testing.T) {
	ctx := context.Background()
	cmd := Command(ctx, "env").
		SetEnv([]string{"ONLY_THIS=exists"})

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("cmd.Output() failed: %v", err)
	}

	out := string(output)
	if !strings.Contains(out, "ONLY_THIS=exists") {
		t.Error("expected ONLY_THIS in output")
	}
	// Should NOT inherit parent environment after SetEnv
	if strings.Contains(out, "PATH=") {
		t.Error("expected PATH to NOT be inherited after SetEnv")
	}
}

func TestCommand_SetEnvThenAddEnv(t *testing.T) {
	ctx := context.Background()
	cmd := Command(ctx, "env").
		SetEnv([]string{"BASE=value"}).
		AddEnv("EXTRA=added")

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("cmd.Output() failed: %v", err)
	}

	out := string(output)
	if !strings.Contains(out, "BASE=value") {
		t.Error("expected BASE in output")
	}
	if !strings.Contains(out, "EXTRA=added") {
		t.Error("expected EXTRA in output")
	}
}

func TestCommand_GracefulTermination(t *testing.T) {
	// Create a context we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Use a short grace period for the test
	cmd := CommandWithGracePeriod(ctx, 100*time.Millisecond, "sleep", "10")

	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start() failed: %v", err)
	}

	// Give the process time to start
	time.Sleep(50 * time.Millisecond)

	// Cancel the context - this should send SIGTERM first
	cancel()

	// Wait for the process to exit
	err := cmd.Wait()

	// The process should have been terminated
	if err == nil {
		t.Error("expected error from terminated process")
	}
}

func TestCommand_SetDir(t *testing.T) {
	ctx := context.Background()
	cmd := Command(ctx, "pwd").SetDir("/tmp")

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("cmd.Output() failed: %v", err)
	}

	// On macOS /tmp is a symlink to /private/tmp
	out := strings.TrimSpace(string(output))
	if out != "/tmp" && out != "/private/tmp" {
		t.Errorf("expected /tmp or /private/tmp, got %s", out)
	}
}

func TestTerminatePID_AlreadyDead(t *testing.T) {
	// Use a PID that doesn't exist
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := TerminatePID(ctx, 999999999)
	if err != nil {
		t.Errorf("expected nil error for non-existent process, got: %v", err)
	}
}

func TestTerminatePID_GracefulExit(t *testing.T) {
	// Start a process that exits on SIGTERM
	ctx := context.Background()
	cmd := Command(ctx, "sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start() failed: %v", err)
	}

	pid := cmd.Process.Pid

	// Reap the child process in a goroutine
	go func() { _ = cmd.Wait() }()

	// Terminate with 2s grace period (context timeout)
	termCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := TerminatePID(termCtx, pid); err != nil {
		t.Errorf("TerminatePID failed: %v", err)
	}
}

func TestTerminatePID_ContextExpires_SendsKill(t *testing.T) {
	// Start a process that ignores SIGTERM (trap it)
	ctx := context.Background()
	cmd := Command(ctx, "sh", "-c", "trap '' TERM; sleep 10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start() failed: %v", err)
	}

	pid := cmd.Process.Pid

	// Reap the child process in a goroutine
	go func() { _ = cmd.Wait() }()

	// Use a short context - should escalate to SIGKILL when context expires
	termCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err := TerminatePID(termCtx, pid)
	if err != nil {
		t.Errorf("TerminatePID failed: %v", err)
	}

	// Process should be gone (killed by SIGKILL after context expired)
	time.Sleep(100 * time.Millisecond) // Give time for process to be reaped
}

func TestTerminateProcess_Nil(t *testing.T) {
	ctx := context.Background()
	err := TerminateProcess(ctx, nil)
	if err != nil {
		t.Errorf("expected nil error for nil process, got: %v", err)
	}
}

func TestIsProcessGone(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"process done", os.ErrProcessDone, true},
		{"no such process string", &testError{"no such process"}, true},
		{"process already finished string", &testError{"process already finished"}, true},
		{"other error", &testError{"something else"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isProcessGone(tt.err)
			if got != tt.expected {
				t.Errorf("isProcessGone(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
