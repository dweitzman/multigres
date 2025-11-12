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

package workflow

import (
	"context"
	"errors"
	"testing"
)

// testPhase is a simple phase enum for testing
type testPhase int

const (
	testPhaseA testPhase = iota
	testPhaseB
	testPhaseC
)

func (p testPhase) String() string {
	switch p {
	case testPhaseA:
		return "PhaseA"
	case testPhaseB:
		return "PhaseB"
	case testPhaseC:
		return "PhaseC"
	default:
		return "Unknown"
	}
}

// testInput is a simple input type for testing
type testInput struct {
	value int
}

// testState accumulates state through the workflow
type testState struct {
	executedPhases []testPhase
	sum            int
}

// testResult is returned on successful completion
type testResult struct {
	totalPhases int
	finalSum    int
}

// mockLogger for testing
type mockLogger struct {
	logs []string
}

func (ml *mockLogger) InfoContext(ctx context.Context, msg string, keysAndValues ...any) {
	ml.logs = append(ml.logs, msg)
}

func (ml *mockLogger) WarnContext(ctx context.Context, msg string, keysAndValues ...any) {
	ml.logs = append(ml.logs, "WARN: "+msg)
}

func (ml *mockLogger) ErrorContext(ctx context.Context, msg string, keysAndValues ...any) {
	ml.logs = append(ml.logs, "ERROR: "+msg)
}

// testExecutor increments the sum by a fixed value
type testExecutor struct {
	name       string
	phases     []testPhase
	increment  int
	shouldFail bool
	canFail    bool
}

func (e *testExecutor) Name() string {
	return e.name
}

func (e *testExecutor) Phases() []testPhase {
	return e.phases
}

func (e *testExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[testPhase, testInput, testState]) error {
	phaseCtx.State.executedPhases = append(phaseCtx.State.executedPhases, phaseCtx.Phase)
	phaseCtx.State.sum += e.increment

	if e.shouldFail {
		return errors.New("executor failed")
	}

	return nil
}

func (e *testExecutor) CanFail(phase testPhase) bool {
	return e.canFail
}

func TestWorkflow_BasicExecution(t *testing.T) {
	logger := &mockLogger{}
	phases := []testPhase{testPhaseA, testPhaseB, testPhaseC}

	wf := NewWorkflow[testPhase, testInput, testState, testResult](phases, logger)

	// Register executors
	wf.Register(&testExecutor{
		name:      "ExecutorA",
		phases:    []testPhase{testPhaseA},
		increment: 10,
	})
	wf.Register(&testExecutor{
		name:      "ExecutorB",
		phases:    []testPhase{testPhaseB},
		increment: 20,
	})
	wf.Register(&testExecutor{
		name:      "ExecutorC",
		phases:    []testPhase{testPhaseC},
		increment: 30,
	})

	// Build result function
	buildResult := func(input *testInput, state *testState, errors []error) (*testResult, error) {
		return &testResult{
			totalPhases: len(state.executedPhases),
			finalSum:    state.sum,
		}, nil
	}

	// Execute workflow
	input := &testInput{value: 100}
	initialState := &testState{sum: 0}

	result, err := wf.Execute(context.Background(), input, initialState, buildResult, nil)
	if err != nil {
		t.Fatalf("Workflow execution failed: %v", err)
	}

	// Verify result
	if result.totalPhases != 3 {
		t.Errorf("Expected 3 phases, got %d", result.totalPhases)
	}

	if result.finalSum != 60 {
		t.Errorf("Expected final sum of 60, got %d", result.finalSum)
	}

	// Verify phases executed in order
	expectedOrder := []testPhase{testPhaseA, testPhaseB, testPhaseC}
	for i, phase := range initialState.executedPhases {
		if phase != expectedOrder[i] {
			t.Errorf("Phase %d: expected %s, got %s", i, expectedOrder[i], phase)
		}
	}
}

func TestWorkflow_NonFatalError(t *testing.T) {
	logger := &mockLogger{}
	phases := []testPhase{testPhaseA, testPhaseB, testPhaseC}

	wf := NewWorkflow[testPhase, testInput, testState, testResult](phases, logger)

	// Register executors, one will fail but is non-fatal
	wf.Register(&testExecutor{
		name:       "ExecutorA",
		phases:     []testPhase{testPhaseA},
		increment:  10,
		shouldFail: true,
		canFail:    true, // Non-fatal
	})
	wf.Register(&testExecutor{
		name:      "ExecutorB",
		phases:    []testPhase{testPhaseB},
		increment: 20,
	})

	buildResult := func(input *testInput, state *testState, errors []error) (*testResult, error) {
		return &testResult{
			totalPhases: len(state.executedPhases),
			finalSum:    state.sum,
		}, nil
	}

	input := &testInput{value: 100}
	initialState := &testState{sum: 0}

	result, err := wf.Execute(context.Background(), input, initialState, buildResult, nil)
	if err != nil {
		t.Fatalf("Workflow should not fail for non-fatal error: %v", err)
	}

	// Verify workflow continued despite error
	if result.totalPhases != 2 {
		t.Errorf("Expected 2 phases executed, got %d", result.totalPhases)
	}

	// ExecutorA failed but still incremented (before failure)
	// ExecutorB succeeded
	if result.finalSum != 30 {
		t.Errorf("Expected final sum of 30, got %d", result.finalSum)
	}

	// Check that warning was logged
	foundWarning := false
	for _, log := range logger.logs {
		if log == "WARN: Executor failed (non-fatal)" {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("Expected warning log for non-fatal failure")
	}
}

func TestWorkflow_FatalError(t *testing.T) {
	logger := &mockLogger{}
	phases := []testPhase{testPhaseA, testPhaseB, testPhaseC}

	wf := NewWorkflow[testPhase, testInput, testState, testResult](phases, logger)

	// Register executors, one will fail and is fatal
	wf.Register(&testExecutor{
		name:      "ExecutorA",
		phases:    []testPhase{testPhaseA},
		increment: 10,
	})
	wf.Register(&testExecutor{
		name:       "ExecutorB",
		phases:     []testPhase{testPhaseB},
		increment:  20,
		shouldFail: true,
		canFail:    false, // Fatal
	})
	wf.Register(&testExecutor{
		name:      "ExecutorC",
		phases:    []testPhase{testPhaseC},
		increment: 30,
	})

	buildResult := func(input *testInput, state *testState, errors []error) (*testResult, error) {
		return &testResult{
			totalPhases: len(state.executedPhases),
			finalSum:    state.sum,
		}, nil
	}

	input := &testInput{value: 100}
	initialState := &testState{sum: 0}

	result, err := wf.Execute(context.Background(), input, initialState, buildResult, nil)
	if err == nil {
		t.Fatal("Expected workflow to fail for fatal error")
	}

	if result != nil {
		t.Error("Expected nil result on fatal error")
	}

	// Verify workflow stopped at PhaseB
	if len(initialState.executedPhases) != 2 {
		t.Errorf("Expected 2 phases executed before failure, got %d", len(initialState.executedPhases))
	}

	// Only ExecutorA should have incremented
	if initialState.sum != 30 { // 10 from A, 20 from B (before it failed)
		t.Errorf("Expected sum of 30, got %d", initialState.sum)
	}
}

func TestWorkflow_MultipleExecutorsPerPhase(t *testing.T) {
	logger := &mockLogger{}
	phases := []testPhase{testPhaseA, testPhaseB}

	wf := NewWorkflow[testPhase, testInput, testState, testResult](phases, logger)

	// Register multiple executors for PhaseA
	wf.Register(&testExecutor{
		name:      "Executor1",
		phases:    []testPhase{testPhaseA},
		increment: 1,
	})
	wf.Register(&testExecutor{
		name:      "Executor2",
		phases:    []testPhase{testPhaseA},
		increment: 2,
	})
	wf.Register(&testExecutor{
		name:      "Executor3",
		phases:    []testPhase{testPhaseA},
		increment: 3,
	})
	wf.Register(&testExecutor{
		name:      "ExecutorB",
		phases:    []testPhase{testPhaseB},
		increment: 10,
	})

	buildResult := func(input *testInput, state *testState, errors []error) (*testResult, error) {
		return &testResult{
			totalPhases: len(state.executedPhases),
			finalSum:    state.sum,
		}, nil
	}

	input := &testInput{value: 100}
	initialState := &testState{sum: 0}

	result, err := wf.Execute(context.Background(), input, initialState, buildResult, nil)
	if err != nil {
		t.Fatalf("Workflow execution failed: %v", err)
	}

	// All three executors in PhaseA should have run, plus ExecutorB
	// Total: 1 + 2 + 3 + 10 = 16
	if result.finalSum != 16 {
		t.Errorf("Expected final sum of 16, got %d", result.finalSum)
	}
}

func TestWorkflow_EarlyExit(t *testing.T) {
	logger := &mockLogger{}
	phases := []testPhase{testPhaseA, testPhaseB, testPhaseC}

	wf := NewWorkflow[testPhase, testInput, testState, testResult](phases, logger)

	wf.Register(&testExecutor{
		name:      "ExecutorA",
		phases:    []testPhase{testPhaseA},
		increment: 10,
	})
	wf.Register(&testExecutor{
		name:      "ExecutorB",
		phases:    []testPhase{testPhaseB},
		increment: 20,
	})
	wf.Register(&testExecutor{
		name:      "ExecutorC",
		phases:    []testPhase{testPhaseC},
		increment: 30,
	})

	// Early exit after PhaseA
	earlyExitChecker := func(
		phase testPhase,
		phaseCtx *PhaseContext[testPhase, testInput, testState],
	) (bool, *testResult, error) {
		if phase == testPhaseA && phaseCtx.State.sum == 10 {
			return true, &testResult{
				totalPhases: 1,
				finalSum:    phaseCtx.State.sum,
			}, nil
		}
		return false, nil, nil
	}

	buildResult := func(input *testInput, state *testState, errors []error) (*testResult, error) {
		return &testResult{
			totalPhases: len(state.executedPhases),
			finalSum:    state.sum,
		}, nil
	}

	input := &testInput{value: 100}
	initialState := &testState{sum: 0}

	result, err := wf.Execute(context.Background(), input, initialState, buildResult, &ExecuteOptions[testPhase, testInput, testState, testResult]{
		EarlyExitChecker: earlyExitChecker,
	})
	if err != nil {
		t.Fatalf("Workflow execution failed: %v", err)
	}

	// Should have exited after PhaseA
	if result.totalPhases != 1 {
		t.Errorf("Expected early exit after 1 phase, got %d", result.totalPhases)
	}

	if result.finalSum != 10 {
		t.Errorf("Expected final sum of 10, got %d", result.finalSum)
	}

	// Only PhaseA should have executed
	if len(initialState.executedPhases) != 1 {
		t.Errorf("Expected 1 phase executed, got %d", len(initialState.executedPhases))
	}
}
