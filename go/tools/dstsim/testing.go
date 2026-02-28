// Copyright 2026 Supabase, Inc.
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

package dstsim

import (
	"bytes"
	"os"
	"strconv"
	"testing"
)

// SimulationTestHelper provides test assertion helpers for simulations
// It wraps a simulator and testing.T to provide convenient test assertions
// that automatically dump recent trace on failure
type SimulationTestHelper[I any, R any, ID comparable] struct {
	t   *testing.T
	sim *Simulator[I, R, ID]
}

// NewSimulationTestHelper creates a test helper for the given simulator
func NewSimulationTestHelper[I any, R any, ID comparable](t *testing.T, sim *Simulator[I, R, ID]) *SimulationTestHelper[I, R, ID] {
	return &SimulationTestHelper[I, R, ID]{t: t, sim: sim}
}

// RequireRunUntil runs the simulation until the condition is met or maxTicks is reached
// If the condition is not met within maxTicks, dumps the last 20 ticks of trace and fails the test
func (h *SimulationTestHelper[I, R, ID]) RequireRunUntil(condition Condition[I, R, ID], maxTicks int64) {
	h.RequireRunUntilWithTrace(condition, maxTicks, 20)
}

// RequireRunUntilWithTrace runs the simulation until the condition is met or maxTicks is reached
// If the condition is not met within maxTicks, dumps the last dumpTicks ticks of trace and fails the test
func (h *SimulationTestHelper[I, R, ID]) RequireRunUntilWithTrace(condition Condition[I, R, ID], maxTicks int64, dumpTicks int) {
	h.t.Helper()
	err := h.sim.RunUntil(condition, maxTicks)
	if err != nil {
		h.t.Logf("Simulation failed: %v", err)
		h.dumpTrace(dumpTicks)
		h.t.FailNow()
		return
	}
	if n := traceEnvDumpCount(); n > 0 {
		h.dumpTrace(n)
	}
}

// RequireRunFor runs the simulation for the specified number of ticks
// If any assertion violations occur, dumps the last 20 ticks of trace and fails the test
func (h *SimulationTestHelper[I, R, ID]) RequireRunFor(ticks int64) {
	h.RequireRunForWithTrace(ticks, 20)
}

// RequireRunForWithTrace runs the simulation for the specified number of ticks
// If any assertion violations occur, dumps the last dumpTicks ticks of trace and fails the test
func (h *SimulationTestHelper[I, R, ID]) RequireRunForWithTrace(ticks int64, dumpTicks int) {
	h.t.Helper()
	err := h.sim.RunFor(ticks)
	if err != nil {
		h.t.Logf("Simulation failed: %v", err)
		h.dumpTrace(dumpTicks)
		h.t.FailNow()
		return
	}
	if n := traceEnvDumpCount(); n > 0 {
		h.dumpTrace(n)
	}
}

// AssertRunUntil runs the simulation until the condition is met or maxTicks is reached
// If the condition is not met within maxTicks, dumps the last 20 ticks of trace and marks the test as failed
// Unlike RequireRunUntil, this does not stop test execution
// Returns true if the condition was met, false otherwise
func (h *SimulationTestHelper[I, R, ID]) AssertRunUntil(condition Condition[I, R, ID], maxTicks int64) bool {
	return h.AssertRunUntilWithTrace(condition, maxTicks, 20)
}

// AssertRunUntilWithTrace runs the simulation until the condition is met or maxTicks is reached
// If the condition is not met within maxTicks, dumps the last dumpTicks ticks of trace and marks the test as failed
// Unlike RequireRunUntilWithTrace, this does not stop test execution
// Returns true if the condition was met, false otherwise
func (h *SimulationTestHelper[I, R, ID]) AssertRunUntilWithTrace(condition Condition[I, R, ID], maxTicks int64, dumpTicks int) bool {
	h.t.Helper()
	err := h.sim.RunUntil(condition, maxTicks)
	if err != nil {
		h.t.Logf("Simulation failed: %v", err)
		h.dumpTrace(dumpTicks)
		h.t.Fail()
		return false
	}
	if n := traceEnvDumpCount(); n > 0 {
		h.dumpTrace(n)
	}
	return true
}

// AssertRunFor runs the simulation for the specified number of ticks
// If any assertion violations occur, dumps the last 20 ticks of trace and marks the test as failed
// Unlike RequireRunFor, this does not stop test execution
// Returns true if the simulation completed without violations, false otherwise
func (h *SimulationTestHelper[I, R, ID]) AssertRunFor(ticks int64) bool {
	return h.AssertRunForWithTrace(ticks, 20)
}

// AssertRunForWithTrace runs the simulation for the specified number of ticks
// If any assertion violations occur, dumps the last dumpTicks ticks of trace and marks the test as failed
// Unlike RequireRunForWithTrace, this does not stop test execution
// Returns true if the simulation completed without violations, false otherwise
func (h *SimulationTestHelper[I, R, ID]) AssertRunForWithTrace(ticks int64, dumpTicks int) bool {
	h.t.Helper()
	err := h.sim.RunFor(ticks)
	if err != nil {
		h.t.Logf("Simulation failed: %v", err)
		h.dumpTrace(dumpTicks)
		h.t.Fail()
		return false
	}
	if n := traceEnvDumpCount(); n > 0 {
		h.dumpTrace(n)
	}
	return true
}

// dumpTrace dumps recent trace to the test log
func (h *SimulationTestHelper[I, R, ID]) dumpTrace(numTicks int) {
	h.t.Helper()
	var buf bytes.Buffer
	h.sim.DumpRecentTrace(&buf, numTicks)
	h.t.Log(buf.String())
}

// traceEnvDumpCount returns the number of trace ticks to dump on success,
// as specified by the DSTSIM_TRACE environment variable.
// Set DSTSIM_TRACE=500 to print the last 500 ticks of trace even on success.
// Returns 0 if the env var is not set or not a positive integer.
func traceEnvDumpCount() int {
	v := os.Getenv("DSTSIM_TRACE")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
