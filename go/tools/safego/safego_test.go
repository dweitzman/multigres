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

package safego

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGoContinueOnPanic_RunsFn(t *testing.T) {
	done := make(chan int, 1)
	GoContinueOnPanic(t.Context(), "test", func() {
		done <- 42
	})
	select {
	case got := <-done:
		assert.Equal(t, 42, got)
	case <-time.After(5 * time.Second):
		t.Fatal("fn did not run")
	}
}

func TestGoContinueOnPanic_RecoversAndProcessSurvives(t *testing.T) {
	ran := make(chan struct{})
	GoContinueOnPanic(t.Context(), "test", func() {
		// Signal during unwinding so we know fn both ran and panicked.
		defer close(ran)
		panic("boom")
	})

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("fn did not run")
	}

	// If the panic were not recovered, the test binary would have crashed
	// before reaching here. A second goroutine confirms the process is healthy.
	pong := make(chan struct{})
	GoContinueOnPanic(t.Context(), "test", func() { close(pong) })
	select {
	case <-pong:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not survive the recovered panic")
	}
}

func TestRecovered_RedactsPanicDetail(t *testing.T) {
	err := Recovered(t.Context(), "my-source", "secret-panic-value")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "my-source")
	// The returned error must be safe to surface across a trust boundary: it
	// must not leak the panic value (the detail lives in the logs only).
	assert.NotContains(t, err.Error(), "secret-panic-value")
}

// TestGoCrashOnPanic_Crashes re-executes this test binary in a subprocess so the
// deliberate re-panic crashes that child, not the parent test run. This is the
// standard Go pattern for exercising process-terminating behavior.
func TestGoCrashOnPanic_Crashes(t *testing.T) {
	if os.Getenv("SAFEGO_CRASH_CHILD") == "1" {
		GoCrashOnPanic(context.Background(), "crash-source", func() {
			panic("fatal-boom")
		})
		// Give the goroutine time to run and crash the process.
		time.Sleep(10 * time.Second)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestGoCrashOnPanic_Crashes$", "-test.v")
	cmd.Env = append(os.Environ(), "SAFEGO_CRASH_CHILD=1")
	out, err := cmd.CombinedOutput()

	require.Error(t, err, "child process should have crashed with a non-zero exit")
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.False(t, exitErr.Success())

	output := string(out)
	assert.Contains(t, output, "recovered from panic in goroutine", "should log before re-panicking")
	assert.Contains(t, output, "fatal-boom", "the original panic value should reach the crash output")
}

func TestPanicCounter(t *testing.T) {
	// Install an in-memory meter provider and reset the lazy init so this test's
	// reader observes the counter.
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	panicCounterOnce = sync.Once{}
	panicCounter = noop.Int64Counter{}
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		panicCounterOnce = sync.Once{}
		panicCounter = noop.Int64Counter{}
	})

	GoContinueOnPanic(t.Context(), "counter-test", func() { panic("x") })

	require.Eventually(t, func() bool {
		return panicCount(t, reader, "counter-test", "continue") == 1
	}, 5*time.Second, 20*time.Millisecond, "continue panic should be counted once")
}

// panicCount collects the recovered-panic counter value for a (source, mode) pair.
func panicCount(t *testing.T, reader sdkmetric.Reader, source, mode string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "goroutine.panic.recovered" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				s, _ := dp.Attributes.Value("source")
				md, _ := dp.Attributes.Value("mode")
				if s.AsString() == source && md.AsString() == mode {
					return dp.Value
				}
			}
		}
	}
	return 0
}
