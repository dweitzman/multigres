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
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// testMetricReader collects the panic counter for TestPanicCounter. The meter
// provider is installed once in TestMain (before any goroutine runs), so the
// package's lazy counter binds to it on first use and no test mutates the
// package globals — a reassignment would race with goroutines calling incPanic.
var testMetricReader sdkmetric.Reader

func TestMain(m *testing.M) {
	testMetricReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))
	os.Exit(m.Run()) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

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
		RegisterCrashFlusher(func(context.Context) {
			fmt.Fprintln(os.Stderr, "CRASH_FLUSH_RAN")
		})
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
	assert.Contains(t, output, "CRASH_FLUSH_RAN", "the registered crash flusher should run before the crash")
	assert.Contains(t, output, "fatal-boom", "the original panic value should reach the crash output")
}

func TestFlushBeforeCrash_RunsFlusher(t *testing.T) {
	ran := make(chan struct{})
	RegisterCrashFlusher(func(context.Context) { close(ran) })
	t.Cleanup(func() { RegisterCrashFlusher(nil) })

	flushBeforeCrash(t.Context())
	select {
	case <-ran:
	default:
		t.Fatal("registered flusher was not invoked")
	}
}

func TestFlushBeforeCrash_NoFlusherIsNoop(t *testing.T) {
	RegisterCrashFlusher(nil)
	// Must not block or panic when nothing is registered.
	flushBeforeCrash(t.Context())
}

func TestFlushBeforeCrash_BoundsAHangingFlusher(t *testing.T) {
	prev := crashFlushTimeout
	crashFlushTimeout = 50 * time.Millisecond
	t.Cleanup(func() { crashFlushTimeout = prev })

	block := make(chan struct{})
	t.Cleanup(func() { close(block) }) // release the leaked flusher goroutine
	RegisterCrashFlusher(func(context.Context) { <-block })
	t.Cleanup(func() { RegisterCrashFlusher(nil) })

	start := time.Now()
	flushBeforeCrash(t.Context())
	assert.Less(t, time.Since(start), time.Second,
		"a flusher that ignores the deadline must not stall the crash past the timeout")
}

func TestFlushBeforeCrash_GuardsAPanickingFlusher(t *testing.T) {
	RegisterCrashFlusher(func(context.Context) { panic("flusher blew up") })
	t.Cleanup(func() { RegisterCrashFlusher(nil) })

	// A panic inside the flusher must not propagate out of flushBeforeCrash
	// (it would otherwise replace the original crash value).
	require.NotPanics(t, func() { flushBeforeCrash(t.Context()) })
}

func TestPanicCounter(t *testing.T) {
	// The meter provider is installed in TestMain; use a unique source so leftover
	// goroutines from other tests (different sources) cannot perturb this count.
	GoContinueOnPanic(t.Context(), "counter-test", func() { panic("x") })

	require.Eventually(t, func() bool {
		return panicCount(t, testMetricReader, "counter-test", "continue") == 1
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
