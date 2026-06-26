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
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// panicCounter counts recovered panics, keyed by the source label and the mode
// (continue|crash|rpc) the panic was handled with. A non-zero rate indicates a
// logic bug — alert on it. Initialized lazily so the package has no init-time
// dependency on the OTel SDK being configured; until then a noop counter is used.
var (
	panicCounterOnce sync.Once
	panicCounter     metric.Int64Counter = noop.Int64Counter{}
)

func initPanicCounter() {
	c, err := otel.Meter("github.com/multigres/multigres/go/tools/safego").Int64Counter(
		"goroutine.panic.recovered",
		metric.WithDescription("Number of panics recovered by safego, by source and handling mode"),
	)
	if err == nil {
		panicCounter = c
	}
}

// incPanic increments the recovered-panic counter for the given source and mode.
func incPanic(ctx context.Context, source string, mode panicMode) {
	panicCounterOnce.Do(initPanicCounter)
	panicCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("mode", string(mode)),
	))
}
