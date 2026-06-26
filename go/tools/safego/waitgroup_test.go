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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroup_GoContinueOnPanic_TracksAndContains(t *testing.T) {
	var wg WaitGroup
	var ran atomic.Int32

	// One worker panics, two complete; Wait must return (panic contained) and
	// every worker's Done must have fired.
	wg.GoContinueOnPanic(t.Context(), "wg-test-ok-1", func() { ran.Add(1) })
	wg.GoContinueOnPanic(t.Context(), "wg-test-boom", func() { ran.Add(1); panic("boom") })
	wg.GoContinueOnPanic(t.Context(), "wg-test-ok-2", func() { ran.Add(1) })

	wg.Wait() // would hang if Done didn't fire on the panicking worker; would crash if not recovered

	assert.Equal(t, int32(3), ran.Load(), "all workers ran; the panic was contained, not propagated")
}
