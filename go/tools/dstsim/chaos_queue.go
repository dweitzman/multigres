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

// ChaosQueue is an exported optionally-chaotic FIFO delivery queue.
// The zero value is ready to use: items are delivered in push order with a
// 1-tick delay and no drops or duplicates. Use SetChaos to inject randomness.
type ChaosQueue[T any] struct {
	q chaosQueue[T]
}

// SetChaos configures the chaos parameters for the queue. The rng field in p
// enables random delays, drops, and duplicates. With rng nil (the default) the
// queue delivers items reliably in FIFO order after max(1, MinDelay) ticks.
func (c *ChaosQueue[T]) SetChaos(p ChaosParams) {
	c.q.chaos = p
}

// Push enqueues item at tick. Returns false if the item was dropped by chaos;
// true if it was enqueued (and possibly duplicated).
func (c *ChaosQueue[T]) Push(item T, tick int64) bool {
	return c.q.push(item, tick)
}

// Pull removes and returns all items whose scheduled delivery tick is <= tick.
func (c *ChaosQueue[T]) Pull(tick int64) []T {
	return c.q.pull(tick)
}

// Drain removes and returns all buffered items regardless of delivery tick.
// Use during restart or teardown to clear in-flight items.
func (c *ChaosQueue[T]) Drain() []T {
	return c.q.drain()
}

// chaosEntry holds a buffered item and its scheduled delivery tick.
type chaosEntry[T any] struct {
	item      T
	deliverAt int64
}

// chaosQueue is an optionally-chaotic delivery buffer for a single channel.
// Items are pushed with a tick and pulled when their scheduled tick arrives.
//
// When Chaos.Reorder is false (the default zero value), push order is preserved:
// each item's deliverAt is clamped to be at least as large as the previous
// item's deliverAt, so items are always delivered in push order. This means
// the buffer stays sorted and pull can exit early on the first un-ready item.
//
// When Chaos.Reorder is true, each item receives an independent random deliverAt,
// allowing messages to arrive out of push order. Pull must scan the full buffer.
type chaosQueue[T any] struct {
	chaos  ChaosParams
	buf    []chaosEntry[T]
	lastAt int64 // last deliverAt assigned; used for FIFO enforcement when !chaos.Reorder
}

// push enqueues item using the queue's ChaosParams to decide delivery fate.
// Returns false if the item is dropped; true otherwise. When Reorder is false,
// deliverAt is clamped to preserve push-order delivery. Duplicates (if generated
// by chaos) are enqueued at the same deliverAt as the original.
func (q *chaosQueue[T]) push(item T, tick int64) bool {
	deliver, delay, dup := q.chaos.Decide()
	if !deliver {
		return false
	}
	at := tick + delay
	if !q.chaos.Reorder {
		if at < q.lastAt {
			at = q.lastAt
		}
		q.lastAt = at
	}
	q.buf = append(q.buf, chaosEntry[T]{item: item, deliverAt: at})
	if dup {
		q.buf = append(q.buf, chaosEntry[T]{item: item, deliverAt: at})
	}
	return true
}

// pull removes and returns all items whose deliverAt is <= tick.
//
// When Reorder is false the buffer is sorted by deliverAt (non-decreasing),
// so pull stops at the first unready item. When Reorder is true, pull must
// scan the full buffer.
func (q *chaosQueue[T]) pull(tick int64) []T {
	if len(q.buf) == 0 {
		return nil
	}
	if !q.chaos.Reorder {
		// Buffer is sorted; find the first item that is not yet ready.
		cut := 0
		for cut < len(q.buf) && q.buf[cut].deliverAt <= tick {
			cut++
		}
		if cut == 0 {
			return nil
		}
		out := make([]T, cut)
		for i, e := range q.buf[:cut] {
			out[i] = e.item
		}
		q.buf = q.buf[cut:]
		return out
	}
	// Reorder=true: scan full buffer, partition in place.
	var out []T
	n := 0
	for _, e := range q.buf {
		if e.deliverAt <= tick {
			out = append(out, e.item)
		} else {
			q.buf[n] = e
			n++
		}
	}
	q.buf = q.buf[:n]
	return out
}

// drain removes and returns all buffered items regardless of their deliverAt.
// Used for stage transitions: transfer in-flight items to the next stage's queue.
func (q *chaosQueue[T]) drain() []T {
	if len(q.buf) == 0 {
		return nil
	}
	out := make([]T, len(q.buf))
	for i, e := range q.buf {
		out[i] = e.item
	}
	q.buf = q.buf[:0]
	q.lastAt = 0
	return out
}
