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

package consensus

// SpanContext is an opaque propagation token. The consensus core treats it as
// an identifier to store and pass between components; only the Tracer
// implementation interprets its contents. A zero SpanContext (Valid==false)
// means "no parent span" and is safe to use anywhere a SpanContext is expected.
//
// The field layout matches OTEL's trace.SpanContext (16-byte trace ID + 8-byte
// span ID) so production adapters can copy fields directly with no encoding.
type SpanContext struct {
	TraceID [16]byte // zero if !Valid
	SpanID  [8]byte  // zero if !Valid
	Valid   bool
}

// Span represents an in-progress traced operation within the consensus layer.
// All methods are safe to call on a nil Span (they are no-ops).
type Span interface {
	// SetAttr attaches a key-value attribute to the span.
	SetAttr(key string, value any)
	// RecordError marks the span as having encountered an error.
	RecordError(err error)
	// End closes the span. tick is the current simulation tick; production
	// implementations may use wall-clock time instead.
	End(tick int64)
	// SpanContext returns the propagation token for this span, suitable for
	// embedding in indicators or passing to child span creation methods.
	SpanContext() SpanContext
}

// CoordTracer creates the specific named spans that a CoordNode can open.
// Each method corresponds to one distinct traced operation; drivers map them to
// backend spans with appropriate names and default attributes.
//
// All methods must be safe to call on a nil receiver (returning a nil Span).
// Nil Span is fully functional — all its methods are no-ops.
type CoordTracer interface {
	// StartFailover opens a span for a coordinator-led failover operation,
	// initiated because stalePrimaryID is considered unreachable.
	StartFailover(tick int64, stalePrimaryID NodeID) Span

	// StartRecruitment opens a child span for the revocation/recruitment phase
	// of an ongoing failover. parent is the failover span's context.
	StartRecruitment(tick int64, parent SpanContext) Span

	// StartPropose opens a child span for the propose phase (writing the new
	// term to shadow WAL on all recruited nodes).
	StartPropose(tick int64, parent SpanContext) Span
}

// PoolerTracer creates the specific named spans that a PoolerNode can open.
//
// All methods must be safe to call on a nil receiver (returning a nil Span).
type PoolerTracer interface {
	// StartHandleRecruit opens a span for processing a RecruitIndicator from
	// a coordinator. parent is the trace context from the indicator.
	StartHandleRecruit(tick int64, parent SpanContext) Span

	// StartApplyTerm opens a span for applying a new term received via shadow
	// WAL (WriteShadowWALIndicator with ApplyNow=true).
	StartApplyTerm(tick int64, parent SpanContext) Span
}

// --- CoordMetrics -------------------------------------------------------

// CoordMetrics holds all observable quantities for a CoordNode.
// The zero value is safe: all metric wrapper fields default to no-ops.
type CoordMetrics struct {
	// TermChangesTotal counts quorum-confirmed term advances (both WAL-driven
	// and coordinator-led). Recommended OTel name: consensus.coord.term_changes_total
	TermChangesTotal TermChangesTotalCounter

	// FailoverTotal counts completed coordinator-led term changes, labelled by
	// outcome. Recommended OTel name: consensus.coord.failover.total
	FailoverTotal FailoverTotalCounter

	// FailoverDurationTicks records how many ticks a coordinator-led failover
	// took from start to quorum establishment.
	// Recommended OTel name: consensus.coord.failover.duration_ticks
	FailoverDurationTicks FailoverDurationHist

	// HealthyNodes reports the number of nodes currently considered healthy by
	// this coordinator. Recommended OTel name: consensus.coord.healthy_nodes
	HealthyNodes HealthyNodesGauge

	// ResumeSendsTotal counts Resume requests sent, labelled by whether it was
	// the first send or a retry.
	// Recommended OTel name: consensus.coord.resume_sends_total
	ResumeSendsTotal ResumeSendsTotalCounter
}

// FailoverOutcome is the result of a coordinator-led term change.
type FailoverOutcome string

const (
	FailoverOutcomeCompleted FailoverOutcome = "completed"
	FailoverOutcomeAbandoned FailoverOutcome = "abandoned"
)

// ResumeSendKind distinguishes the first Resume send from a retry.
type ResumeSendKind string

const (
	ResumeSendKindFirst ResumeSendKind = "first"
	ResumeSendKindRetry ResumeSendKind = "retry"
)

// TermChangesTotalCounter counts quorum term advances.
type TermChangesTotalCounter struct {
	fn func(n int64, sc SpanContext)
}

// NewTermChangesTotalCounter creates a TermChangesTotalCounter backed by fn.
func NewTermChangesTotalCounter(fn func(n int64, sc SpanContext)) TermChangesTotalCounter {
	return TermChangesTotalCounter{fn: fn}
}

// Add increments the counter by n, attaching sc as an exemplar hint.
func (c TermChangesTotalCounter) Add(n int64, sc SpanContext) {
	if c.fn != nil {
		c.fn(n, sc)
	}
}

// FailoverTotalCounter counts coordinator-led term changes by outcome.
type FailoverTotalCounter struct {
	fn func(outcome FailoverOutcome, sc SpanContext)
}

// NewFailoverTotalCounter creates a FailoverTotalCounter backed by fn.
func NewFailoverTotalCounter(fn func(outcome FailoverOutcome, sc SpanContext)) FailoverTotalCounter {
	return FailoverTotalCounter{fn: fn}
}

// Add records one failover with the given outcome.
func (c FailoverTotalCounter) Add(outcome FailoverOutcome, sc SpanContext) {
	if c.fn != nil {
		c.fn(outcome, sc)
	}
}

// FailoverDurationHist records failover duration in ticks.
type FailoverDurationHist struct {
	fn func(ticks int64, sc SpanContext)
}

// NewFailoverDurationHist creates a FailoverDurationHist backed by fn.
func NewFailoverDurationHist(fn func(ticks int64, sc SpanContext)) FailoverDurationHist {
	return FailoverDurationHist{fn: fn}
}

// Record observes a failover duration.
func (h FailoverDurationHist) Record(ticks int64, sc SpanContext) {
	if h.fn != nil {
		h.fn(ticks, sc)
	}
}

// HealthyNodesGauge reports the current count of healthy nodes.
type HealthyNodesGauge struct {
	fn func(count int64, sc SpanContext)
}

// NewHealthyNodesGauge creates a HealthyNodesGauge backed by fn.
func NewHealthyNodesGauge(fn func(count int64, sc SpanContext)) HealthyNodesGauge {
	return HealthyNodesGauge{fn: fn}
}

// Set reports the current healthy node count.
func (g HealthyNodesGauge) Set(count int64, sc SpanContext) {
	if g.fn != nil {
		g.fn(count, sc)
	}
}

// ResumeSendsTotalCounter counts Resume sends by kind.
type ResumeSendsTotalCounter struct {
	fn func(n int64, kind ResumeSendKind, sc SpanContext)
}

// NewResumeSendsTotalCounter creates a ResumeSendsTotalCounter backed by fn.
func NewResumeSendsTotalCounter(fn func(n int64, kind ResumeSendKind, sc SpanContext)) ResumeSendsTotalCounter {
	return ResumeSendsTotalCounter{fn: fn}
}

// Add records n Resume sends of the given kind.
func (c ResumeSendsTotalCounter) Add(n int64, kind ResumeSendKind, sc SpanContext) {
	if c.fn != nil {
		c.fn(n, kind, sc)
	}
}

// spanContext returns the SpanContext of s, or a zero SpanContext if s is nil.
// Used internally to safely extract a context from a potentially-nil span.
func spanContext(s Span) SpanContext {
	if s == nil {
		return SpanContext{}
	}
	return s.SpanContext()
}

// endSpan calls End on s and sets the pointer to nil, guarding against nil s.
// Intended for use in deferred cleanup or phase transitions.
func endSpan(s *Span, tick int64) {
	if s == nil || *s == nil {
		return
	}
	(*s).End(tick)
	*s = nil
}
