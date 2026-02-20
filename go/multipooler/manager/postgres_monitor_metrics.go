// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// RemedialActionType represents the type of remedial action taken.
type RemedialActionType string

const (
	RemedialActionReopenManager       RemedialActionType = "reopen_manager"
	RemedialActionStartPostgres       RemedialActionType = "start_postgres"
	RemedialActionRestoreFromBackup   RemedialActionType = "restore_from_backup"
	RemedialActionAdjustTypeToPrimary RemedialActionType = "adjust_type_to_primary"
	RemedialActionAdjustTypeToReplica RemedialActionType = "adjust_type_to_replica"
)

// RemedialActionStatus represents the possible status values for a remedial action.
type RemedialActionStatus string

const (
	RemedialActionStatusSuccess RemedialActionStatus = "success"
	RemedialActionStatusFailure RemedialActionStatus = "failure"
)

// PostgresMonitorMetrics holds OpenTelemetry metrics for PostgreSQL monitoring.
type PostgresMonitorMetrics struct {
	meter                  metric.Meter
	detectedProblems       DetectedProblems
	remedialActionDuration RemedialActionDuration
}

// DetectedProblems wraps an Int64UpDownCounter for reporting detected problems.
type DetectedProblems struct {
	metric.Int64UpDownCounter
}

// Add increments or decrements the detected problems counter.
func (m DetectedProblems) Add(ctx context.Context, delta int64, problemType, poolerID string) {
	m.Int64UpDownCounter.Add(ctx, delta,
		metric.WithAttributes(
			attribute.String("problem_type", problemType),
			attribute.String("pooler_id", poolerID),
		))
}

// RemedialActionDuration wraps a Float64Histogram for recording action durations.
// The histogram implicitly tracks count, so no separate counter is needed.
type RemedialActionDuration struct {
	metric.Float64Histogram
}

// Record records the duration of a remedial action.
func (m RemedialActionDuration) Record(ctx context.Context, seconds float64, action RemedialActionType, status RemedialActionStatus) {
	m.Float64Histogram.Record(ctx, seconds,
		metric.WithAttributes(
			attribute.String("action", string(action)),
			attribute.String("status", string(status)),
		))
}

// NewPostgresMonitorMetrics initializes OpenTelemetry metrics for PostgreSQL monitoring.
// Individual metrics that fail to initialize will use noop implementations.
func NewPostgresMonitorMetrics() (*PostgresMonitorMetrics, error) {
	m := &PostgresMonitorMetrics{
		meter: otel.Meter("github.com/multigres/multigres/multipooler/manager"),
	}

	var errs []error

	// UpDownCounter for detected problems
	detectedProblemsCounter, err := m.meter.Int64UpDownCounter(
		"multipooler.postgres_monitor.detected_problems",
		metric.WithDescription("Current count of problems detected by PostgreSQL monitor"),
	)
	if err != nil {
		errs = append(errs, err)
		m.detectedProblems = DetectedProblems{noop.Int64UpDownCounter{}}
	} else {
		m.detectedProblems = DetectedProblems{detectedProblemsCounter}
	}

	// Histogram for remedial action duration (implicitly tracks count)
	remedialActionDurationHist, err := m.meter.Float64Histogram(
		"multipooler.postgres_monitor.remedial_action.duration",
		metric.WithDescription("Duration of remedial actions by PostgreSQL monitor"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, err)
		m.remedialActionDuration = RemedialActionDuration{noop.Float64Histogram{}}
	} else {
		m.remedialActionDuration = RemedialActionDuration{remedialActionDurationHist}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
