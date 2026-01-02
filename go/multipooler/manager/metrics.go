// Copyright 2025 Supabase, Inc.
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
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Metrics holds all OpenTelemetry metrics for the multipooler manager.
// Connection pool metrics (db.client.connection.count, db.client.connection.max)
// are handled directly by connpool using dbconv UpDownCounters.
type Metrics struct {
	meter metric.Meter

	// Replication lag gauge (only relevant for replicas)
	replicationLag metric.Float64ObservableGauge
}

// NewMetrics initializes OpenTelemetry metrics for the multipooler manager.
// Returns a Metrics instance (with noop fallbacks for failed metrics) and any initialization
// errors that occurred.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{
		meter: otel.Meter("github.com/multigres/multigres/go/multipooler/manager"),
	}

	var errs []error

	// Gauge for replication lag (per-instance, like vttablet)
	replicationLagGauge, err := m.meter.Float64ObservableGauge(
		"multipooler.replication.lag",
		metric.WithDescription("Replication lag for this replica (0 for primaries)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("multipooler.replication.lag gauge: %w", err))
		m.replicationLag = noop.Float64ObservableGauge{}
	} else {
		m.replicationLag = replicationLagGauge
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// ReplicationLagProvider returns replication lag data for metrics.
// Returns (lag duration, isPrimary, error).
type ReplicationLagProvider func() (time.Duration, bool, error)

// RegisterReplicationLagCallback registers a callback for the replication lag gauge.
// The provider is called periodically to observe current replication lag.
// For primaries, lag is reported as 0. For replicas, lag is reported from the heartbeat reader.
func (m *Metrics) RegisterReplicationLagCallback(provider ReplicationLagProvider) error {
	if provider == nil {
		return nil
	}
	_, err := m.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			lag, isPrimary, providerErr := provider()
			if providerErr != nil {
				// Don't report lag if there's an error (e.g., no heartbeat received).
				// The callback should succeed even if the provider fails temporarily.
				return nil //nolint:nilerr // intentionally ignore provider errors
			}
			if isPrimary {
				// Primaries have 0 lag by definition
				observer.ObserveFloat64(m.replicationLag, 0)
			} else {
				observer.ObserveFloat64(m.replicationLag, lag.Seconds())
			}
			return nil
		},
		m.replicationLag,
	)
	return err
}
