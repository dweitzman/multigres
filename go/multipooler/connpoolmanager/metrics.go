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

package connpoolmanager

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/semconv/v1.37.0/dbconv"

	"github.com/multigres/multigres/go/multipooler/pools/connpool"
)

// Metrics holds the shared instruments for all connection pools.
// These are created once per Manager and passed to all connection pools,
// which then record state transitions incrementally via UpDownCounters.
type Metrics struct {
	// ConnectionCount tracks connections by state (idle/used) following OTel semconv.
	// Each pool records its own transitions with its pool name as an attribute.
	ConnectionCount *connpool.ConnectionCount

	// ConnectionMax tracks max connection capacity per pool following OTel semconv.
	ConnectionMax *dbconv.ClientConnectionMax
}

// NewMetrics initializes the shared instruments for connection pool metrics.
// Returns a Metrics instance with the instruments, or an error if creation fails.
// These instruments follow the OTel database semantic conventions.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/multipooler/connpoolmanager")

	var errs []error

	connectionCount, err := connpool.NewConnectionCount(meter)
	if err != nil {
		errs = append(errs, fmt.Errorf("db.client.connection.count: %w", err))
	}

	connectionMax, err := dbconv.NewClientConnectionMax(meter)
	if err != nil {
		errs = append(errs, fmt.Errorf("db.client.connection.max: %w", err))
	}

	m := &Metrics{
		ConnectionCount: &connectionCount,
		ConnectionMax:   &connectionMax,
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
