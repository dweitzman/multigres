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

package recovery

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/coordinator"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/timer"
)

// runIfNotRunning executes fn in a goroutine only if inProgress flag is false.
// If the operation is already in progress, it logs a debug message and returns immediately.
// This prevents pile-up of concurrent operations that may be slow.
func runIfNotRunning(logger *slog.Logger, inProgress *atomic.Bool, taskName string, fn func()) {
	if !inProgress.CompareAndSwap(false, true) {
		logger.Debug("skipping task, previous run still in progress", "task", taskName)
		return
	}
	go func() {
		defer inProgress.Store(false)
		fn()
	}()
}

// Engine orchestrates health checking and automated recovery for Multigres poolers.
//
// The Engine provides high availability for Multigres
// by continuously monitoring pooler health and automatically
// recovering from failures.
//
// # Architecture
//
// The Engine runs three main loops operating at different intervals:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│                        Engine                                    │
//	├──────────────────────────────────────────────────────────────────┤
//	│                                                                  │
//	│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
//	│  │ Healthcheck Loop│  │  Recovery Loop  │  │ Maintenance Loop│   │
//	│  │  (5s)           │  │   (1s)          │  │                 │   │
//	│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
//	│           │                    │                    │            │
//	│           └────────────────────┼────────────────────┘            │
//	│                                ▼                                 │
//	│                        (State Store)                             │
//	└──────────────────────────────────────────────────────────────────┘
//
// # Loop Details
//
// Maintenance Loop:
//
//	Keeps the engine's view of the cluster up-to-date and performs general maintenance tasks.
//	Runs two types of operations at configurable intervals:
//
//	Cluster Metadata Refresh:
//	- Queries all cells in the topology to find multipoolers that are part of the shards_to_watch
//	- Reads database information for the shards being watched (this will contain durability policy information)
//
//	Bookkeeping Tasks:
//	- Forget unseen poolers (remove stale entries)
//	- Clean up stale data from the in memory state store
//
// Healthcheck Loop:
//
//	Continuously monitors the health of all poolers by polling their status.
//	This loop maintains an up-to-date health snapshot in the state store.
//
//	The health check system uses a queue-based architecture with worker pools:
//
//	1. Discovery Phase (runs during cluster metadata refresh):
//	   - When refreshClusterMetadata() discovers poolers from topology
//	   - Newly discovered poolers (never health checked) are immediately queued
//	   - This ensures fast initial health check after discovery
//
//	2. Worker Pool (always running):
//	   - N concurrent worker goroutines (configurable via health-check-workers)
//	   - Workers consume from the health check queue (blocking)
//	   - Each worker calls pollPooler() to perform the actual health check
//	   - Updates LastCheckAttempted, LastSeen, IsUpToDate in the store
//
//	3. Health Check Ticker (periodic re-queueing):
//	   - Runs every pooler-health-check-interval (e.g., 5s)
//	   - Scans all poolers in the store
//	   - Queues poolers with outdated checks (LastCheckAttempted older than interval)
//	   - Ensures continuous health monitoring even if checks fail
//
//	The queue deduplicates entries, so a pooler can only be queued once at a time.
//
//	Health Check Queue Flow:
//
//	  ┌──────────────────────┐                  ┌──────────────────────┐
//	  │  Metadata Refresh    │──new poolers  ─> │                      │
//	  │  (periodic: 30s)     │                  │   Health Check       │
//	  └──────────────────────┘                  │   Queue              │
//	                                            │   (deduplicates)     │
//	  ┌──────────────────────┐                  │                      │
//	  │  Health Check Ticker │──stale checks -> │                      │
//	  │  (periodic: 5s)      │                  └──────────┬───────────┘
//	  └──────────────────────┘                             │
//	                                                       │ consume
//	                                                       ▼
//	                                            ┌─────────────────────┐
//	                                            │   Worker Pool       │
//	                                            │   (N workers)       │
//	                                            └──────────┬──────────┘
//	                                                       │
//	                                                       │ updates
//	                                                       ▼
//	                                            ┌──────────────────────┐
//	                                            │   Pooler Store       │
//	                                            └──────────────────────┘
//
// Recovery Loop:
//
//	Detects problems and executes automated recovery actions (runs every 1s).
//	This is where the actual failover logic lives.
//
//	Recovery Loop Flow:
//
//	  ┌──────────────────────┐
//	  │   Pooler Store       │──────────┐
//	  │  (Health Snapshots)  │          │ read
//	  └──────────────────────┘          │
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Analysis Generator  │
//	                         │  (builds analyses)   │
//	                         └──────────┬───────────┘
//	                                    │ per-pooler analysis
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │   Analyzers          │
//	                         │  (detect problems)   │
//	                         └──────────┬───────────┘
//	                                    │ problems
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Deduplication       │
//	                         │  & Prioritization    │
//	                         └──────────┬───────────┘
//	                                    │ filtered problems
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Validation          │
//	                         │  (force re-poll)     │
//	                         └──────────┬───────────┘
//	                                    │ validated problems
//	                                    ▼
//	                         ┌──────────────────────┐
//	                         │  Recovery Actions    │
//	                         │  (automated repairs) │
//	                         └──────────────────────┘
//
//	Deduplication Strategy:
//
//	The recovery loop applies intelligent deduplication to avoid redundant
//	recovery attempts and ensure safe, efficient problem resolution:
//
//	Sort by Priority:
//	  - Higher priority problems are processed first
//	  - Ensures critical issues (e.g., primary failure) take precedence
//
//	Smart Filtering by Scope:
//	  - If ANY shard-wide problem exists:
//	    * Return ONLY the highest priority shard-wide problem
//	    * Rationale: Shard-wide recoveries (e.g., failover) fix multiple
//	      pooler-level issues, so addressing them first is more efficient
//	  - Otherwise (only single-pooler problems):
//	    * Deduplicate by pooler ID
//	    * Keep highest priority problem per pooler
//	    * Rationale: One pooler can have multiple issues; fixing the
//	      highest priority issue often resolves downstream problems
//
//	Validation Before Recovery:
//	  - Force re-poll affected poolers to get fresh state
//	  - Re-run analyzers to confirm problem still exists
//	  - Prevents acting on stale/transient issues
//
//	Post-Recovery Refresh:
//	  - After shard-wide recoveries, force refresh all shard poolers
//	  - Ensures accurate state before next recovery cycle
//	  - Prevents re-queueing problems that were just fixed
//
// # Configuration
//
// The Engine requires:
//   - watch-targets: List of database/tablegroup/shard targets to monitor
//   - bookkeeping-interval: How often to run cleanup tasks (default: 1m)
//   - cluster-metadata-refresh-interval: How often to refresh from topology (default: 15s)
//   - cluster-metadata-refresh-timeout: Timeout for metadata refresh operation (default: 30s)
//
// Example:
//
//	engine := Engine(
//	    "zone1",                          // cell
//	    topoStore,                        // topology service
//	    logger,                           // structured logger
//	    []string{"postgres"},             // watch entire database
//	    1*time.Minute,                    // bookkeeping interval
//	    15*time.Second,                   // metadata refresh interval
//	)
//	engine.Start()
type Engine struct {
	ts        topoclient.Store
	logger    *slog.Logger
	config    *config.Config
	rpcClient rpcclient.MultiPoolerClient

	// In-memory state store
	poolerStore *store.PoolerHealthStore

	// Health check queue for concurrent pooler polling
	healthCheckQueue *Queue

	// Current configuration values
	mu                sync.Mutex // protects shardWatchTargets
	shardWatchTargets []config.WatchTarget

	// Config reloader for dynamic updates (only shardWatchTargets is dynamic)
	reloadConfig func() []string

	// Goroutine management - prevent pile-up of concurrent operations
	metadataRefreshInProgress atomic.Bool
	bookkeepingInProgress     atomic.Bool
	healthCheckInProgress     atomic.Bool

	// Cache for deduplication (prevents redundant health checks)
	recentPollCache   map[string]time.Time
	recentPollCacheMu sync.Mutex

	// Detected problems tracking for metrics (replaced each cycle)
	detectedProblemsMu sync.Mutex
	detectedProblems   []DetectedProblemData

	// Metrics
	metrics *Metrics

	// Action factory for creating recovery actions
	actionFactory *analysis.RecoveryActionFactory

	// recoveryGracePeriodTracker tracker for grace periods before recovery actions
	recoveryGracePeriodTracker *RecoveryGracePeriodTracker

	// Recovery loop runner (using PeriodicRunner for proper Stop synchronization)
	recoveryRunner *timer.PeriodicRunner

	// Context for shutting down loops
	shutdownCtx context.Context
	cancel      context.CancelFunc

	// WaitGroup to track goroutines for graceful shutdown
	wg sync.WaitGroup
}

// NewEngine creates a new RecoveryEngine instance.
func NewEngine(
	ts topoclient.Store,
	logger *slog.Logger,
	config *config.Config,
	shardWatchTargets []config.WatchTarget,
	rpcClient rpcclient.MultiPoolerClient,
	coordinator *coordinator.Coordinator,
) *Engine {
	ctx, cancel := context.WithCancel(context.TODO())

	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	engine := &Engine{
		ts:                ts,
		logger:            logger,
		config:            config,
		rpcClient:         rpcClient,
		poolerStore:       poolerStore,
		healthCheckQueue:  NewQueue(logger, config),
		shardWatchTargets: shardWatchTargets,
		recentPollCache:   make(map[string]time.Time),
		detectedProblems:  nil,
		shutdownCtx:       ctx,
		cancel:            cancel,
	}

	// Initialize metrics
	var err error
	engine.metrics, err = NewMetrics()
	if err != nil {
		logger.Error("failed to initialize recovery metrics", "error", err)
	}

	// Register callback for pooler store size observable gauge
	err = engine.metrics.RegisterPoolerStoreSizeCallback(func() int {
		return poolerStore.Len()
	})
	if err != nil {
		logger.Error("failed to monitor pooler store size", "error", err)
	}

	// Register callback for detected problems gauge
	err = engine.metrics.RegisterDetectedProblemsCallback(func() []DetectedProblemData {
		return engine.collectDetectedProblemsData()
	})
	if err != nil {
		logger.Error("failed to register detected problems callback", "error", err)
	}

	// Create action factory for recovery actions
	engine.actionFactory = analysis.NewRecoveryActionFactory(
		config,
		poolerStore,
		rpcClient,
		ts,
		coordinator,
		logger,
	)

	// Create deadline tracker for grace periods
	engine.recoveryGracePeriodTracker = NewRecoveryGracePeriodTracker(engine.shutdownCtx, config,
		WithLogger(logger))

	// Create recovery runner (Start() will begin execution)
	engine.recoveryRunner = timer.NewPeriodicRunner(ctx, config.GetRecoveryCycleInterval())

	return engine
}

// SetConfigReloader sets the function to reload configuration dynamically.
// The reloader function should return raw string targets (e.g., from viper).
// Only shardWatchTargets can be reloaded; intervals require a restart.
func (re *Engine) SetConfigReloader(reloader func() []string) {
	re.reloadConfig = reloader
}

// Start initializes and starts the RecoveryEngine loops.
func (re *Engine) Start() error {
	re.logger.Info("starting recovery engine",
		"cell", re.config.GetCell(),
		"watch_targets", re.shardWatchTargets,
		"bookkeeping_interval", re.config.GetBookkeepingInterval(),
		"cluster_metadata_refresh_interval", re.config.GetClusterMetadataRefreshInterval(),
		"cluster_metadata_refresh_timeout", re.config.GetClusterMetadataRefreshTimeout(),
		"pooler_health_check_interval", re.config.GetPoolerHealthCheckInterval(),
		"health_check_workers", re.config.GetHealthCheckWorkers(),
	)

	// Start health check worker pool
	re.startHealthCheckWorkers()

	// Start maintenance loop (cluster metadata refresh + bookkeeping)
	re.wg.Go(func() {
		re.runMaintenanceLoop()
	})

	// Start health check ticker loop (queues poolers for health checking)
	re.wg.Go(func() {
		re.runHealthCheckTickerLoop()
	})

	// Start recovery runner (manages its own goroutines and lifecycle)
	re.recoveryRunner.Start(func(ctx context.Context) {
		re.performRecoveryCycle(ctx)
	}, func() {
		re.logger.Info("recovery loop started", "interval", re.config.GetRecoveryCycleInterval())
	})

	re.logger.Info("recovery engine started successfully")
	return nil
}

// Stop gracefully shuts down the RecoveryEngine.
// It cancels the context and waits for all goroutines to finish.
func (re *Engine) Stop() {
	re.logger.Info("stopping recovery engine")

	// Stop recovery runner first
	re.recoveryRunner.Stop()

	// Cancel context and wait for other goroutines
	re.cancel()
	re.wg.Wait()
	re.logger.Info("recovery engine stopped")
}

// runMaintenanceLoop runs the cluster metadata refresh and bookkeeping tasks.
// Supports dynamic reloading of shardWatchTargets via SetConfigReloader.
func (re *Engine) runMaintenanceLoop() {
	bookkeepingTicker := time.NewTicker(re.config.GetBookkeepingInterval())
	defer bookkeepingTicker.Stop()

	metadataTicker := time.NewTicker(re.config.GetClusterMetadataRefreshInterval())
	defer metadataTicker.Stop()

	re.logger.Info("maintenance loop started")

	// Do initial metadata refresh
	re.refreshClusterMetadata()

	for {
		select {
		case <-re.shutdownCtx.Done():
			re.logger.Info("maintenance loop stopped")
			return

		case <-metadataTicker.C:
			runIfNotRunning(re.logger, &re.metadataRefreshInProgress, "cluster_metadata_refresh", re.refreshClusterMetadata)

		case <-bookkeepingTicker.C:
			runIfNotRunning(re.logger, &re.bookkeepingInProgress, "bookkeeping", re.runBookkeeping)
		}
	}
}

// startHealthCheckWorkers starts the worker pool that consumes from the health check queue.
// Each worker runs in its own goroutine and processes health checks concurrently.
func (re *Engine) startHealthCheckWorkers() {
	numWorkers := re.config.GetHealthCheckWorkers()
	for range numWorkers {
		re.wg.Go(func() {
			re.handlePoolerHealthChecks()
		})
	}
	re.logger.Info("health check worker pool started", "workers", numWorkers)
}

// runHealthCheckTickerLoop periodically queues poolers for health checking.
func (re *Engine) runHealthCheckTickerLoop() {
	healthCheckTicker := time.NewTicker(re.config.GetPoolerHealthCheckInterval())
	defer healthCheckTicker.Stop()

	re.logger.Info("health check ticker loop started")

	for {
		select {
		case <-re.shutdownCtx.Done():
			re.logger.Info("health check ticker loop stopped")
			return

		case <-healthCheckTicker.C:
			runIfNotRunning(re.logger, &re.healthCheckInProgress, "health_check_queue", re.queuePoolersHealthCheck)
		}
	}
}

// reloadConfigs checks for configuration changes and reloads if necessary.
// Only shardWatchTargets can be reloaded; intervals require a restart.
func (re *Engine) reloadConfigs() {
	if re.reloadConfig == nil {
		return
	}

	// Get raw target strings from viper (or other config source)
	rawTargets := re.reloadConfig()

	// Handle empty targets - keep current configuration
	if len(rawTargets) == 0 {
		re.logger.Warn("ignoring empty watch-targets during reload, keeping current targets")
		return
	}

	// Parse the raw strings into ShardWatchTarget structs
	newTargets, err := config.ParseShardWatchTargets(rawTargets)
	if err != nil {
		re.logger.Error("failed to parse watch-targets during reload", "error", err)
		return
	}

	// Acquire lock and update if changed
	re.mu.Lock()
	defer re.mu.Unlock()

	if !shardWatchTargetsEqual(re.shardWatchTargets, newTargets) {
		re.logger.Info("reloading shard watch targets",
			"old", shardWatchTargetsToStrings(re.shardWatchTargets),
			"new", shardWatchTargetsToStrings(newTargets),
		)
		re.shardWatchTargets = newTargets
	}
}

// shardWatchTargetsEqual compares two ShardWatchTarget slices for equality.
func shardWatchTargetsEqual(a, b []config.WatchTarget) bool {
	return slices.Equal(a, b)
}

// shardWatchTargetsToStrings converts ShardWatchTargets to their string representations.
func shardWatchTargetsToStrings(targets []config.WatchTarget) []string {
	result := make([]string, len(targets))
	for i, t := range targets {
		result[i] = t.String()
	}
	return result
}

// collectDetectedProblemsData returns the current detected problems for metrics.
// This is called by the observable gauge callback.
func (re *Engine) collectDetectedProblemsData() []DetectedProblemData {
	re.detectedProblemsMu.Lock()
	defer re.detectedProblemsMu.Unlock()
	return re.detectedProblems
}

// updateDetectedProblems replaces the detected problems slice with current problems.
// Called each recovery cycle - the slice is replaced entirely rather than incrementally updated
// for simplicity.
func (re *Engine) updateDetectedProblems(problems []types.Problem) {
	data := make([]DetectedProblemData, 0, len(problems))
	for _, p := range problems {
		data = append(data, DetectedProblemData{
			AnalysisType: string(p.CheckName),
			DBNamespace:  p.ShardKey.Database,
			Shard:        p.ShardKey.Shard,
			PoolerID:     topoclient.MultiPoolerIDString(p.PoolerID),
		})
	}

	re.detectedProblemsMu.Lock()
	re.detectedProblems = data
	re.detectedProblemsMu.Unlock()
}

// DisableRecovery stops the recovery loop and waits for in-flight actions to complete.
// When this returns, no recovery actions are running and no new ones will start.
// Intended for testing scenarios where manual control over recovery is needed.
func (re *Engine) DisableRecovery() {
	re.logger.Warn("Disabling recovery - no automatic repairs will occur")
	re.recoveryRunner.Stop()
}

// EnableRecovery resumes the recovery loop.
func (re *Engine) EnableRecovery() {
	re.recoveryRunner.Start(func(ctx context.Context) {
		re.performRecoveryCycle(ctx)
	}, func() {
		re.logger.Info("Enabling recovery - automatic repairs will resume")
	},
	)
}

// IsRecoveryEnabled returns whether the recovery loop is currently running.
func (re *Engine) IsRecoveryEnabled() bool {
	return re.recoveryRunner.Running()
}

// TriggerRecoveryNow immediately executes recovery operations and blocks until
// no problems remain or the timeout is reached.
//
// This method:
// 1. Force health checks all poolers to get fresh state
// 2. Runs recovery cycles repeatedly until no problems are detected
// 3. Returns when either: (a) no problems remain, or (b) timeout
//
// Returns:
// - cyclesRun: number of recovery cycles executed
// - remainingProblemCodes: problem codes still detected (empty if all resolved)
// - error: any error that occurred during execution
func (re *Engine) TriggerRecoveryNow(ctx context.Context) ([]DetectedProblemData, error) {
	re.logger.InfoContext(ctx, "TriggerRecoveryNow: forcing immediate recovery execution")

	// Force health check all poolers to get fresh state
	re.logger.DebugContext(ctx, "TriggerRecoveryNow: forcing health checks on all poolers")
	re.forceHealthCheckAllShardPoolers(ctx)

	// Create channel to wait for first cycle completion
	cycleDone := make(chan error, 1)

	// Ensure recovery is running with immediate execution and wait for first cycle
	// StartWithOptions returns true if we started it (was stopped)
	wasStarted := re.recoveryRunner.StartWithOptions(
		func(ctx context.Context) {
			re.performRecoveryCycle(ctx)
		},
		timer.WithFastStart(),
		timer.WithAfterNextFullCycle(cycleDone),
	)

	if wasStarted {
		re.logger.DebugContext(ctx, "Temporarily enabled recovery for TriggerRecoveryNow")
	}

	// If we started it, stop it when we're done
	if wasStarted {
		defer re.recoveryRunner.Stop()
	}

	// Wait for first cycle to complete before polling
	select {
	case <-cycleDone:
		// First cycle done, proceed to polling
	case <-ctx.Done():
		// Timeout before first cycle completed
	}

	// Now poll until all problems are resolved or timeout
	for {
		problems := re.collectDetectedProblemsData()
		if ctx.Err() != nil || len(problems) == 0 {
			return problems, ctx.Err()
		}

		// Brief sleep to avoid tight loop
		select {
		case <-ctx.Done():
			return re.collectDetectedProblemsData(), ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// forceHealthCheckAllShardPoolers forces health checks on all poolers across all shards.
func (re *Engine) forceHealthCheckAllShardPoolers(ctx context.Context) {
	// Collect unique shards from pooler store
	shards := make(map[commontypes.ShardKey]bool)
	re.poolerStore.Range(func(poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState) bool {
		if poolerHealth != nil && poolerHealth.MultiPooler != nil {
			shardKey := commontypes.ShardKey{
				Database:   poolerHealth.MultiPooler.Database,
				TableGroup: poolerHealth.MultiPooler.TableGroup,
				Shard:      poolerHealth.MultiPooler.Shard,
			}
			shards[shardKey] = true
		}
		return true
	})

	// Force health check each shard
	for shardKey := range shards {
		re.forceHealthCheckShardPoolers(ctx, shardKey, nil /* poolersToIgnore */)
	}
}
