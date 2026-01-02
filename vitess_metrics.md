# Vitess Prometheus Metrics Reference

This document lists all Prometheus metrics available in Vitess components (vtgate, vttablet, vtorc).

## Metric Type Legend

| Vitess Type | Prometheus Type | Description                                  |
| ----------- | --------------- | -------------------------------------------- |
| `Counter`   | `counter`       | Monotonically increasing value               |
| `Gauge`     | `gauge`         | Value that can go up or down                 |
| `Timings`   | `histogram`     | Request duration distribution with buckets   |
| `Histogram` | `histogram`     | Value distribution with configurable buckets |
| `Rates`     | `gauge`         | Computed rate over time windows              |

---

## VTGate Metrics

### Query Execution

| Metric                         | Type      | Labels                    | Description                                                  |
| ------------------------------ | --------- | ------------------------- | ------------------------------------------------------------ |
| `QueryExecutions`              | counter   | `Query`, `Plan`, `Tablet` | Queries executed by query type, plan type, tablet type       |
| `QueryRoutes`                  | counter   | `Query`, `Plan`, `Tablet` | Queries routed to tablets                                    |
| `QueryExecutionsByTable`       | counter   | `Query`, `Table`          | Queries per table                                            |
| `TransactionsProcessed`        | counter   | `Shard`, `Type`           | Transactions by distribution (single/cross) and type (rw/ro) |
| `CommitModeTimings`            | histogram | `mode`                    | Commit mode duration                                         |
| `CommitUnresolved`             | counter   | -                         | Failed atomic commits                                        |
| `PartialSuccessScatterQueries` | counter   | -                         | Partially successful scatter queries                         |

### Query Plan Cache

| Metric                    | Type    | Labels | Description              |
| ------------------------- | ------- | ------ | ------------------------ |
| `QueryPlanCacheLength`    | gauge   | -      | Plan cache entry count   |
| `QueryPlanCacheSize`      | gauge   | -      | Plan cache size in bytes |
| `QueryPlanCacheCapacity`  | gauge   | -      | Plan cache max capacity  |
| `QueryPlanCacheHits`      | counter | -      | Plan cache hits          |
| `QueryPlanCacheMisses`    | counter | -      | Plan cache misses        |
| `QueryPlanCacheEvictions` | counter | -      | Plan cache evictions     |

### Errors and Warnings

| Metric                    | Type    | Labels                                    | Description                                                                                         |
| ------------------------- | ------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `VtgateApiErrorCounts`    | counter | `Operation`, `Keyspace`, `DbType`, `Code` | API errors                                                                                          |
| `VtGateWarnings`          | counter | `type`                                    | Warnings (IgnoredSet, NonAtomicCommit, ResultsExceeded, WarnPayloadSizeExceeded, WarnUnshardedOnly) |
| `VindexUnknownParameters` | gauge   | -                                         | Unrecognized vindex parameters                                                                      |
| `TabletCallErrorCount`    | counter | `Keyspace`, `ShardName`, `ErrorCode`      | Tablet call errors                                                                                  |

### VSchema

| Metric                | Type    | Labels    | Description           |
| --------------------- | ------- | --------- | --------------------- |
| `VtgateVSchemaCounts` | counter | `changes` | VSchema change counts |

### VStream

| Metric                                | Type    | Labels                            | Description                      |
| ------------------------------------- | ------- | --------------------------------- | -------------------------------- |
| `VStreamEventsDelayedBySkewAlignment` | counter | -                                 | Events delayed by skew alignment |
| `VStreamsCreated`                     | counter | `Keyspace`, `Shard`, `TabletType` | VStreams created                 |
| `VStreamsLag`                         | gauge   | `Keyspace`, `Shard`, `TabletType` | VStream lag                      |
| `VStreamsCount`                       | counter | `Keyspace`, `Shard`, `TabletType` | VStream count                    |
| `VStreamsEventsStreamed`              | counter | `Keyspace`, `Shard`, `TabletType` | Events streamed                  |
| `VStreamsEndedWithErrors`             | counter | `Keyspace`, `Shard`, `TabletType` | VStreams ended with errors       |

### Results

| Metric                    | Type    | Labels     | Description                |
| ------------------------- | ------- | ---------- | -------------------------- |
| `RowsReturned`            | counter | `Keyspace` | Rows returned              |
| `RowsAffected`            | counter | `Keyspace` | Rows affected              |
| `QueryTextCharsProcessed` | counter | `Keyspace` | Query text chars processed |

### Failover Buffer

| Metric                          | Type    | Labels                            | Description                                                                                    |
| ------------------------------- | ------- | --------------------------------- | ---------------------------------------------------------------------------------------------- |
| `BufferStarts`                  | counter | `Keyspace`, `ShardName`           | Buffer start events                                                                            |
| `BufferStops`                   | counter | `Keyspace`, `ShardName`, `Reason` | Buffer stop events (Reason: ReshardingComplete, NewPrimarySeen, MaxDurationExceeded, Shutdown) |
| `BufferSize`                    | gauge   | -                                 | Configured buffer size                                                                         |
| `BufferFailoverDurationSumMs`   | counter | `Keyspace`, `ShardName`           | Cumulative failover duration                                                                   |
| `BufferUtilizationSum`          | gauge   | `Keyspace`, `ShardName`           | Cumulative buffer utilization %                                                                |
| `BufferUtilizationDryRunSum`    | counter | `Keyspace`, `ShardName`           | Dry-run utilization %                                                                          |
| `BufferRequestsBuffered`        | counter | `Keyspace`, `ShardName`           | Requests buffered                                                                              |
| `BufferRequestsBufferedDryRun`  | counter | `Keyspace`, `ShardName`           | Dry-run buffered requests                                                                      |
| `BufferRequestsDrained`         | counter | `Keyspace`, `ShardName`           | Requests drained                                                                               |
| `BufferRequestsEvicted`         | counter | `Keyspace`, `ShardName`, `Reason` | Requests evicted (Reason: BufferFull, ContextDone, WindowExceeded)                             |
| `BufferRequestsSkipped`         | counter | `Keyspace`, `ShardName`, `Reason` | Requests skipped                                                                               |
| `BufferLastFailoverDurationMs`  | gauge   | `Keyspace`, `ShardName`           | Last failover duration                                                                         |
| `BufferLastRequestsInFlightMax` | gauge   | `Keyspace`, `ShardName`           | Max in-flight during last failover                                                             |
| `BufferLastRequestsDryRunMax`   | gauge   | `Keyspace`, `ShardName`           | Max dry-run during last failover                                                               |

### Rate Metrics

| Metric              | Type  | Labels      | Description                      |
| ------------------- | ----- | ----------- | -------------------------------- |
| `QPSByOperation`    | gauge | `Operation` | Queries per second by operation  |
| `QPSByKeyspace`     | gauge | `Keyspace`  | QPS by keyspace                  |
| `QPSByDbType`       | gauge | `DbType`    | QPS by tablet type               |
| `ErrorsByOperation` | gauge | `Operation` | Errors per second by operation   |
| `ErrorsByKeyspace`  | gauge | `Keyspace`  | Errors per second by keyspace    |
| `ErrorsByDbType`    | gauge | `DbType`    | Errors per second by tablet type |
| `ErrorsByCode`      | gauge | `Code`      | Errors per second by code        |

### Replica Warming

| Metric                        | Type    | Labels              | Description            |
| ----------------------------- | ------- | ------------------- | ---------------------- |
| `ReplicaWarmingReadsMirrored` | counter | `Keyspace`, `Shard` | Warming reads mirrored |

---

## VTTablet Metrics

### Query Engine

| Metric                          | Type    | Labels                        | Description                   |
| ------------------------------- | ------- | ----------------------------- | ----------------------------- |
| `QueryCounts`                   | counter | `Table`, `Plan`               | Query counts                  |
| `QueryCountsWithTabletType`     | counter | `Table`, `Plan`, `TabletType` | Query counts with tablet type |
| `QueryTimesNs`                  | counter | `Table`, `Plan`               | Query times in nanoseconds    |
| `QueryRowsAffected`             | counter | `Table`, `Plan`               | Rows affected                 |
| `QueryRowsReturned`             | counter | `Table`, `Plan`               | Rows returned                 |
| `QueryTextCharactersProcessed`  | counter | `Table`, `Plan`               | Query text chars processed    |
| `QueryErrorCounts`              | counter | `Table`, `Plan`               | Error counts                  |
| `QueryErrorCountsWithCode`      | counter | `Table`, `Plan`, `Code`       | Errors with code              |
| `QueryEnginePlanCacheLength`    | gauge   | -                             | Plan cache entries            |
| `QueryEnginePlanCacheSize`      | gauge   | -                             | Plan cache bytes              |
| `QueryEnginePlanCacheCapacity`  | gauge   | -                             | Plan cache capacity           |
| `QueryEnginePlanCacheHits`      | counter | -                             | Cache hits                    |
| `QueryEnginePlanCacheMisses`    | counter | -                             | Cache misses                  |
| `QueryEnginePlanCacheEvictions` | counter | -                             | Cache evictions               |
| `MaxResultSize`                 | gauge   | -                             | Max result size setting       |
| `WarnResultSize`                | gauge   | -                             | Warn result size setting      |
| `StreamBufferSize`              | gauge   | -                             | Stream buffer size            |
| `TableACLExemptCount`           | counter | -                             | ACL exempt count              |

### Query Timings

| Metric                     | Type      | Labels       | Description            |
| -------------------------- | --------- | ------------ | ---------------------- |
| `Mysql`                    | histogram | `operation`  | MySQL query time       |
| `Queries`                  | histogram | `plan_type`  | Query timings by plan  |
| `QueryTimingsByTabletType` | histogram | `TabletType` | Timings by tablet type |
| `Waits`                    | histogram | `type`       | Wait operations        |

### Transactions

| Metric                   | Type      | Labels        | Description                 |
| ------------------------ | --------- | ------------- | --------------------------- |
| `Transactions`           | histogram | `operation`   | Transaction stats           |
| `TransactionTimeout`     | gauge     | -             | Transaction timeout         |
| `OlapTransactionTimeout` | gauge     | -             | OLAP transaction timeout    |
| `ReservedConnections`    | histogram | `operation`   | Reserved connection stats   |
| `UnresolvedTransaction`  | gauge     | `ManagerType` | Unresolved 2PC transactions |
| `CommitPreparedFail`     | counter   | `FailureType` | Failed prepared commits     |
| `RedoPreparedFail`       | counter   | `FailureType` | Failed redo operations      |

### Transaction Limiter

| Metric                      | Type    | Labels | Description        |
| --------------------------- | ------- | ------ | ------------------ |
| `TxLimiterRejections`       | counter | `user` | Rejections by user |
| `TxLimiterRejectionsDryRun` | counter | `user` | Dry-run rejections |

### Transaction Serializer

| Metric                                  | Type    | Labels  | Description             |
| --------------------------------------- | ------- | ------- | ----------------------- |
| `TxSerializerWaits`                     | counter | `table` | Serializer waits        |
| `TxSerializerWaitsDryRun`               | counter | `table` | Dry-run waits           |
| `TxSerializerQueueExceeded`             | counter | `table` | Queue exceeded          |
| `TxSerializerQueueExceededDryRun`       | counter | `table` | Dry-run queue exceeded  |
| `TxSerializerGlobalQueueExceeded`       | counter | -       | Global queue exceeded   |
| `TxSerializerGlobalQueueExceededDryRun` | counter | -       | Dry-run global exceeded |

### Transaction Throttler

| Metric                                     | Type    | Labels           | Description           |
| ------------------------------------------ | ------- | ---------------- | --------------------- |
| `TransactionThrottlerRunning`              | gauge   | -                | Throttler state (0/1) |
| `TransactionThrottlerRequests`             | counter | `workload`       | Requests by workload  |
| `TransactionThrottlerThrottled`            | counter | `workload`       | Throttled requests    |
| `TransactionThrottlerHealthchecksRead`     | counter | `cell`, `DbType` | Healthchecks read     |
| `TransactionThrottlerHealthchecksRecorded` | counter | `cell`, `DbType` | Healthchecks recorded |

### Replication & Heartbeat

| Metric                     | Type    | Labels | Description                |
| -------------------------- | ------- | ------ | -------------------------- |
| `replicationLagSec`        | gauge   | -      | Replication lag in seconds |
| `HeartbeatWrites`          | counter | -      | Heartbeats written         |
| `HeartbeatWriteErrors`     | counter | -      | Write errors               |
| `HeartbeatReads`           | counter | -      | Heartbeats read            |
| `HeartbeatReadErrors`      | counter | -      | Read errors                |
| `HeartbeatCumulativeLagNs` | counter | -      | Cumulative lag (ns)        |
| `HeartbeatCurrentLagNs`    | gauge   | -      | Current lag (ns)           |

### VStreamer

| Metric                                   | Type      | Labels         | Description                             |
| ---------------------------------------- | --------- | -------------- | --------------------------------------- |
| `VStreamerCount`                         | gauge     | -              | Active vstreamers                       |
| `VStreamerEventsStreamed`                | counter   | -              | Events streamed                         |
| `VStreamerCompressedTransactionsDecoded` | counter   | -              | Compressed txns decoded                 |
| `VStreamPacketSize`                      | gauge     | -              | Max packet size                         |
| `VStreamerNumPackets`                    | counter   | -              | Packets sent                            |
| `VStreamersCreated`                      | counter   | -              | Vstreamers created                      |
| `VStreamersEndedWithErrors`              | counter   | -              | Ended with errors                       |
| `VStreamerPhaseTiming`                   | histogram | `phase-timing` | Phase timings                           |
| `VStreamerErrors`                        | counter   | `type`         | Errors (Catchup, Copy, Send, TablePlan) |
| `VStreamerFlushedBinlogs`                | counter   | -              | Binlog flushes                          |
| `VSchemaErrors`                          | counter   | -              | VSchema errors                          |
| `VSchemaUpdates`                         | counter   | -              | VSchema updates                         |

### Row/Result Streamer

| Metric                           | Type      | Labels             | Description              |
| -------------------------------- | --------- | ------------------ | ------------------------ |
| `ResultStreamerNumPackets`       | counter   | -                  | Result packets           |
| `ResultStreamerNumRows`          | counter   | -                  | Result rows              |
| `RowStreamerNumPackets`          | counter   | -                  | Row packets              |
| `RowStreamerNumRows`             | counter   | -                  | Row rows                 |
| `RowStreamerWaits`               | histogram | `copy-phase-waits` | Wait timings             |
| `RowStreamerMaxInnoDBTrxHistLen` | gauge     | -                  | InnoDB trx history limit |
| `RowStreamerMaxMySQLReplLagSecs` | gauge     | -                  | MySQL repl lag limit     |
| `TableStreamerNumTables`         | counter   | -                  | Tables streamed          |

### Schema Engine

| Metric                    | Type      | Labels           | Description           |
| ------------------------- | --------- | ---------------- | --------------------- |
| `SchemaReload`            | histogram | `type`           | Schema reload timings |
| `SchemaReloadTime`        | gauge     | -                | Reload interval       |
| `TableFileSize`           | gauge     | `Table`          | Table file size       |
| `TableAllocatedSize`      | gauge     | `Table`          | Table allocated size  |
| `TableRows`               | gauge     | `Table`          | Estimated rows        |
| `TableClusteredIndexSize` | gauge     | `Table`          | Clustered index size  |
| `IndexCardinality`        | gauge     | `Table`, `Index` | Index cardinality     |
| `IndexBytes`              | gauge     | `Table`, `Index` | Index size            |
| `InnodbRowsRead`          | counter   | -                | InnoDB rows read      |

### Throttler

| Metric                          | Type    | Labels | Description        |
| ------------------------------- | ------- | ------ | ------------------ |
| `ThrottlerCheckAnyTotal`        | counter | -      | Total checks       |
| `ThrottlerCheckRequest`         | counter | -      | Check requests     |
| `GetThrottlerStatusRequest`     | counter | -      | Status requests    |
| `ThrottlerHeartbeatRequests`    | counter | -      | Heartbeat requests |
| `ThrottlerProbeRecentlyChecked` | counter | -      | Recent probes      |

### Tablet Manager

| Metric            | Type    | Labels         | Description               |
| ----------------- | ------- | -------------- | ------------------------- |
| `TabletTypeCount` | counter | `type`         | Type changes              |
| `BackupIsRunning` | gauge   | `mode`         | Backup running (0/1)      |
| `IsInSrvKeyspace` | gauge   | -              | In serving keyspace (0/1) |
| `TabletTags`      | gauge   | `key`, `value` | Tablet tags               |

### Errors and Kills

| Metric           | Type    | Labels       | Description                                                                                                                 |
| ---------------- | ------- | ------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `Kills`          | counter | `query_type` | Killed connections (Transactions, Queries, ReservedConnection)                                                              |
| `InternalErrors` | counter | `type`       | Internal errors (Task, StrayTransactions, Panic, HungQuery, Schema, TwopcCommit, TwopcResurrection, WatchdogFail, Messages) |
| `Warnings`       | counter | `type`       | Warnings (ResultsExceeded)                                                                                                  |
| `ErrorCounters`  | counter | `type`       | Errors (Fail, TxPoolFull, NotInTx, Deadlock, etc.)                                                                          |

### Table ACL

| Metric                 | Type    | Labels                                          | Description        |
| ---------------------- | ------- | ----------------------------------------------- | ------------------ |
| `TableACLAllowed`      | counter | `TableName`, `TableGroup`, `PlanID`, `Username` | ACL accepts        |
| `TableACLDenied`       | counter | `TableName`, `TableGroup`, `PlanID`, `Username` | ACL denials        |
| `TableACLPseudoDenied` | counter | `TableName`, `TableGroup`, `PlanID`, `Username` | ACL pseudo-denials |

### User Stats

| Metric                    | Type    | Labels                          | Description             |
| ------------------------- | ------- | ------------------------------- | ----------------------- |
| `UserTableQueryCount`     | counter | `TableName`, `CallerID`, `Type` | Queries by caller/table |
| `UserTableQueryTimesNs`   | counter | `TableName`, `CallerID`, `Type` | Query latency           |
| `UserTransactionCount`    | counter | `CallerID`, `Conclusion`        | Transactions by caller  |
| `UserTransactionTimesNs`  | counter | `CallerID`, `Conclusion`        | Transaction latency     |
| `UserActiveReservedCount` | counter | `CallerID`                      | Active reserved conns   |
| `UserReservedCount`       | counter | `CallerID`                      | Reserved connections    |
| `UserReservedTimesNs`     | counter | `CallerID`                      | Reserved conn latency   |

### Connection Pool

| Metric         | Type      | Labels     | Description                |
| -------------- | --------- | ---------- | -------------------------- |
| `*GetConnTime` | histogram | `Settings` | Time to acquire connection |

### Messager

| Metric         | Type  | Labels               | Description   |
| -------------- | ----- | -------------------- | ------------- |
| `MessageStats` | gauge | `TableName`, `State` | Message stats |

### Online DDL

| Metric                           | Type    | Labels | Description           |
| -------------------------------- | ------- | ------ | --------------------- |
| `StartedMigrations`              | counter | -      | Initiated migrations  |
| `SuccessfulMigrations`           | counter | -      | Successful migrations |
| `FailedMigrations`               | counter | -      | Failed migrations     |
| `OnlineDDLStaleMigrationMinutes` | gauge   | -      | Stale migration age   |

### VReplication

| Metric                                | Type    | Labels               | Description    |
| ------------------------------------- | ------- | -------------------- | -------------- |
| `VReplicationStreamCount`             | gauge   | -                    | Active streams |
| `VReplicationLagSecondsMax`           | gauge   | -                    | Max lag        |
| `VReplicationLagSecondsTotal`         | counter | `workflow`, `counts` | Total lag      |
| `VReplicationQueryCountTotal`         | counter | `workflow`, `counts` | Queries        |
| `VReplicationCopyRowCountTotal`       | counter | `workflow`, `counts` | Rows copied    |
| `VReplicationCopyLoopCountTotal`      | counter | `workflow`, `counts` | Copy loops     |
| `VReplicationPhaseTimingsTotal`       | counter | `workflow`           | Phase timings  |
| `VReplicationThrottledCountTotal`     | counter | `workflow`, `counts` | Throttled      |
| `VReplicationNoopQueryCountTotal`     | counter | `workflow`, `counts` | Noop queries   |
| `VReplicationBulkQueryCountTotal`     | counter | `workflow`, `counts` | Bulk queries   |
| `VReplicationTrxQueryBatchCountTotal` | counter | `workflow`, `counts` | Trx batches    |

### VDiff

| Metric                   | Type    | Labels     | Description    |
| ------------------------ | ------- | ---------- | -------------- |
| `VDiffCount`             | gauge   | -          | Current vdiffs |
| `VDiffErrorCountTotal`   | counter | `workflow` | Errors         |
| `VDiffRowsComparedTotal` | counter | `workflow` | Rows compared  |

---

## VTOrc Metrics

### Discovery

| Metric                                   | Type      | Labels   | Description        |
| ---------------------------------------- | --------- | -------- | ------------------ |
| `DiscoveriesAttempt`                     | counter   | -        | Discovery attempts |
| `DiscoveriesFail`                        | counter   | -        | Failed discoveries |
| `DiscoveriesInstancePollSecondsExceeded` | counter   | -        | Slow polls         |
| `DiscoveriesQueueLength`                 | gauge     | -        | Queue length       |
| `DiscoveriesRecentCount`                 | gauge     | -        | Recent count       |
| `DiscoveryWorkers`                       | gauge     | -        | Worker count       |
| `DiscoveryWorkersActive`                 | gauge     | -        | Active workers     |
| `DiscoverInstanceTimings`                | histogram | `Action` | Discovery timings  |
| `DiscoveryInstanceTimings`               | histogram | `Action` | Instance timings   |

### Recovery

| Metric                 | Type    | Labels                                    | Description           |
| ---------------------- | ------- | ----------------------------------------- | --------------------- |
| `PendingRecoveries`    | gauge   | -                                         | Pending recoveries    |
| `DetectedProblems`     | gauge   | `Analysis`, `Keyspace`, `Shard`           | Problems detected     |
| `RecoveriesCount`      | counter | `Analysis`, `Keyspace`, `Shard`           | Recovery attempts     |
| `SuccessfulRecoveries` | counter | `Analysis`, `Keyspace`, `Shard`           | Successful recoveries |
| `FailedRecoveries`     | counter | `Analysis`, `Keyspace`, `Shard`           | Failed recoveries     |
| `SkippedRecoveries`    | counter | `Analysis`, `Keyspace`, `Shard`, `Reason` | Skipped recoveries    |

### Shard Locks

| Metric             | Type      | Labels   | Description                                                  |
| ------------------ | --------- | -------- | ------------------------------------------------------------ |
| `ShardLockTimings` | histogram | `Action` | Lock timings (AcquireShardLock, LockAction, UnlockShardLock) |
| `ShardLocksActive` | gauge     | -        | Active locks                                                 |

### Instance & Analysis

| Metric                 | Type    | Labels | Description      |
| ---------------------- | ------- | ------ | ---------------- |
| `InstanceRead`         | counter | -      | Instance reads   |
| `InstanceReadTopology` | counter | -      | Topology reads   |
| `AnalysisChangeWrite`  | counter | -      | Analysis changes |
| `AuditWrite`           | counter | -      | Audit writes     |

### Errant GTIDs

| Metric                   | Type  | Labels        | Description               |
| ------------------------ | ----- | ------------- | ------------------------- |
| `CurrentErrantGTIDCount` | gauge | `TabletAlias` | Errant GTIDs per tablet   |
| `ErrantGtidTabletCount`  | gauge | -             | Tablets with errant GTIDs |

### Tablet Info (Labels)

| Metric                | Type  | Labels                                           | Description     |
| --------------------- | ----- | ------------------------------------------------ | --------------- |
| `TabletAlias`         | gauge | `Keyspace`, `Shard`, `TabletAlias`, `TabletType` | Tablet alias    |
| `TabletKeyspace`      | gauge | `Keyspace`, `Shard`, `TabletAlias`, `TabletType` | Tablet keyspace |
| `TabletShard`         | gauge | `Keyspace`, `Shard`, `TabletAlias`, `TabletType` | Tablet shard    |
| `TabletType`          | gauge | `Keyspace`, `Shard`, `TabletAlias`, `TabletType` | Tablet type     |
| `TabletKeyRangeStart` | gauge | `Keyspace`, `Shard`, `TabletAlias`, `TabletType` | Key range start |
| `TabletKeyRangeEnd`   | gauge | `Keyspace`, `Shard`, `TabletAlias`, `TabletType` | Key range end   |

---

## Common/Shared Metrics

### Build Information

| Metric             | Type  | Labels                                                                                             | Description     |
| ------------------ | ----- | -------------------------------------------------------------------------------------------------- | --------------- |
| `BuildInformation` | gauge | `build_git_branch`, `build_git_rev`, `build_host`, `build_number`, `build_timestamp`, `build_user` | Build info      |
| `BuildTimestamp`   | gauge | -                                                                                                  | Build timestamp |
| `BuildNumber`      | gauge | -                                                                                                  | Build number    |

### Process

| Metric   | Type  | Labels | Description         |
| -------- | ----- | ------ | ------------------- |
| `Uptime` | gauge | -      | Process uptime (ns) |
| `MaxFds` | gauge | -      | FD limit            |

### Topology Watcher

| Metric                      | Type    | Labels | Description        |
| --------------------------- | ------- | ------ | ------------------ |
| `TopologyWatcherOperations` | counter | `type` | Watcher operations |
| `TopologyWatcherErrors`     | counter | `type` | Watcher errors     |

### Health Check

| Metric                         | Type    | Labels                                | Description         |
| ------------------------------ | ------- | ------------------------------------- | ------------------- |
| `HealthcheckErrors`            | counter | `Keyspace`, `ShardName`, `TabletType` | Healthcheck errors  |
| `HealthcheckPrimaryPromoted`   | counter | `Keyspace`, `ShardName`               | Primary promotions  |
| `HealthCheckChannelFullErrors` | counter | -                                     | Channel full errors |

### Tablet Manager Client

| Metric                                         | Type      | Labels | Description                                     |
| ---------------------------------------------- | --------- | ------ | ----------------------------------------------- |
| `tabletmanagerclient_cachedconn_reuse`         | gauge     | -      | Connection reuse count                          |
| `tabletmanagerclient_cachedconn_new`           | gauge     | -      | New connections                                 |
| `tabletmanagerclient_cachedconn_dial_timeouts` | gauge     | -      | Dial timeouts                                   |
| `tabletmanagerclient_cachedconn_dial_timings`  | histogram | `path` | Dial timings (cache_fast, sema_fast, sema_poll) |

### Backup/Restore

| Metric              | Type    | Labels                                              | Description           |
| ------------------- | ------- | --------------------------------------------------- | --------------------- |
| `BackupBytes`       | counter | `Component`, `Implementation`, `Operation`, `Scope` | Bytes backed up       |
| `BackupCount`       | counter | `Component`, `Implementation`, `Operation`, `Scope` | File count            |
| `BackupDurationNs`  | counter | `Component`, `Implementation`, `Operation`, `Scope` | Duration              |
| `BackupDurationS`   | gauge   | -                                                   | Duration (deprecated) |
| `RestoreBytes`      | counter | `Component`, `Implementation`, `Operation`, `Scope` | Bytes restored        |
| `RestoreCount`      | counter | `Component`, `Implementation`, `Operation`, `Scope` | File count            |
| `RestoreDurationNs` | counter | `Component`, `Implementation`, `Operation`, `Scope` | Duration              |
| `RestoreDurationS`  | gauge   | -                                                   | Duration (deprecated) |

### Tablet Picker

| Metric                                | Type    | Labels                                | Description            |
| ------------------------------------- | ------- | ------------------------------------- | ---------------------- |
| `TabletPickerNoTabletFoundErrorCount` | counter | `cells`, `keyspace`, `shard`, `types` | No tablet found errors |

---

## Accessing Metrics

Metrics are exposed at the `/metrics` endpoint on each component's HTTP port:

| Component | Default Port | Endpoint                      |
| --------- | ------------ | ----------------------------- |
| vtgate    | 15001        | `http://<host>:15001/metrics` |
| vttablet  | 15100+       | `http://<host>:15101/metrics` |
| vtctld    | 15000        | `http://<host>:15000/metrics` |
| vtorc     | 16000        | `http://<host>:16000/metrics` |

**Note:** The `vttestserver` binary does NOT include Prometheus metrics support. Use the full Vitess deployment for metrics.

---

## Label Value Reference

### Common Error Types

- `Fail`, `TxPoolFull`, `NotInTx`, `Deadlock`, `Retry`, `Fatal`, `OK`

### Common Plan Types

- `Select`, `Insert`, `Update`, `Delete`, `DDL`, `Set`, `Other`

### Tablet Types

- `PRIMARY`, `REPLICA`, `RDONLY`, `SPARE`, `BATCH`

### Buffer Stop Reasons

- `ReshardingComplete`, `NewPrimarySeen`, `MaxDurationExceeded`, `Shutdown`, `MoveTablesSwitchedTraffic`

### VStreamer Error Types

- `Catchup`, `Copy`, `Send`, `TablePlan`

### Recovery Analysis Types

- `DeadPrimary`, `PrimarySemiSyncMustNotBeSet`, `PrimarySemiSyncMustBeSet`, `ReplicationStopped`, etc.
