# Vitess Tracing Spans Reference

This document lists all tracing spans created in Vitess, organized by component.

## Tracing Backends

Vitess supports the following tracing backends via the `--tracer` flag:

| Backend | Flag Value            | Description                           |
| ------- | --------------------- | ------------------------------------- |
| Jaeger  | `opentracing-jaeger`  | OpenTracing-compatible Jaeger tracing |
| Datadog | `opentracing-datadog` | Datadog APM tracing                   |
| Noop    | `noop`                | No-op (default, no tracing)           |

### Jaeger Configuration

| Flag/Env Variable                                  | Description                                                    |
| -------------------------------------------------- | -------------------------------------------------------------- |
| `--jaeger-agent-host` / `JAEGER_AGENT_HOST`        | Host and port to send spans                                    |
| `--tracing-sampling-type` / `JAEGER_SAMPLER_TYPE`  | Sampling strategy (const, probabilistic, rateLimiting, remote) |
| `--tracing-sampling-rate` / `JAEGER_SAMPLER_PARAM` | Sampling rate (default: 0.1)                                   |
| `--tracing-enable-logging`                         | Enable span logging                                            |

---

## Span Annotations

Spans can have annotations (tags) added via `span.Annotate(key, value)`. Common annotations include:

| Annotation Key       | Description                               |
| -------------------- | ----------------------------------------- |
| `sql-statement-type` | SQL statement type (SELECT, INSERT, etc.) |
| `keyspace`           | Target keyspace                           |
| `shard`              | Target shard                              |
| `tablet_alias`       | Tablet alias string                       |
| `tablet_type`        | Tablet type (PRIMARY, REPLICA, etc.)      |
| `workflow`           | Workflow name                             |
| `cluster_id`         | VTAdmin cluster ID                        |
| `cell`               | Cell name                                 |

---

## VTGate Spans

### Query Execution

| Span Name                | Location                                                               | Annotations                    |
| ------------------------ | ---------------------------------------------------------------------- | ------------------------------ |
| `executor.Execute`       | [executor.go:252](go/vt/vtgate/executor.go#L252)                       | `method`, `sql-statement-type` |
| `executor.StreamExecute` | [executor.go:315](go/vt/vtgate/executor.go#L315)                       | `method`, `sql-statement-type` |
| `VTGate.Execute`         | [plugin_mysql_server.go:209](go/vt/vtgate/plugin_mysql_server.go#L209) | `sql-statement-type`           |

---

## VTTablet Spans

### TabletServer

| Span Name                          | Location                                                                  | Annotations                                                                           |
| ---------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `TabletServer.Execute`             | [tabletserver.go:907](go/vt/vttablet/tabletserver/tabletserver.go#L907)   | `sql-statement-type`                                                                  |
| `TabletServer.ExecuteWithSettings` | [tabletserver.go:1520](go/vt/vttablet/tabletserver/tabletserver.go#L1520) | `sql-statement-type`                                                                  |
| `TabletServer.<requestName>`       | [tabletserver.go:1573](go/vt/vttablet/tabletserver/tabletserver.go#L1573) | `isolation-level`, `workload_name`, `sql-statement-type`, `cell`, `shard`, `keyspace` |

### Query Engine

| Span Name                    | Location                                                                | Annotations |
| ---------------------------- | ----------------------------------------------------------------------- | ----------- |
| `QueryEngine.GetPlan`        | [query_engine.go:398](go/vt/vttablet/tabletserver/query_engine.go#L398) | -           |
| `QueryEngine.GetStreamPlan`  | [query_engine.go:445](go/vt/vttablet/tabletserver/query_engine.go#L445) | -           |
| `QueryEngine.GetConnSetting` | [query_engine.go:494](go/vt/vttablet/tabletserver/query_engine.go#L494) | -           |

### Query Executor

| Span Name                        | Location                                                                      | Annotations          |
| -------------------------------- | ----------------------------------------------------------------------------- | -------------------- |
| `QueryExecutor.getConn`          | [query_executor.go:834](go/vt/vttablet/tabletserver/query_executor.go#L834)   | -                    |
| `QueryExecutor.getStreamConn`    | [query_executor.go:844](go/vt/vttablet/tabletserver/query_executor.go#L844)   | -                    |
| `QueryExecutor.execDBConn`       | [query_executor.go:1170](go/vt/vttablet/tabletserver/query_executor.go#L1170) | -                    |
| `QueryExecutor.execStatefulConn` | [query_executor.go:1198](go/vt/vttablet/tabletserver/query_executor.go#L1198) | -                    |
| `QueryExecutor.execStreamSQL`    | [query_executor.go:1267](go/vt/vttablet/tabletserver/query_executor.go#L1267) | `sql-statement-type` |

### Transactions

| Span Name               | Location                                                          | Annotations |
| ----------------------- | ----------------------------------------------------------------- | ----------- |
| `TxEngine.Begin`        | [tx_engine.go:273](go/vt/vttablet/tabletserver/tx_engine.go#L273) | -           |
| `TxEngine.Commit`       | [tx_engine.go:297](go/vt/vttablet/tabletserver/tx_engine.go#L297) | -           |
| `TxEngine.Rollback`     | [tx_engine.go:311](go/vt/vttablet/tabletserver/tx_engine.go#L311) | -           |
| `TxEngine.ReserveBegin` | [tx_engine.go:626](go/vt/vttablet/tabletserver/tx_engine.go#L626) | -           |
| `TxEngine.Reserve`      | [tx_engine.go:652](go/vt/vttablet/tabletserver/tx_engine.go#L652) | -           |
| `TxPool.Begin`          | [tx_pool.go:233](go/vt/vttablet/tabletserver/tx_pool.go#L233)     | -           |
| `TxPool.Commit`         | [tx_pool.go:186](go/vt/vttablet/tabletserver/tx_pool.go#L186)     | -           |
| `TxPool.Rollback`       | [tx_pool.go:211](go/vt/vttablet/tabletserver/tx_pool.go#L211)     | -           |

### Connection Pool

| Span Name       | Location                                                                      | Annotations                                 |
| --------------- | ----------------------------------------------------------------------------- | ------------------------------------------- |
| `Pool.Get`      | [connpool/pool.go:120](go/vt/vttablet/tabletserver/connpool/pool.go#L120)     | `capacity`, `in_use`, `available`, `active` |
| `DBConn.Exec`   | [connpool/dbconn.go:124](go/vt/vttablet/tabletserver/connpool/dbconn.go#L124) | -                                           |
| `DBConn.Stream` | [connpool/dbconn.go:249](go/vt/vttablet/tabletserver/connpool/dbconn.go#L249) | `sql-statement-type`                        |

### Tablet Manager

| Span Name                 | Location                                                         | Annotations |
| ------------------------- | ---------------------------------------------------------------- | ----------- |
| `tmState.refreshFromTopo` | [tm_state.go:128](go/vt/vttablet/tabletmanager/tm_state.go#L128) | -           |
| `tmState.update`          | [tm_state.go:298](go/vt/vttablet/tabletmanager/tm_state.go#L298) | -           |

---

## VTCtld Spans

### Cell Management

| Span Name                       | Annotations                                |
| ------------------------------- | ------------------------------------------ |
| `VtctldServer.AddCellInfo`      | `cell`, `cell_root`, `cell_address`        |
| `VtctldServer.AddCellsAlias`    | `cells_alias`, `cells`                     |
| `VtctldServer.DeleteCellInfo`   | `cell`, `force`                            |
| `VtctldServer.DeleteCellsAlias` | `cells_alias`                              |
| `VtctldServer.GetCellInfoNames` | -                                          |
| `VtctldServer.GetCellInfo`      | `cell`                                     |
| `VtctldServer.GetCellsAliases`  | -                                          |
| `VtctldServer.UpdateCellInfo`   | `cell`, `cell_server_address`, `cell_root` |
| `VtctldServer.UpdateCellsAlias` | `cells_alias`, `cells_alias_cells`         |

### Keyspace Management

| Span Name                                  | Annotations                                                                                                        |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `VtctldServer.CreateKeyspace`              | `keyspace`, `keyspace_type`, `force`, `allow_empty_vschema`, `durability_policy`, `base_keyspace`, `snapshot_time` |
| `VtctldServer.DeleteKeyspace`              | `keyspace`, `recursive`, `force`                                                                                   |
| `VtctldServer.GetKeyspace`                 | `keyspace`                                                                                                         |
| `VtctldServer.GetKeyspaces`                | -                                                                                                                  |
| `VtctldServer.RebuildKeyspaceGraph`        | `keyspace`, `cells`, `allow_partial`                                                                               |
| `VtctldServer.RemoveKeyspaceCell`          | `keyspace`, `cell`, `force`, `recursive`                                                                           |
| `VtctldServer.SetKeyspaceDurabilityPolicy` | `keyspace`, `durability_policy`                                                                                    |
| `VtctldServer.ValidateKeyspace`            | `keyspace`, `ping_tablets`                                                                                         |
| `VtctldServer.ValidateSchemaKeyspace`      | `keyspace`, `shards`                                                                                               |
| `VtctldServer.ValidateVersionKeyspace`     | -                                                                                                                  |

### Shard Management

| Span Name                               | Annotations                                                                                     |
| --------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `VtctldServer.CreateShard`              | `keyspace`, `shard`, `force`, `include_parent`                                                  |
| `VtctldServer.DeleteShards`             | `num_shards`, `even_if_serving`, `recursive`, `force`                                           |
| `VtctldServer.GetShard`                 | `keyspace`, `shard`                                                                             |
| `VtctldServer.deleteShard`              | `keyspace`, `shard`, `recursive`, `even_if_serving`, `force`                                    |
| `VtctldServer.deleteShardCell`          | `keyspace`, `shard`, `cell`, `recursive`                                                        |
| `VtctldServer.removeShardCell`          | `keyspace`, `shard`, `cell`, `recursive`, `force`                                               |
| `VtctldServer.RemoveShardCell`          | `keyspace`, `shard`, `cell`, `force`, `recursive`                                               |
| `VtctldServer.SetShardIsPrimaryServing` | `keyspace`, `shard`, `is_serving`                                                               |
| `VtctldServer.SetShardTabletControl`    | `keyspace`, `shard`, `tablet_type`, `cells`, `denied_tables`, `disable_query_service`, `remove` |
| `VtctldServer.ValidateShard`            | `keyspace`, `shard`, `ping_tablets`                                                             |
| `VtctldServer.ValidateVersionShard`     | -                                                                                               |

### Tablet Management

| Span Name                       | Annotations                                                             |
| ------------------------------- | ----------------------------------------------------------------------- |
| `VtctldServer.ChangeTabletTags` | `tablet_alias`, `replace`, `before_tablet_tags`, `after_tablet_tags`    |
| `VtctldServer.ChangeTabletType` | `tablet_alias`, `dry_run`, `tablet_type`, `before_tablet_type`          |
| `VtctldServer.DeleteTablets`    | `num_tablets`, `allow_primary`                                          |
| `VtctldServer.deleteTablet`     | `tablet_alias`, `allow_primary`, `is_primary`                           |
| `VtctldServer.GetFullStatus`    | `tablet_alias`                                                          |
| `VtctldServer.GetPermissions`   | `tablet_alias`                                                          |
| `VtctldServer.GetTablet`        | `tablet_alias`                                                          |
| `VtctldServer.GetTablets`       | `cells`, `keyspace`, `tablet_type`, `strict`, `tablet_aliases`, `shard` |
| `VtctldServer.PingTablet`       | `tablet_alias`                                                          |
| `VtctldServer.RefreshState`     | `tablet_alias`                                                          |
| `VtctldServer.ReloadSchema`     | `tablet_alias`                                                          |
| `VtctldServer.RunHealthCheck`   | `tablet_alias`                                                          |
| `VtctldServer.SetWritable`      | `tablet_alias`, `writable`                                              |
| `VtctldServer.SleepTablet`      | `tablet_alias`, `sleep_duration`                                        |

### Replication

| Span Name                                 | Annotations                                                                                                                                      |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `VtctldServer.EmergencyReparentShard`     | `keyspace`, `shard`, `new_primary_alias`, `ignore_replicas`, `wait_replicas_timeout_sec`, `prevent_cross_cell_promotion`, `wait_for_all_tablets` |
| `VtctldServer.InitShardPrimary`           | `keyspace`, `shard`, `wait_replicas_timeout_sec`, `force`                                                                                        |
| `VtctldServer.PlannedReparentShard`       | `keyspace`, `shard`, `wait_replicas_timeout_sec`, `avoid_primary_alias`, `expected_primary_alias`, `new_primary_alias`                           |
| `VtctldServer.ReparentTablet`             | `tablet_alias`                                                                                                                                   |
| `VtctldServer.ShardReplicationAdd`        | `tablet_alias`, `keyspace`, `shard`                                                                                                              |
| `VtctldServer.ShardReplicationFix`        | `keyspace`, `shard`, `cell`, `problem_tablet`, `problem_type`                                                                                    |
| `VtctldServer.ShardReplicationPositions`  | `keyspace`, `shard`                                                                                                                              |
| `VtctldServer.ShardReplicationRemove`     | `tablet_alias`, `keyspace`, `shard`                                                                                                              |
| `VtctldServer.StartReplication`           | `tablet_alias`                                                                                                                                   |
| `VtctldServer.StopReplication`            | `tablet_alias`                                                                                                                                   |
| `VtctldServer.TabletExternallyReparented` | `tablet_alias`                                                                                                                                   |
| `VtctldServer.getPrimaryPosition`         | `tablet_alias`                                                                                                                                   |
| `VtctldServer.getReplicationStatus`       | `tablet_alias`                                                                                                                                   |

### Schema

| Span Name                                  | Annotations                                                                                                              |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `VtctldServer.ApplySchema`                 | `keyspace`, `ddl-strategy`, `caller_id`                                                                                  |
| `VtctldServer.CancelSchemaMigration`       | `keyspace`, `uuid`                                                                                                       |
| `VtctldServer.CleanupSchemaMigration`      | `keyspace`, `uuid`                                                                                                       |
| `VtctldServer.CompleteSchemaMigration`     | `keyspace`, `uuid`                                                                                                       |
| `VtctldServer.ForceCutOverSchemaMigration` | `keyspace`, `uuid`                                                                                                       |
| `VtctldServer.GetSchema`                   | `tablet_alias`, `tables`, `exclude_tables`, `include_views`, `table_names_only`, `table_sizes_only`, `table_schema_only` |
| `VtctldServer.GetSchemaMigrations`         | `keyspace`, `uuid`, `migration_context`, `migration_status`, `recent`, `skip_limit`                                      |
| `VtctldServer.LaunchSchemaMigration`       | `keyspace`, `uuid`                                                                                                       |
| `VtctldServer.ReloadSchemaShard`           | `keyspace`, `shard`, `concurrency`, `include_primary`, `wait_position`, `is_partial_result`                              |
| `VtctldServer.ReloadSchemaKeyspace`        | `keyspace`, `concurrency`, `include_primary`, `wait_position`                                                            |
| `VtctldServer.RetrySchemaMigration`        | `keyspace`, `uuid`                                                                                                       |
| `VtctldServer.ValidatePermissionsKeyspace` | `keyspace`, `shards`                                                                                                     |

### VSchema

| Span Name                          | Annotations                                                |
| ---------------------------------- | ---------------------------------------------------------- |
| `VtctldServer.ApplyVSchema`        | `keyspace`, `cells`, `skip_rebuild`, `dry_run`, `sql_mode` |
| `VtctldServer.GetVSchema`          | `keyspace`                                                 |
| `VtctldServer.RebuildVSchemaGraph` | `cells`                                                    |

### Routing Rules

| Span Name                                | Annotations                     |
| ---------------------------------------- | ------------------------------- |
| `VtctldServer.ApplyRoutingRules`         | `skip_rebuild`, `rebuild_cells` |
| `VtctldServer.ApplyShardRoutingRules`    | `skip_rebuild`, `rebuild_cells` |
| `VtctldServer.ApplyKeyspaceRoutingRules` | `skip_rebuild`, `rebuild_cells` |
| `VtctldServer.GetRoutingRules`           | -                               |
| `VtctldServer.GetShardRoutingRules`      | -                               |
| `VtctldServer.GetKeyspaceRoutingRules`   | -                               |
| `VtctldServer.GetMirrorRules`            | -                               |

### Backup/Restore

| Span Name                        | Annotations                                                                                                                           |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `VtctldServer.Backup`            | `tablet_alias`, `allow_primary`, `concurrency`, `incremental_from_pos`, `backup_engine`, `keyspace`, `shard`                          |
| `VtctldServer.BackupShard`       | `keyspace`, `shard`, `allow_primary`, `concurrency`, `incremental_from_pos`, `upgrade_safe`, `mysql_shutdown_timeout`, `tablet_alias` |
| `VtctldServer.GetBackups`        | `keyspace`, `shard`, `limit`, `detailed`, `detailed_limit`, `backup_path`                                                             |
| `VtctldServer.RemoveBackup`      | `keyspace`, `shard`, `bucket`, `backup_name`                                                                                          |
| `VtctldServer.RestoreFromBackup` | `tablet_alias`, `backup_timestamp`, `keyspace`, `shard`                                                                               |

### Workflows

| Span Name                            | Annotations                                                                                      |
| ------------------------------------ | ------------------------------------------------------------------------------------------------ |
| `VtctldServer.GetWorkflows`          | `keyspace`, `active_only`, `include_logs`                                                        |
| `VtctldServer.WorkflowDelete`        | `keyspace`, `workflow`, `keep_data`, `keep_routing_rules`, `shards`                              |
| `VtctldServer.WorkflowStatus`        | `keyspace`, `workflow`                                                                           |
| `VtctldServer.WorkflowSwitchTraffic` | `keyspace`, `workflow`, `tablet-types`, `direction`, `enable-reverse-replication`, `force`       |
| `VtctldServer.WorkflowUpdate`        | `keyspace`, `workflow`, `cells`, `tablet_types`, `on_ddl`, `state`, `config_overrides`, `shards` |
| `VtctldServer.WorkflowMirrorTraffic` | `keyspace`, `workflow`, `percent`                                                                |

### MoveTables

| Span Name                            | Annotations                                                                |
| ------------------------------------ | -------------------------------------------------------------------------- |
| `VtctldServer.MoveTablesCreate`      | `keyspace`, `workflow`, `cells`, `tablet_types`, `on_ddl`                  |
| `VtctldServer.MoveTablesComplete`    | `keyspace`, `workflow`, `keep_data`, `keep_routing_rules`, `dry_run`       |
| `workflow.Server.moveTablesCreate`   | `keyspace`, `workflow`, `workflow_type`, `cells`, `tablet_types`, `on_ddl` |
| `workflow.Server.MoveTablesComplete` | -                                                                          |

### Reshard

| Span Name                       | Annotations                                                                                 |
| ------------------------------- | ------------------------------------------------------------------------------------------- |
| `VtctldServer.ReshardCreate`    | `keyspace`, `workflow`, `cells`, `source_shards`, `target_shards`, `tablet_types`, `on_ddl` |
| `workflow.Server.ReshardCreate` | `keyspace`, `workflow`, `source_shards`, `target_shards`, `cells`, `tablet_types`, `on_ddl` |

### VDiff

| Span Name                     | Annotations                                                                                                                               |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `VtctldServer.VDiffCreate`    | `keyspace`, `workflow`, `uuid`, `source_cells`, `target_cells`, `tablet_types`, `tables`, `auto_start`, `auto_retry`, `max_diff_duration` |
| `VtctldServer.VDiffDelete`    | `keyspace`, `workflow`, `argument`                                                                                                        |
| `VtctldServer.VDiffResume`    | `keyspace`, `workflow`, `uuid`, `shards`                                                                                                  |
| `VtctldServer.VDiffShow`      | `keyspace`, `workflow`, `argument`                                                                                                        |
| `VtctldServer.VDiffStop`      | `keyspace`, `workflow`, `uuid`, `shards`                                                                                                  |
| `workflow.Server.VDiffCreate` | `keyspace`, `workflow`, `uuid`, `source_cells`, `target_cells`, `tablet_types`, `tables`, `auto_retry`, `max_diff_duration`, `auto_start` |
| `workflow.Server.VDiffDelete` | `keyspace`, `workflow`, `argument`                                                                                                        |
| `workflow.Server.VDiffResume` | `keyspace`, `workflow`, `uuid`, `target_shards`                                                                                           |
| `workflow.Server.VDiffShow`   | `keyspace`, `workflow`, `argument`                                                                                                        |
| `workflow.Server.VDiffStop`   | `keyspace`, `workflow`, `uuid`, `target_shards`                                                                                           |

### Materialize

| Span Name                        | Annotations                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------- |
| `VtctldServer.MaterializeCreate` | `workflow`, `source_keyspace`, `target_keyspace`, `cells`, `tablet_types`, `table_settings` |

### Lookup Vindex

| Span Name                                 | Annotations                                                                       |
| ----------------------------------------- | --------------------------------------------------------------------------------- |
| `VtctldServer.LookupVindexComplete`       | `name`, `keyspace`, `table_keyspace`                                              |
| `VtctldServer.LookupVindexCreate`         | `workflow`, `keyspace`, `continue_after_copy_with_owner`, `cells`, `tablet_types` |
| `VtctldServer.LookupVindexExternalize`    | `name`, `keyspace`, `table_keyspace`                                              |
| `VtctldServer.LookupVindexInternalize`    | `name`, `keyspace`, `table_keyspace`                                              |
| `workflow.Server.LookupVindexComplete`    | `keyspace`, `name`, `table_keyspace`                                              |
| `workflow.Server.LookupVindexCreate`      | `workflow`, `keyspace`, `continue_after_copy_with_owner`, `cells`, `tablet_types` |
| `workflow.Server.LookupVindexExternalize` | `keyspace`, `name`, `table_keyspace`, `delete_workflow`                           |
| `workflow.Server.LookupVindexInternalize` | `keyspace`, `name`, `table_keyspace`                                              |

### Mount

| Span Name                      | Annotations                                           |
| ------------------------------ | ----------------------------------------------------- |
| `VtctldServer.MountRegister`   | `topo_type`, `topo_server`, `topo_root`, `mount_name` |
| `VtctldServer.MountUnregister` | `mount_name`                                          |
| `VtctldServer.MountList`       | -                                                     |
| `VtctldServer.MountShow`       | `mount_name`                                          |

### Migrate

| Span Name                        | Annotations                                                      |
| -------------------------------- | ---------------------------------------------------------------- |
| `VtctldServer.MigrateCreate`     | `target_keyspace`, `workflow`, `cells`, `tablet_types`, `on_ddl` |
| `VtctldServer.WorkflowAddTables` | `workflow`, `keyspace`, `table_settings`                         |

### Throttler

| Span Name                            | Annotations                |
| ------------------------------------ | -------------------------- |
| `VtctldServer.CheckThrottler`        | `tablet_alias`, `app_name` |
| `VtctldServer.GetThrottlerStatus`    | `tablet_alias`             |
| `VtctldServer.UpdateThrottlerConfig` | -                          |

### Transactions

| Span Name                                | Annotations            |
| ---------------------------------------- | ---------------------- |
| `VtctldServer.GetUnresolvedTransactions` | `keyspace`             |
| `VtctldServer.ConcludeTransaction`       | `dtid`, `participants` |
| `VtctldServer.GetTransactionInfo`        | `dtid`                 |

### Topology

| Span Name                          | Annotations                  |
| ---------------------------------- | ---------------------------- |
| `VtctldServer.GetTopology`         | `version`                    |
| `VtctldServer.GetSrvKeyspaceNames` | -                            |
| `VtctldServer.GetSrvKeyspaces`     | `cells`                      |
| `VtctldServer.GetSrvVSchema`       | `cell`                       |
| `VtctldServer.GetSrvVSchemas`      | `cells`                      |
| `VtctldServer.DeleteSrvVSchema`    | `cell`                       |
| `VtctldServer.GetShardReplication` | `keyspace`, `shard`, `cells` |

### Validation

| Span Name                         | Annotations    |
| --------------------------------- | -------------- |
| `VtctldServer.Validate`           | `ping_tablets` |
| `VtctldServer.ValidateVSchema`    | -              |
| `VtctldServer.validateAllTablets` | -              |
| `VtctldServer.validateTablet`     | `tablet_alias` |

### SQL Execution

| Span Name                             | Annotations                                                    |
| ------------------------------------- | -------------------------------------------------------------- |
| `VtctldServer.ExecuteFetchAsApp`      | `tablet_alias`, `max_rows`, `use_pool`                         |
| `VtctldServer.ExecuteFetchAsDBA`      | `tablet_alias`, `max_rows`, `disable_binlogs`, `reload_schema` |
| `VtctldServer.ExecuteMultiFetchAsDBA` | `tablet_alias`, `max_rows`, `disable_binlogs`, `reload_schema` |
| `VtctldServer.ExecuteHook`            | `tablet_alias`, `hook_name`                                    |

### Other

| Span Name                                | Annotations                                                                         |
| ---------------------------------------- | ----------------------------------------------------------------------------------- |
| `VtctldServer.GetVersion`                | -                                                                                   |
| `VtctldServer.FindAllShardsInKeyspace`   | `keyspace`                                                                          |
| `VtctldServer.SourceShardAdd`            | `keyspace`, `shard`, `uid`, `source_keyspace`, `source_shard`, `keyrange`, `tables` |
| `VtctldServer.SourceShardDelete`         | `keyspace`, `shard`, `uid`                                                          |
| `VtctldServer.SetVtorcEmergencyReparent` | `keyspace`, `shard`, `disable`                                                      |
| `VtctldServer.CopySchemaShard`           | `source_tablet_alias`, `destination_keyspace`, `destination_shard`                  |

---

## Topo Server Spans

| Span Name                                 | Location                                           | Annotations                      |
| ----------------------------------------- | -------------------------------------------------- | -------------------------------- |
| `TopoServer.Lock`                         | [locks.go:170](go/vt/topo/locks.go#L170)           | `action`, `path`                 |
| `TopoServer.Unlock`                       | [locks.go:208](go/vt/topo/locks.go#L208)           | `action`, `path`                 |
| `TopoServer.GetTablet`                    | [tablet.go:180](go/vt/topo/tablet.go#L180)         | `tablet`                         |
| `TopoServer.UpdateTablet`                 | [tablet.go:340](go/vt/topo/tablet.go#L340)         | `tablet`                         |
| `TopoServer.UpdateTabletFields`           | [tablet.go:369](go/vt/topo/tablet.go#L369)         | `tablet`                         |
| `topo.GetTabletMap`                       | [tablet.go:491](go/vt/topo/tablet.go#L491)         | `num_tablets`                    |
| `topo.GetTabletList`                      | [tablet.go:541](go/vt/topo/tablet.go#L541)         | `num_tablets`                    |
| `TopoServer.GetShard`                     | [shard.go:178](go/vt/topo/shard.go#L178)           | `keyspace`, `shard`              |
| `TopoServer.UpdateShard`                  | [shard.go:201](go/vt/topo/shard.go#L201)           | `keyspace`, `shard`              |
| `topo.FindAllTabletAliasesInShardbyCell`  | [shard.go:564](go/vt/topo/shard.go#L564)           | `keyspace`, `shard`, `num_cells` |
| `topo.GetTabletsByShardCell`              | [shard.go:642](go/vt/topo/shard.go#L642)           | `keyspace`, `shard`, `num_cells` |
| `TopoServer.UpdateShardReplicationFields` | [replication.go:81](go/vt/topo/replication.go#L81) | `keyspace`, `shard`, `tablet`    |

---

## Workflow Server Spans

| Span Name                                        | Annotations                                                                                          |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `workflow.Server.GetWorkflows`                   | `keyspace`, `workflow`, `active_only`, `include_logs`, `shards`                                      |
| `workflow.Server.WorkflowDelete`                 | `keyspace`, `workflow`, `keep_data`, `keep_routing_rules`, `shards`                                  |
| `workflow.Server.WorkflowUpdate`                 | `keyspace`, `workflow`, `cells`, `tablet_types`, `on_ddl`, `state`, `config_overrides`, `shards`     |
| `workflow.Server.WorkflowSwitchTraffic`          | `keyspace`, `workflow`, `tablet-types`, `direction`, `enable-reverse-replication`, `shards`, `force` |
| `workflowFetcher.workflow.fetchCopyStates`       | `shard`, `tablet_alias`                                                                              |
| `workflowFetcher.workflow.getWorkflowCopyStates` | `keyspace`, `shard`, `tablet_alias`, `stream_ids`                                                    |
| `workflowFetcher.workflow.fetchStreamLogs`       | `keyspace`, `workflow`                                                                               |

---

## VTAdmin Spans

### API Endpoints

| Span Name                            | Annotations                                |
| ------------------------------------ | ------------------------------------------ |
| `API.ApplySchema`                    | `cluster_id`                               |
| `API.CancelSchemaMigration`          | `cluster_id`                               |
| `API.CleanupSchemaMigration`         | `cluster_id`                               |
| `API.CompleteSchemaMigration`        | `cluster_id`                               |
| `API.ConcludeTransaction`            | -                                          |
| `API.CreateKeyspace`                 | `cluster_id`                               |
| `API.CreateShard`                    | `cluster_id`                               |
| `API.DeleteKeyspace`                 | `cluster_id`                               |
| `API.DeleteShards`                   | `cluster_id`                               |
| `API.DeleteTablet`                   | -                                          |
| `API.EmergencyFailoverShard`         | -                                          |
| `API.FindSchema`                     | `table`                                    |
| `API.GetBackups`                     | -                                          |
| `API.GetCellInfos`                   | -                                          |
| `API.GetCellsAliases`                | -                                          |
| `API.GetClusters`                    | -                                          |
| `API.GetFullStatus`                  | -                                          |
| `API.GetGates`                       | -                                          |
| `API.GetKeyspace`                    | -                                          |
| `API.GetKeyspaces`                   | -                                          |
| `API.GetSchema`                      | `cluster_id`, `keyspace`, `table`          |
| `API.GetSchemas`                     | -                                          |
| `API.GetSchemaMigrations`            | -                                          |
| `API.GetShardReplicationPositions`   | -                                          |
| `API.GetSrvKeyspace`                 | `cluster_id`, `cell`                       |
| `API.GetSrvKeyspaces`                | -                                          |
| `API.GetSrvVSchema`                  | -                                          |
| `API.GetSrvVSchemas`                 | -                                          |
| `API.GetTablet`                      | -                                          |
| `API.GetTablets`                     | -                                          |
| `API.GetTopologyPath`                | -                                          |
| `API.GetTransactionInfo`             | -                                          |
| `API.GetUnresolvedTransactions`      | -                                          |
| `API.GetVSchema`                     | -                                          |
| `API.GetVSchemas`                    | -                                          |
| `API.GetVtctlds`                     | -                                          |
| `API.GetWorkflow`                    | `keyspace`, `workflow_name`, `active_only` |
| `API.GetWorkflowStatus`              | `keyspace`, `workflow_name`                |
| `API.GetWorkflows`                   | -                                          |
| `API.LaunchSchemaMigration`          | `cluster_id`                               |
| `API.MaterializeCreate`              | `cluster_id`                               |
| `API.MoveTablesComplete`             | `cluster_id`                               |
| `API.MoveTablesCreate`               | `cluster_id`                               |
| `API.PingTablet`                     | -                                          |
| `API.PlannedFailoverShard`           | -                                          |
| `API.RebuildKeyspaceGraph`           | -                                          |
| `API.RefreshState`                   | -                                          |
| `API.RefreshTabletReplicationSource` | -                                          |
| `API.ReloadSchemas`                  | -                                          |
| `API.RemoveKeyspaceCell`             | -                                          |
| `API.ReshardCreate`                  | `cluster_id`                               |
| `API.RetrySchemaMigration`           | `cluster_id`                               |
| `API.RunHealthCheck`                 | -                                          |
| `API.SetReadOnly`                    | -                                          |
| `API.SetReadWrite`                   | -                                          |
| `API.StartReplication`               | -                                          |
| `API.StartWorkflow`                  | `keyspace`, `workflow_name`                |
| `API.StopReplication`                | -                                          |
| `API.StopWorkflow`                   | `keyspace`, `workflow_name`                |
| `API.TabletExternallyPromoted`       | -                                          |
| `API.VDiffCreate`                    | -                                          |
| `API.VDiffShow`                      | -                                          |
| `API.VExplain`                       | `keyspace`                                 |
| `API.VTExplain`                      | -                                          |
| `API.Validate`                       | -                                          |
| `API.ValidateKeyspace`               | -                                          |
| `API.ValidateSchemaKeyspace`         | -                                          |
| `API.ValidateShard`                  | -                                          |
| `API.ValidateVersionKeyspace`        | -                                          |
| `API.ValidateVersionShard`           | -                                          |
| `API.WorkflowDelete`                 | `cluster_id`                               |
| `API.WorkflowSwitchTraffic`          | `cluster_id`                               |

### HTTP Handler

| Span Name                   | Location                                                         | Annotations                        |
| --------------------------- | ---------------------------------------------------------------- | ---------------------------------- |
| `vtadmin:http:<route_name>` | [handlers/trace.go:53](go/vt/vtadmin/http/handlers/trace.go#L53) | `route_uri`, `route_path_template` |

### Cluster Operations

All cluster spans include `cluster_id` annotation. Key spans:

| Span Name                         | Additional Annotations                     |
| --------------------------------- | ------------------------------------------ |
| `Cluster.ApplySchema`             | `keyspace`, `sql`, `ddl_strategy`, etc.    |
| `Cluster.CreateKeyspace`          | `keyspace`                                 |
| `Cluster.DeleteKeyspace`          | `keyspace`                                 |
| `Cluster.EmergencyFailoverShard`  | `keyspace`, `shard`, `new_primary`, etc.   |
| `Cluster.FindAllShardsInKeyspace` | `keyspace`                                 |
| `Cluster.FindTablet`              | -                                          |
| `Cluster.FindTablets`             | `max_result_length`                        |
| `Cluster.FindWorkflows`           | `active_only`                              |
| `Cluster.GetBackups`              | `keyspace`, `shard`                        |
| `Cluster.GetKeyspace`             | `keyspace`                                 |
| `Cluster.GetKeyspaces`            | -                                          |
| `Cluster.GetSchema`               | `keyspace`, `is_backfill`, `cache_hit`     |
| `Cluster.GetSchemas`              | `is_backfill`, `cache_hit`                 |
| `Cluster.GetSchemaMigrations`     | `keyspace`, `uuid`, etc.                   |
| `Cluster.GetTablets`              | -                                          |
| `Cluster.GetVSchema`              | `keyspace`                                 |
| `Cluster.GetWorkflow`             | `active_only`, `keyspace`, `workflow_name` |
| `Cluster.GetWorkflows`            | `active_only`                              |
| `Cluster.PlannedFailoverShard`    | `keyspace`, `shard`, `new_primary`, etc.   |
| `Cluster.RefreshState`            | `tablet_alias`                             |
| `Cluster.ReloadSchemas`           | -                                          |

### Discovery

| Span Name                             | Annotations |
| ------------------------------------- | ----------- |
| `ConsulDiscovery.DiscoverVTGate`      | -           |
| `ConsulDiscovery.DiscoverVTGateAddr`  | -           |
| `ConsulDiscovery.DiscoverVTGateAddrs` | -           |
| `ConsulDiscovery.DiscoverVTGates`     | -           |
| `ConsulDiscovery.DiscoverVtctld`      | -           |
| `ConsulDiscovery.DiscoverVtctldAddr`  | -           |
| `ConsulDiscovery.DiscoverVtctldAddrs` | -           |
| `ConsulDiscovery.DiscoverVtctlds`     | -           |
| `JSONDiscovery.DiscoverVTGate`        | -           |
| `JSONDiscovery.DiscoverVTGateAddr`    | -           |
| `JSONDiscovery.DiscoverVTGateAddrs`   | -           |
| `JSONDiscovery.DiscoverVTGates`       | -           |
| `JSONDiscovery.DiscoverVtctld`        | -           |
| `JSONDiscovery.DiscoverVtctldAddr`    | -           |
| `JSONDiscovery.DiscoverVtctldAddrs`   | -           |
| `JSONDiscovery.DiscoverVtctlds`       | -           |

### Proxies

| Span Name                 | Annotations            |
| ------------------------- | ---------------------- |
| `VTGateProxy.Dial`        | `is_using_credentials` |
| `VTGateProxy.ShowTablets` | -                      |
| `VTGateProxy.VExplain`    | -                      |
| `VTGateProxy.PingContext` | -                      |
| `VtctldClientProxy.Dial`  | `is_using_credentials` |

### Resolver

| Span Name                            | Annotations                                           |
| ------------------------------------ | ----------------------------------------------------- |
| `(vtadmin/cluster/resolver).resolve` | `cluster_id`, `component`, `addrs`, `balancer_policy` |

---

## Span Count Summary

| Component       | Approximate Span Count |
| --------------- | ---------------------- |
| VTGate          | ~5                     |
| VTTablet        | ~20                    |
| VTCtld          | ~150                   |
| Topo Server     | ~12                    |
| Workflow Server | ~15                    |
| VTAdmin         | ~100+                  |
| **Total**       | **~300+**              |
