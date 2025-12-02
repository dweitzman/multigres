# Non-Locking Bootstrap Protocol Design

## Problem Statement

When bootstrapping a new shard, there's a chicken-and-egg problem:

- Hot standbys require restoring from a primary's backup
- But the durability policy (requiring standby acknowledgment) can't be enforced until standbys exist
- If the primary fails during backup creation, we need safe recovery without distributed locking

## Design Overview

Use consensus term numbers as logical timestamps for backups, ensuring monotonicity provides safety without requiring distributed locks.

### Key Insight

Term numbers already provide a total ordering in the consensus protocol. By tracking a "minimum valid backup term" (floor) in consensus state, stale/zombie backups become harmless rather than dangerous. Backups use pgbackrest's native timestamp naming; the term number in consensus state maps to the corresponding pgbackrest timestamp.

## Consensus State Changes

Extend `ConsensusTerm` proto with:

```protobuf
message ConsensusTerm {
  int64 term_number = 1;
  ID accepted_term_from_coordinator_id = 2;
  google.protobuf.Timestamp last_acceptance_time = 3;
  ID leader_id = 4;

  // NEW: Bootstrap-related field
  int64 min_bootstrap_backup_term = 5;  // Minimum valid backup term (floor)
}
```

### Invariants

1. **Backup floor is a minimum**: A multipooler must never restore from a backup with term < its observed `min_bootstrap_backup_term`.

2. **Monotonically increasing**: `min_bootstrap_backup_term` can only increase, never decrease. Once set to a non-zero value, it cannot be erased or reset to zero. This prevents "unbootstrapping" - a shard that has been bootstrapped cannot be accidentally returned to an unbootstrapped state.

## Protocol Flow

### Happy Path: Fresh Bootstrap

1. Orchestrator selects node A as primary at term 1
2. A creates backup, uploads to pgbackrest repo on S3
3. A updates consensus state: `min_bootstrap_backup_term=1`
4. Standbys see floor=1, restore from the backup
5. Once restored and first heartbeat is written (proving quorum), nodes serve reads

### Primary Failure During Backup Upload

1. Term 1: A starts uploading backup, crashes mid-upload
2. Orchestrator elects B at term 2
3. B queries quorum (union of revocation quorum + new leadership quorum), sees `min_bootstrap_backup_term=0` everywhere
4. B creates backup, uploads to pgbackrest repo on S3
5. B sets `min_bootstrap_backup_term=2`
6. If A's zombie upload completes → backup exists but floor=2, so it's ignored
7. Standbys restore from floor-2 backup

### Primary Failure After Setting Floor

1. Term 1: A uploads backup, sets `min_bootstrap_backup_term=1`
2. Standbys B, C start restoring (slow operation)
3. A crashes
4. Orchestrator elects B at term 2
5. Orchestrator gathers `MAX(min_bootstrap_backup_term)=1` from union of revocation quorum + new leadership quorum
6. B is informed floor=1
7. B restores from that backup if needed, becomes primary
8. **No new backup created** - reuses existing floor

## Component Changes

### 1. Backup Handling (pgbackrest)

Use pgbackrest's native timestamp-based naming. The term number stored in consensus state maps to a pgbackrest timestamp. When selecting a backup for restore, find the backup corresponding to the floor term's timestamp.

### 2. ConsensusState (go/multipooler/manager/consensus_state.go)

Add methods:

- `GetMinBootstrapBackupTerm(ctx) (int64, error)`
- `SetMinBootstrapBackupTerm(ctx, term int64) error`

### 3. InitializeEmptyPrimary (go/multipooler/manager/rpc_initialization.go)

Modified flow:

```
func InitializeEmptyPrimary(ctx, req):
    if req.MinBootstrapBackupTerm > 0:
        // Floor provided by orchestrator - reuse existing backup
        restoreFromBackup(ctx, req.MinBootstrapBackupTerm)
        return

    // No floor - create new backup at current term
    createBackup(ctx)
    consensusState.SetMinBootstrapBackupTerm(ctx, currentTerm)
```

### 4. InitializeAsStandby (go/multipooler/manager/rpc_initialization.go)

Modified flow:

```
func InitializeAsStandby(ctx, req):
    floor := consensusState.GetMinBootstrapBackupTerm(ctx)

    // Find backup >= floor in pgbackrest repo
    backup := findValidBackup(ctx, floor)
    restoreFromBackup(ctx, backup)

    // Query service disabled until first heartbeat proves quorum
```

### 5. BootstrapShardAction (go/multiorch/actions/bootstrap_shard.go)

Modified flow:

```
func Execute(ctx, shardID, cohort):
    // Gather floor from union of revocation quorum + new leadership quorum
    maxFloor := 0
    for _, pooler := range allQuorumMembers:
        status := getInitializationStatus(pooler)
        if status.MinBootstrapBackupTerm > maxFloor:
            maxFloor = status.MinBootstrapBackupTerm

    // Initialize primary with floor
    req := InitializeEmptyPrimaryRequest{
        ConsensusTerm: newTerm,
        MinBootstrapBackupTerm: maxFloor,  // NEW
    }
    primary.InitializeEmptyPrimary(req)
```

### 6. BeginTerm RPC (go/multipooler/manager/rpc_consensus.go)

When joining a term, a multipooler must validate its bootstrap state against the floor provided by orchestrator. If the multipooler's local state is stale, it must re-restore before participating in the term:

```
func BeginTerm(ctx, req):
    localFloor := consensusState.GetMinBootstrapBackupTerm(ctx)

    if req.MinBootstrapBackupTerm > localFloor:
        // Our local state is stale - must re-restore from newer backup
        restoreFromBackup(ctx, req.MinBootstrapBackupTerm)
        consensusState.SetMinBootstrapBackupTerm(ctx, req.MinBootstrapBackupTerm)

    // Continue with normal BeginTerm logic...
```

This ensures any multipooler joining a term is always up-to-date with the quorum's floor before participating.

### 7. Read Path (multipooler query handling)

Query service is disabled after loading a bootstrap backup until the first heartbeat is written, proving the primary has quorum:

```
func handleQuery(ctx, query):
    if !hasWrittenFirstHeartbeat():
        return error("shard is bootstrapping, waiting for quorum")
    // ... normal query handling
```

## Safety Properties

1. **No stale reads**: All nodes reject reads until first heartbeat proves quorum
2. **No data loss**: Floor ensures all nodes use same-or-newer backup
3. **No split-brain**: Term monotonicity means only one valid backup per scenario
4. **Zombie-safe**: Late-arriving backups from crashed primaries are ≤ floor, ignored

## Liveness Properties

1. **Progress guaranteed**: If primary fails, new primary can continue
2. **No locks to deadlock**: Only local state + pgbackrest repo operations
3. **Reuses work**: If valid backup exists (floor > 0), new primary reuses it

## Edge Cases

| Scenario                                                 | Behavior                                                                                                          |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Primary crashes mid-upload, zombie completes             | Backup exists at old term, but new primary sets higher floor → ignored                                            |
| Primary crashes after upload, before consensus update    | Orphaned backup, new primary creates its own. Wasted storage but correct                                          |
| Standby sees higher backup in pgbackrest repo than floor | Can use it (floor is minimum, not maximum)                                                                        |
| Network partition during bootstrap                       | Partitioned primary might upload backup, but won't be able to update consensus in quorum → new primary takes over |

### Competing/Stale Orchestrators

If an orchestrator has stale information and tries to bootstrap an already-bootstrapped shard:

1. Cluster successfully bootstrapped at term 1, accepting writes
2. Stale orchestrator tries to start new bootstrap with floor=0
3. When orchestrator attempts to revoke the existing quorum, the active nodes have `min_bootstrap_backup_term > 0`
4. Since `min_bootstrap_backup_term` is monotonically increasing, orchestrator cannot set it to a lower value
5. Orchestrator fails revocation and learns the actual floor from the quorum
6. Orchestrator discovers bootstrap already succeeded - no action needed

The monotonicity invariant ensures you cannot accidentally re-bootstrap or unbootstrap an operational shard.

## Design Decisions

### Backup timeout vs interruption

Bootstrap backups are for empty databases with no user data, so they complete quickly. Use a short context timeout (~30 seconds) rather than trying to interrupt mid-backup when a new term appears. The action lock prevents concurrent operations anyway.

### Read rejection via heartbeat

**Both primary and standbys** reject all external reads after loading a bootstrap backup until the first heartbeat is successfully written. This proves the primary has quorum, without needing a separate `bootstrap_complete` flag.

If there are multiple competing backups from different terms, orchestrator repairs this during crash recovery by propagating the MAX floor discovered from the union of revocation quorum + new leadership quorum.

### Orphaned backup cleanup

Orphaned backups (term < floor) can be left in the pgbackrest repo indefinitely. Optionally, any multipooler on startup can garbage-collect backups where:

- `backup_term < min_bootstrap_backup_term` (below floor), AND
- `backup_age > threshold` (e.g., 7 days)

This keeps the pgbackrest repo clean without requiring a dedicated cleanup process.

## Files to Modify

- `proto/multipoolermanagerdata.proto` - Add `min_bootstrap_backup_term` field to ConsensusTerm
- `go/multipooler/manager/consensus_state.go` - Bootstrap state methods
- `go/multipooler/manager/rpc_initialization.go` - Modified init flows
- `go/multipooler/manager/rpc_consensus.go` - BeginTerm floor validation and re-restore
- `go/multiorch/actions/bootstrap_shard.go` - Floor gathering and propagation
- Query path files (TBD) - Read rejection until first heartbeat
