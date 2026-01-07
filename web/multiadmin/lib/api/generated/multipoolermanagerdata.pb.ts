/* eslint-disable */
// @ts-nocheck
/*
* This file is a generated Typescript file for GRPC Gateway, DO NOT MODIFY
*/

import * as ClustermetadataClustermetadata from "./clustermetadata.pb"
import * as GoogleProtobufDuration from "./google/protobuf/duration.pb"
import * as GoogleProtobufTimestamp from "./google/protobuf/timestamp.pb"

export enum ReplicationPauseMode {
  REPLICATION_PAUSE_MODE_REPLAY_ONLY = "REPLICATION_PAUSE_MODE_REPLAY_ONLY",
  REPLICATION_PAUSE_MODE_RECEIVER_ONLY = "REPLICATION_PAUSE_MODE_RECEIVER_ONLY",
  REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER = "REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER",
}

export enum SynchronousMethod {
  SYNCHRONOUS_METHOD_UNSPECIFIED = "SYNCHRONOUS_METHOD_UNSPECIFIED",
  SYNCHRONOUS_METHOD_FIRST = "SYNCHRONOUS_METHOD_FIRST",
  SYNCHRONOUS_METHOD_ANY = "SYNCHRONOUS_METHOD_ANY",
}

export enum StandbyUpdateOperation {
  STANDBY_UPDATE_OPERATION_UNSPECIFIED = "STANDBY_UPDATE_OPERATION_UNSPECIFIED",
  STANDBY_UPDATE_OPERATION_ADD = "STANDBY_UPDATE_OPERATION_ADD",
  STANDBY_UPDATE_OPERATION_REMOVE = "STANDBY_UPDATE_OPERATION_REMOVE",
  STANDBY_UPDATE_OPERATION_REPLACE = "STANDBY_UPDATE_OPERATION_REPLACE",
}

export enum SynchronousCommitLevel {
  SYNCHRONOUS_COMMIT_LEVEL_OFF = "SYNCHRONOUS_COMMIT_LEVEL_OFF",
  SYNCHRONOUS_COMMIT_LEVEL_LOCAL = "SYNCHRONOUS_COMMIT_LEVEL_LOCAL",
  SYNCHRONOUS_COMMIT_LEVEL_REMOTE_WRITE = "SYNCHRONOUS_COMMIT_LEVEL_REMOTE_WRITE",
  SYNCHRONOUS_COMMIT_LEVEL_ON = "SYNCHRONOUS_COMMIT_LEVEL_ON",
  SYNCHRONOUS_COMMIT_LEVEL_REMOTE_APPLY = "SYNCHRONOUS_COMMIT_LEVEL_REMOTE_APPLY",
}

export enum BackupMetadataStatus {
  STATUS_UNKNOWN = "STATUS_UNKNOWN",
  STATUS_INCOMPLETE = "STATUS_INCOMPLETE",
  STATUS_COMPLETE = "STATUS_COMPLETE",
}

export type PrimaryConnInfo = {
  host?: string
  port?: number
  user?: string
  applicationName?: string
  raw?: string
}

export type StandbyReplicationStatus = {
  lastReplayLsn?: string
  lastReceiveLsn?: string
  isWalReplayPaused?: boolean
  walReplayPauseState?: string
  lag?: GoogleProtobufDuration.Duration
  lastXactReplayTimestamp?: string
  primaryConnInfo?: PrimaryConnInfo
}

export type WaitForLSNRequest = {
  targetLsn?: string
  timeout?: GoogleProtobufDuration.Duration
}

export type WaitForLSNResponse = {
}

export type StartReplicationRequest = {
}

export type StartReplicationResponse = {
}

export type SetPrimaryConnInfoRequest = {
  primary?: ClustermetadataClustermetadata.MultiPooler
  stopReplicationBefore?: boolean
  startReplicationAfter?: boolean
  currentTerm?: string
  force?: boolean
}

export type SetPrimaryConnInfoResponse = {
}

export type StopReplicationRequest = {
  mode?: ReplicationPauseMode
  wait?: boolean
}

export type StopReplicationResponse = {
  status?: StandbyReplicationStatus
}

export type StandbyReplicationStatusRequest = {
}

export type StandbyReplicationStatusResponse = {
  status?: StandbyReplicationStatus
}

export type SynchronousReplicationConfiguration = {
  synchronousCommit?: SynchronousCommitLevel
  synchronousMethod?: SynchronousMethod
  numSync?: number
  standbyIds?: ClustermetadataClustermetadata.ID[]
}

export type PrimaryStatus = {
  lsn?: string
  ready?: boolean
  connectedFollowers?: ClustermetadataClustermetadata.ID[]
  syncReplicationConfig?: SynchronousReplicationConfiguration
}

export type PrimaryStatusRequest = {
}

export type PrimaryStatusResponse = {
  status?: PrimaryStatus
}

export type PrimaryPositionRequest = {
}

export type PrimaryPositionResponse = {
  lsnPosition?: string
}

export type Status = {
  poolerType?: ClustermetadataClustermetadata.PoolerType
  primaryStatus?: PrimaryStatus
  replicationStatus?: StandbyReplicationStatus
  isInitialized?: boolean
  hasDataDirectory?: boolean
  postgresRunning?: boolean
  postgresRole?: string
  walPosition?: string
  consensusTerm?: string
  shardId?: string
}

export type StatusRequest = {
}

export type StatusResponse = {
  status?: Status
}

export type ReplicationStats = {
  pid?: number
  clientAddr?: string
  state?: string
  syncState?: string
  sentLsn?: string
  writeLsn?: string
  flushLsn?: string
  replayLsn?: string
  writeLag?: GoogleProtobufDuration.Duration
  flushLag?: GoogleProtobufDuration.Duration
  replayLag?: GoogleProtobufDuration.Duration
}

export type FollowerInfo = {
  followerId?: ClustermetadataClustermetadata.ID
  applicationName?: string
  isConnected?: boolean
  replicationStats?: ReplicationStats
}

export type GetFollowersRequest = {
}

export type GetFollowersResponse = {
  followers?: FollowerInfo[]
  syncConfig?: SynchronousReplicationConfiguration
}

export type DemoteRequest = {
  consensusTerm?: string
  drainTimeout?: GoogleProtobufDuration.Duration
  force?: boolean
}

export type DemoteResponse = {
  wasAlreadyDemoted?: boolean
  consensusTerm?: string
  lsnPosition?: string
  connectionsTerminated?: number
}

export type UndoDemoteRequest = {
}

export type UndoDemoteResponse = {
}

export type StopReplicationAndGetStatusRequest = {
  mode?: ReplicationPauseMode
  wait?: boolean
}

export type StopReplicationAndGetStatusResponse = {
  status?: StandbyReplicationStatus
}

export type ChangeTypeRequest = {
  poolerType?: ClustermetadataClustermetadata.PoolerType
}

export type ChangeTypeResponse = {
}

export type PromoteRequest = {
  consensusTerm?: string
  expectedLsn?: string
  syncReplicationConfig?: ConfigureSynchronousReplicationRequest
  force?: boolean
  reason?: string
  coordinatorId?: string
  cohortMembers?: string[]
  acceptedMembers?: string[]
}

export type PromoteResponse = {
  lsnPosition?: string
  wasAlreadyPrimary?: boolean
  consensusTerm?: string
}

export type ResetReplicationRequest = {
}

export type ResetReplicationResponse = {
  status?: StandbyReplicationStatus
}

export type ConfigureSynchronousReplicationRequest = {
  synchronousCommit?: SynchronousCommitLevel
  synchronousMethod?: SynchronousMethod
  numSync?: number
  standbyIds?: ClustermetadataClustermetadata.ID[]
  reloadConfig?: boolean
}

export type ConfigureSynchronousReplicationResponse = {
}

export type StateRequest = {
}

export type StateResponse = {
  state?: string
  errorMessage?: string
}

export type SetTermRequest = {
  term?: ConsensusTerm
}

export type SetTermResponse = {
}

export type UpdateSynchronousStandbyListRequest = {
  operation?: StandbyUpdateOperation
  standbyIds?: ClustermetadataClustermetadata.ID[]
  reloadConfig?: boolean
  consensusTerm?: string
  force?: boolean
}

export type UpdateSynchronousStandbyListResponse = {
}

export type ConsensusTerm = {
  termNumber?: string
  acceptedTermFromCoordinatorId?: ClustermetadataClustermetadata.ID
  lastAcceptanceTime?: GoogleProtobufTimestamp.Timestamp
  leaderId?: ClustermetadataClustermetadata.ID
}

export type InitializeEmptyPrimaryRequest = {
  consensusTerm?: string
  durabilityPolicyName?: string
  durabilityQuorumRule?: ClustermetadataClustermetadata.QuorumRule
  coordinatorId?: string
}

export type InitializeEmptyPrimaryResponse = {
  success?: boolean
  errorMessage?: string
  backupId?: string
}

export type InitializeAsStandbyRequest = {
  primary?: ClustermetadataClustermetadata.MultiPooler
  consensusTerm?: string
  force?: boolean
  backupId?: string
}

export type InitializeAsStandbyResponse = {
  success?: boolean
  errorMessage?: string
  finalLsn?: string
}

export type BackupRequest = {
  forcePrimary?: boolean
  type?: string
  jobId?: string
}

export type BackupResponse = {
  backupId?: string
}

export type RestoreFromBackupRequest = {
  backupId?: string
}

export type RestoreFromBackupResponse = {
}

export type GetBackupsRequest = {
  limit?: number
}

export type GetBackupsResponse = {
  backups?: BackupMetadata[]
}

export type GetBackupByJobIdRequest = {
  jobId?: string
}

export type GetBackupByJobIdResponse = {
  backup?: BackupMetadata
}

export type BackupMetadata = {
  tableGroup?: string
  shard?: string
  status?: BackupMetadataStatus
  backupId?: string
  finalLsn?: string
  jobId?: string
  backupSizeBytes?: string
  type?: string
  multipoolerId?: string
  poolerType?: ClustermetadataClustermetadata.PoolerType
}

export type GetDurabilityPolicyRequest = {
}

export type GetDurabilityPolicyResponse = {
  policy?: ClustermetadataClustermetadata.DurabilityPolicy
}

export type CreateDurabilityPolicyRequest = {
  policyName?: string
  quorumRule?: ClustermetadataClustermetadata.QuorumRule
}

export type CreateDurabilityPolicyResponse = {
}

export type RewindToSourceRequest = {
  source?: ClustermetadataClustermetadata.MultiPooler
  dryRun?: boolean
}

export type RewindToSourceResponse = {
  success?: boolean
  errorMessage?: string
}