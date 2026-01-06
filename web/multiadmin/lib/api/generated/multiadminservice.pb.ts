/* eslint-disable */
// @ts-nocheck
/*
* This file is a generated Typescript file for GRPC Gateway, DO NOT MODIFY
*/

import * as ClustermetadataClustermetadata from "./clustermetadata.pb"
import * as fm from "./fetch.pb"
import * as GoogleProtobufTimestamp from "./google/protobuf/timestamp.pb"
import * as MultipoolermanagerdataMultipoolermanagerdata from "./multipoolermanagerdata.pb"

export enum BackupType {
  BACKUP_TYPE_UNKNOWN = "BACKUP_TYPE_UNKNOWN",
  BACKUP_TYPE_FULL = "BACKUP_TYPE_FULL",
  BACKUP_TYPE_DIFFERENTIAL = "BACKUP_TYPE_DIFFERENTIAL",
  BACKUP_TYPE_INCREMENTAL = "BACKUP_TYPE_INCREMENTAL",
}

export enum JobType {
  JOB_TYPE_UNKNOWN = "JOB_TYPE_UNKNOWN",
  JOB_TYPE_BACKUP = "JOB_TYPE_BACKUP",
  JOB_TYPE_RESTORE = "JOB_TYPE_RESTORE",
}

export enum JobStatus {
  JOB_STATUS_UNKNOWN = "JOB_STATUS_UNKNOWN",
  JOB_STATUS_PENDING = "JOB_STATUS_PENDING",
  JOB_STATUS_RUNNING = "JOB_STATUS_RUNNING",
  JOB_STATUS_COMPLETED = "JOB_STATUS_COMPLETED",
  JOB_STATUS_FAILED = "JOB_STATUS_FAILED",
}

export enum BackupStatus {
  BACKUP_STATUS_UNKNOWN = "BACKUP_STATUS_UNKNOWN",
  BACKUP_STATUS_INCOMPLETE = "BACKUP_STATUS_INCOMPLETE",
  BACKUP_STATUS_COMPLETE = "BACKUP_STATUS_COMPLETE",
  BACKUP_STATUS_FAILED = "BACKUP_STATUS_FAILED",
}

export type GetCellRequest = {
  name?: string
}

export type GetCellResponse = {
  cell?: ClustermetadataClustermetadata.Cell
}

export type GetDatabaseRequest = {
  name?: string
}

export type GetDatabaseResponse = {
  database?: ClustermetadataClustermetadata.Database
}

export type GetCellNamesRequest = {
}

export type GetCellNamesResponse = {
  names?: string[]
}

export type GetDatabaseNamesRequest = {
}

export type GetDatabaseNamesResponse = {
  names?: string[]
}

export type GetGatewaysRequest = {
  cells?: string[]
}

export type GetGatewaysResponse = {
  gateways?: ClustermetadataClustermetadata.MultiGateway[]
}

export type GetPoolersRequest = {
  cells?: string[]
  database?: string
  shard?: string
}

export type GetPoolersResponse = {
  poolers?: ClustermetadataClustermetadata.MultiPooler[]
}

export type GetOrchsRequest = {
  cells?: string[]
}

export type GetOrchsResponse = {
  orchs?: ClustermetadataClustermetadata.MultiOrch[]
}

export type BackupRequest = {
  database?: string
  tableGroup?: string
  shard?: string
  type?: BackupType
  forcePrimary?: boolean
}

export type BackupResponse = {
  jobId?: string
}

export type RestoreFromBackupRequest = {
  database?: string
  tableGroup?: string
  shard?: string
  backupId?: string
  poolerId?: ClustermetadataClustermetadata.ID
}

export type RestoreFromBackupResponse = {
  jobId?: string
}

export type GetBackupJobStatusRequest = {
  jobId?: string
  database?: string
  tableGroup?: string
  shard?: string
}

export type GetBackupJobStatusResponse = {
  jobId?: string
  jobType?: JobType
  status?: JobStatus
  errorMessage?: string
  database?: string
  tableGroup?: string
  shard?: string
  backupType?: BackupType
  requestedBackupId?: string
  backupId?: string
}

export type GetBackupsRequest = {
  database?: string
  tableGroup?: string
  shard?: string
  limit?: number
}

export type GetBackupsResponse = {
  backups?: BackupInfo[]
}

export type BackupInfo = {
  backupId?: string
  database?: string
  tableGroup?: string
  shard?: string
  type?: BackupType
  status?: BackupStatus
  backupTime?: GoogleProtobufTimestamp.Timestamp
  backupSizeBytes?: string
  multipoolerServiceId?: string
  poolerType?: ClustermetadataClustermetadata.PoolerType
}

export type GetPoolerStatusRequest = {
  poolerId?: ClustermetadataClustermetadata.ID
}

export type GetPoolerStatusResponse = {
  status?: MultipoolermanagerdataMultipoolermanagerdata.Status
}

export class MultiAdminService {
  static GetCell(req: GetCellRequest, initReq?: fm.InitReq): Promise<GetCellResponse> {
    return fm.fetchReq<GetCellRequest, GetCellResponse>(`/api/v1/cells/${req["name"]}?${fm.renderURLSearchParams(req, ["name"])}`, {...initReq, method: "GET"})
  }
  static GetDatabase(req: GetDatabaseRequest, initReq?: fm.InitReq): Promise<GetDatabaseResponse> {
    return fm.fetchReq<GetDatabaseRequest, GetDatabaseResponse>(`/api/v1/databases/${req["name"]}?${fm.renderURLSearchParams(req, ["name"])}`, {...initReq, method: "GET"})
  }
  static GetCellNames(req: GetCellNamesRequest, initReq?: fm.InitReq): Promise<GetCellNamesResponse> {
    return fm.fetchReq<GetCellNamesRequest, GetCellNamesResponse>(`/api/v1/cells?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetDatabaseNames(req: GetDatabaseNamesRequest, initReq?: fm.InitReq): Promise<GetDatabaseNamesResponse> {
    return fm.fetchReq<GetDatabaseNamesRequest, GetDatabaseNamesResponse>(`/api/v1/databases?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetGateways(req: GetGatewaysRequest, initReq?: fm.InitReq): Promise<GetGatewaysResponse> {
    return fm.fetchReq<GetGatewaysRequest, GetGatewaysResponse>(`/api/v1/gateways?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetPoolers(req: GetPoolersRequest, initReq?: fm.InitReq): Promise<GetPoolersResponse> {
    return fm.fetchReq<GetPoolersRequest, GetPoolersResponse>(`/api/v1/poolers?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetOrchs(req: GetOrchsRequest, initReq?: fm.InitReq): Promise<GetOrchsResponse> {
    return fm.fetchReq<GetOrchsRequest, GetOrchsResponse>(`/api/v1/orchs?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static Backup(req: BackupRequest, initReq?: fm.InitReq): Promise<BackupResponse> {
    return fm.fetchReq<BackupRequest, BackupResponse>(`/api/v1/backups`, {...initReq, method: "POST", body: JSON.stringify(req, fm.replacer)})
  }
  static RestoreFromBackup(req: RestoreFromBackupRequest, initReq?: fm.InitReq): Promise<RestoreFromBackupResponse> {
    return fm.fetchReq<RestoreFromBackupRequest, RestoreFromBackupResponse>(`/api/v1/restores`, {...initReq, method: "POST", body: JSON.stringify(req, fm.replacer)})
  }
  static GetBackupJobStatus(req: GetBackupJobStatusRequest, initReq?: fm.InitReq): Promise<GetBackupJobStatusResponse> {
    return fm.fetchReq<GetBackupJobStatusRequest, GetBackupJobStatusResponse>(`/api/v1/jobs/${req["jobId"]}?${fm.renderURLSearchParams(req, ["jobId"])}`, {...initReq, method: "GET"})
  }
  static GetBackups(req: GetBackupsRequest, initReq?: fm.InitReq): Promise<GetBackupsResponse> {
    return fm.fetchReq<GetBackupsRequest, GetBackupsResponse>(`/api/v1/backups?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetPoolerStatus(req: GetPoolerStatusRequest, initReq?: fm.InitReq): Promise<GetPoolerStatusResponse> {
    return fm.fetchReq<GetPoolerStatusRequest, GetPoolerStatusResponse>(`/multiadmin.MultiAdminService/GetPoolerStatus`, {...initReq, method: "POST", body: JSON.stringify(req, fm.replacer)})
  }
}