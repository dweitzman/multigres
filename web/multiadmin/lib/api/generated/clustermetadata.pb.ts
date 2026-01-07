/* eslint-disable */
// @ts-nocheck
/*
* This file is a generated Typescript file for GRPC Gateway, DO NOT MODIFY
*/

import * as GoogleProtobufTimestamp from "./google/protobuf/timestamp.pb"

export enum PoolerType {
  POOLER_TYPE_UNKNOWN = "POOLER_TYPE_UNKNOWN",
  POOLER_TYPE_PRIMARY = "POOLER_TYPE_PRIMARY",
  POOLER_TYPE_REPLICA = "POOLER_TYPE_REPLICA",
  POOLER_TYPE_DRAINED = "POOLER_TYPE_DRAINED",
}

export enum PoolerServingStatus {
  POOLER_SERVING_STATUS_SERVING = "POOLER_SERVING_STATUS_SERVING",
  POOLER_SERVING_STATUS_NOT_SERVING = "POOLER_SERVING_STATUS_NOT_SERVING",
  POOLER_SERVING_STATUS_BACKUP = "POOLER_SERVING_STATUS_BACKUP",
  POOLER_SERVING_STATUS_RESTORE = "POOLER_SERVING_STATUS_RESTORE",
  POOLER_SERVING_STATUS_SERVING_RDONLY = "POOLER_SERVING_STATUS_SERVING_RDONLY",
}

export enum QuorumType {
  QUORUM_TYPE_UNKNOWN = "QUORUM_TYPE_UNKNOWN",
  QUORUM_TYPE_ANY_N = "QUORUM_TYPE_ANY_N",
  QUORUM_TYPE_MULTI_CELL_ANY_N = "QUORUM_TYPE_MULTI_CELL_ANY_N",
}

export enum AsyncReplicationFallbackMode {
  ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN = "ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN",
  ASYNC_REPLICATION_FALLBACK_MODE_ALLOW = "ASYNC_REPLICATION_FALLBACK_MODE_ALLOW",
  ASYNC_REPLICATION_FALLBACK_MODE_REJECT = "ASYNC_REPLICATION_FALLBACK_MODE_REJECT",
}

export enum IDComponentType {
  COMPONENT_TYPE_UNKNOWN = "COMPONENT_TYPE_UNKNOWN",
  COMPONENT_TYPE_MULTIPOOLER = "COMPONENT_TYPE_MULTIPOOLER",
  COMPONENT_TYPE_MULTIGATEWAY = "COMPONENT_TYPE_MULTIGATEWAY",
  COMPONENT_TYPE_MULTIORCH = "COMPONENT_TYPE_MULTIORCH",
}

export type GlobalTopoConfig = {
  implementation?: string
  serverAddresses?: string[]
  root?: string
}

export type Cell = {
  name?: string
  serverAddresses?: string[]
  root?: string
}

export type Database = {
  name?: string
  backupLocation?: string
  durabilityPolicy?: string
  cells?: string[]
}

export type MultiPooler = {
  id?: ID
  database?: string
  tableGroup?: string
  shard?: string
  keyRange?: KeyRange
  type?: PoolerType
  servingStatus?: PoolerServingStatus
  hostname?: string
  portMap?: {[key: string]: number}
  poolerDir?: string
}

export type MultiGateway = {
  id?: ID
  hostname?: string
  portMap?: {[key: string]: number}
}

export type MultiOrch = {
  id?: ID
  hostname?: string
  portMap?: {[key: string]: number}
}

export type ID = {
  component?: IDComponentType
  cell?: string
  name?: string
}

export type KeyRange = {
  start?: Uint8Array
  end?: Uint8Array
}

export type DurabilityPolicy = {
  policyName?: string
  policyVersion?: string
  quorumRule?: QuorumRule
  isActive?: boolean
  createdAt?: GoogleProtobufTimestamp.Timestamp
  updatedAt?: GoogleProtobufTimestamp.Timestamp
}

export type QuorumRule = {
  quorumType?: QuorumType
  requiredCount?: number
  description?: string
  asyncFallback?: AsyncReplicationFallbackMode
}