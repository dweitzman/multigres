/* eslint-disable */
// @ts-nocheck
/*
* This file is a generated Typescript file for GRPC Gateway, DO NOT MODIFY
*/

import * as GoogleProtobufTimestamp from "./google/protobuf/timestamp.pb"

export enum PoolerType {
  UNKNOWN = "UNKNOWN",
  PRIMARY = "PRIMARY",
  REPLICA = "REPLICA",
  DRAINED = "DRAINED",
}

export enum PoolerServingStatus {
  SERVING = "SERVING",
  NOT_SERVING = "NOT_SERVING",
  BACKUP = "BACKUP",
  RESTORE = "RESTORE",
  SERVING_RDONLY = "SERVING_RDONLY",
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
  UNKNOWN = "UNKNOWN",
  MULTIPOOLER = "MULTIPOOLER",
  MULTIGATEWAY = "MULTIGATEWAY",
  MULTIORCH = "MULTIORCH",
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