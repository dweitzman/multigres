// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Wrapper around generated MultiAdminService to provide:
 * 1. Configured base URL for all requests
 * 2. Better error handling with ApiError class
 * 3. Instance-based API (instead of static methods)
 */

import { MultiAdminService } from "./generated/multiadminservice.pb";
import type * as types from "./generated/multiadminservice.pb";

export class ApiError extends Error {
  constructor(
    public status: number,
    public body: string,
    public url: string
  ) {
    super(`API error ${status}: ${body}`);
    this.name = "ApiError";
  }
}

export interface ApiClientConfig {
  baseUrl: string;
}

export class MultiAdminClient {
  private pathPrefix: string;

  constructor(config: ApiClientConfig) {
    // Remove trailing slash if present
    this.pathPrefix = config.baseUrl.replace(/\/$/, "");
  }

  // Cell operations

  async getCell(req: types.GetCellRequest): Promise<types.GetCellResponse> {
    return MultiAdminService.GetCell(req, { pathPrefix: this.pathPrefix });
  }

  async getCellNames(req: types.GetCellNamesRequest = {}): Promise<types.GetCellNamesResponse> {
    return MultiAdminService.GetCellNames(req, { pathPrefix: this.pathPrefix });
  }

  // Database operations

  async getDatabase(req: types.GetDatabaseRequest): Promise<types.GetDatabaseResponse> {
    return MultiAdminService.GetDatabase(req, { pathPrefix: this.pathPrefix });
  }

  async getDatabaseNames(req: types.GetDatabaseNamesRequest = {}): Promise<types.GetDatabaseNamesResponse> {
    return MultiAdminService.GetDatabaseNames(req, { pathPrefix: this.pathPrefix });
  }

  // Gateway operations

  async getGateways(req: types.GetGatewaysRequest = {}): Promise<types.GetGatewaysResponse> {
    return MultiAdminService.GetGateways(req, { pathPrefix: this.pathPrefix });
  }

  // Pooler operations

  async getPoolers(req: types.GetPoolersRequest = {}): Promise<types.GetPoolersResponse> {
    return MultiAdminService.GetPoolers(req, { pathPrefix: this.pathPrefix });
  }

  // Orchestrator operations

  async getOrchs(req: types.GetOrchsRequest = {}): Promise<types.GetOrchsResponse> {
    return MultiAdminService.GetOrchs(req, { pathPrefix: this.pathPrefix });
  }

  // Backup operations

  async backup(req: types.BackupRequest): Promise<types.BackupResponse> {
    return MultiAdminService.Backup(req, { pathPrefix: this.pathPrefix });
  }

  async restoreFromBackup(req: types.RestoreFromBackupRequest): Promise<types.RestoreFromBackupResponse> {
    return MultiAdminService.RestoreFromBackup(req, { pathPrefix: this.pathPrefix });
  }

  async getBackupJobStatus(req: types.GetBackupJobStatusRequest): Promise<types.GetBackupJobStatusResponse> {
    return MultiAdminService.GetBackupJobStatus(req, { pathPrefix: this.pathPrefix });
  }

  async getBackups(req: types.GetBackupsRequest = {}): Promise<types.GetBackupsResponse> {
    return MultiAdminService.GetBackups(req, { pathPrefix: this.pathPrefix });
  }

  async getPoolerStatus(req: types.GetPoolerStatusRequest): Promise<types.GetPoolerStatusResponse> {
    return MultiAdminService.GetPoolerStatus(req, { pathPrefix: this.pathPrefix });
  }
}
