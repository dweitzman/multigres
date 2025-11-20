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

package provisionercfg

// CellConfig holds the configuration for a single cell
type CellConfig struct {
	Name     string `yaml:"name"`
	RootPath string `yaml:"root-path"`
}

// TopologyConfig holds the configuration for cluster topology
type TopologyConfig struct {
	Backend        string       `yaml:"backend"`
	GlobalRootPath string       `yaml:"global-root-path"`
	Cells          []CellConfig `yaml:"cells"`
}

// CellServicesConfig holds the service configuration for a specific cell
type CellServicesConfig struct {
	Multigateway MultigatewayConfig `yaml:"multigateway"`
	Multipooler  MultipoolerConfig  `yaml:"multipooler"`
	Multiorch    MultiorchConfig    `yaml:"multiorch"`
	Pgctld       PgctldConfig       `yaml:"pgctld"`
}

// LocalProvisionerConfig represents the typed configuration for the local provisioner
type LocalProvisionerConfig struct {
	RootWorkingDir string                        `yaml:"root-working-dir"`
	DefaultDbName  string                        `yaml:"default-db-name"`
	BackupRepoPath string                        `yaml:"backup-repo-path,omitempty"`
	Etcd           EtcdConfig                    `yaml:"etcd"`
	Topology       TopologyConfig                `yaml:"topology"`
	Multiadmin     MultiadminConfig              `yaml:"multiadmin"`
	Cells          map[string]CellServicesConfig `yaml:"cells,omitempty"`
}

// EtcdConfig holds etcd service configuration
type EtcdConfig struct {
	Version string `yaml:"version"`
	DataDir string `yaml:"data-dir"`
	Port    int    `yaml:"port"`
}

// MultigatewayConfig holds multigateway service configuration
type MultigatewayConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	PgPort   int    `yaml:"pg-port"`
	LogLevel string `yaml:"log-level"`
}

// MultipoolerConfig holds multipooler service configuration
type MultipoolerConfig struct {
	Path           string `yaml:"path"`
	Database       string `yaml:"database"`
	TableGroup     string `yaml:"table-group"`
	ServiceID      string `yaml:"service-id"`
	PoolerDir      string `yaml:"pooler-dir"`  // Directory path for PostgreSQL socket files
	PgPort         int    `yaml:"pg-port"`     // PostgreSQL port number (same as pgctld)
	BackupConf     string `yaml:"backup-conf"` // Path to backup configuration file (pgbackrest.conf)
	HttpPort       int    `yaml:"http-port"`
	GrpcPort       int    `yaml:"grpc-port"`
	GRPCSocketFile string `yaml:"grpc-socket-file"` // Unix socket file path for gRPC
	LogLevel       string `yaml:"log-level"`
}

// MultiorchConfig holds multiorch service configuration
type MultiorchConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	LogLevel string `yaml:"log-level"`
}

// MultiadminConfig holds multiadmin service configuration
type MultiadminConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	LogLevel string `yaml:"log-level"`
}

// PgctldConfig holds pgctld service configuration
type PgctldConfig struct {
	Path           string `yaml:"path"`
	PoolerDir      string `yaml:"pooler-dir"`       // Base directory for this pgctld instance
	GrpcPort       int    `yaml:"grpc-port"`        // gRPC port for pgctld server
	GRPCSocketFile string `yaml:"grpc-socket-file"` // Unix socket file path for gRPC
	PgPort         int    `yaml:"pg-port"`          // PostgreSQL port
	PgDatabase     string `yaml:"pg-database"`      // PostgreSQL database name
	PgUser         string `yaml:"pg-user"`          // PostgreSQL username
	PgPwfile       string `yaml:"pg-pwfile"`        // PostgreSQL password file path (optional)
	Timeout        int    `yaml:"timeout"`          // Operation timeout in seconds
	LogLevel       string `yaml:"log_level"`        // Log level
}
