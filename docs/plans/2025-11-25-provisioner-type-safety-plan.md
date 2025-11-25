# Provisioner Type Safety Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace string-based port/config maps with typed structs to catch missing fields and typos at compile time.

**Architecture:** Add a `Port` type with Number/Protocol/Name fields. Config structs get `Ports()` methods. Remove `getServiceConfig()`/`getCellServiceConfig()` and access config structs directly. Update `ProvisionResult` and `LocalProvisionedService` to use `[]Port`.

**Tech Stack:** Go, standard library only (no new dependencies)

**Design Doc:** [2025-11-25-provisioner-type-safety-design.md](2025-11-25-provisioner-type-safety-design.md)

---

## Phase 1: Add Port Type and Helpers

### Task 1: Add Port Type to provisioner.go

**Files:**

- Modify: `go/provisioner/provisioner.go:49-60`

**Step 1: Add Port type definition after imports, before ProvisionResult**

Add this code after line 47 (after imports, before `ProvisionResult`):

```go
// Port represents a network port with its protocol and identifier
type Port struct {
	// Number is the port number
	Number int `json:"number"`
	// Protocol is the network protocol: "http", "grpc", "tcp", "postgres"
	Protocol string `json:"protocol"`
	// Name is a human-readable identifier: "client", "peer", "http", "grpc", etc.
	Name string `json:"name"`
}
```

**Step 2: Build to verify no syntax errors**

Run: `go build ./go/provisioner/...`
Expected: Success, no errors

**Step 3: Commit**

```bash
git add go/provisioner/provisioner.go
git commit -m "feat(provisioner): add Port type for typed port representation"
```

---

### Task 2: Add Port Helper Functions

**Files:**

- Modify: `go/provisioner/provisioner.go`

**Step 1: Add helper functions after the Port type**

Add after the Port struct definition:

```go
// FindPort finds a port by name in a slice of ports.
// Returns the port and true if found, zero Port and false otherwise.
func FindPort(ports []Port, name string) (Port, bool) {
	for _, p := range ports {
		if p.Name == name {
			return p, true
		}
	}
	return Port{}, false
}

// FindPortByProtocol finds the first port with a given protocol.
// Returns the port and true if found, zero Port and false otherwise.
func FindPortByProtocol(ports []Port, protocol string) (Port, bool) {
	for _, p := range ports {
		if p.Protocol == protocol {
			return p, true
		}
	}
	return Port{}, false
}

// PortsToMap converts a slice of ports to a map[string]int keyed by port name.
// This is useful for backwards compatibility with code expecting the old format.
func PortsToMap(ports []Port) map[string]int {
	m := make(map[string]int, len(ports))
	for _, p := range ports {
		m[p.Name] = p.Number
	}
	return m
}
```

**Step 2: Build to verify no syntax errors**

Run: `go build ./go/provisioner/...`
Expected: Success, no errors

**Step 3: Commit**

```bash
git add go/provisioner/provisioner.go
git commit -m "feat(provisioner): add Port helper functions"
```

---

### Task 3: Add Tests for Port Helpers

**Files:**

- Create: `go/provisioner/port_test.go`

**Step 1: Create test file**

```go
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

package provisioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindPort(t *testing.T) {
	ports := []Port{
		{Number: 8080, Protocol: "http", Name: "http"},
		{Number: 9090, Protocol: "grpc", Name: "grpc"},
	}

	t.Run("finds existing port", func(t *testing.T) {
		port, found := FindPort(ports, "http")
		assert.True(t, found)
		assert.Equal(t, 8080, port.Number)
		assert.Equal(t, "http", port.Protocol)
	})

	t.Run("returns false for missing port", func(t *testing.T) {
		port, found := FindPort(ports, "missing")
		assert.False(t, found)
		assert.Equal(t, Port{}, port)
	})
}

func TestFindPortByProtocol(t *testing.T) {
	ports := []Port{
		{Number: 8080, Protocol: "http", Name: "http"},
		{Number: 9090, Protocol: "grpc", Name: "grpc"},
	}

	t.Run("finds existing protocol", func(t *testing.T) {
		port, found := FindPortByProtocol(ports, "grpc")
		assert.True(t, found)
		assert.Equal(t, 9090, port.Number)
	})

	t.Run("returns false for missing protocol", func(t *testing.T) {
		port, found := FindPortByProtocol(ports, "tcp")
		assert.False(t, found)
		assert.Equal(t, Port{}, port)
	})
}

func TestPortsToMap(t *testing.T) {
	ports := []Port{
		{Number: 8080, Protocol: "http", Name: "http_port"},
		{Number: 9090, Protocol: "grpc", Name: "grpc_port"},
	}

	m := PortsToMap(ports)

	assert.Equal(t, 8080, m["http_port"])
	assert.Equal(t, 9090, m["grpc_port"])
	assert.Len(t, m, 2)
}
```

**Step 2: Run tests**

Run: `go test ./go/provisioner/... -v -run "TestFind|TestPortsToMap"`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add go/provisioner/port_test.go
git commit -m "test(provisioner): add tests for Port helper functions"
```

---

## Phase 2: Add Ports() Methods to Config Structs

### Task 4: Add Ports() Method to EtcdConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Add import for provisioner package if not present**

Check imports at top of file. Add if missing:

```go
"github.com/multigres/multigres/go/provisioner"
```

**Step 2: Add Ports() method after EtcdConfig struct (around line 69)**

```go
// Ports returns the typed port configuration for etcd
func (c EtcdConfig) Ports() []provisioner.Port {
	ports := []provisioner.Port{
		{Number: c.Port, Protocol: "http", Name: "client"},
	}
	// Include peer port if configured (defaults to Port+1 if not set)
	peerPort := c.PeerPort
	if peerPort == 0 {
		peerPort = c.Port + 1
	}
	ports = append(ports, provisioner.Port{Number: peerPort, Protocol: "http", Name: "peer"})
	return ports
}
```

**Step 3: Build to verify**

Run: `go build ./go/provisioner/local/...`
Expected: Success

**Step 4: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "feat(provisioner): add Ports() method to EtcdConfig"
```

---

### Task 5: Add Ports() Method to MultigatewayConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Add Ports() method after MultigatewayConfig struct (around line 78)**

```go
// Ports returns the typed port configuration for multigateway
func (c MultigatewayConfig) Ports() []provisioner.Port {
	return []provisioner.Port{
		{Number: c.HttpPort, Protocol: "http", Name: "http"},
		{Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
		{Number: c.PgPort, Protocol: "postgres", Name: "pg"},
	}
}
```

**Step 2: Build to verify**

Run: `go build ./go/provisioner/local/...`
Expected: Success

**Step 3: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "feat(provisioner): add Ports() method to MultigatewayConfig"
```

---

### Task 6: Add Ports() Method to MultipoolerConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Add Ports() method after MultipoolerConfig struct (around line 93)**

```go
// Ports returns the typed port configuration for multipooler
func (c MultipoolerConfig) Ports() []provisioner.Port {
	return []provisioner.Port{
		{Number: c.HttpPort, Protocol: "http", Name: "http"},
		{Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
		{Number: c.PgPort, Protocol: "postgres", Name: "pg"},
	}
}
```

**Step 2: Build to verify**

Run: `go build ./go/provisioner/local/...`
Expected: Success

**Step 3: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "feat(provisioner): add Ports() method to MultipoolerConfig"
```

---

### Task 7: Add Ports() Method to MultiorchConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Add Ports() method after MultiorchConfig struct (around line 101)**

```go
// Ports returns the typed port configuration for multiorch
func (c MultiorchConfig) Ports() []provisioner.Port {
	return []provisioner.Port{
		{Number: c.HttpPort, Protocol: "http", Name: "http"},
		{Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
	}
}
```

**Step 2: Build to verify**

Run: `go build ./go/provisioner/local/...`
Expected: Success

**Step 3: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "feat(provisioner): add Ports() method to MultiorchConfig"
```

---

### Task 8: Add Ports() Method to MultiadminConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Add Ports() method after MultiadminConfig struct (around line 109)**

```go
// Ports returns the typed port configuration for multiadmin
func (c MultiadminConfig) Ports() []provisioner.Port {
	return []provisioner.Port{
		{Number: c.HttpPort, Protocol: "http", Name: "http"},
		{Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
	}
}
```

**Step 2: Build to verify**

Run: `go build ./go/provisioner/local/...`
Expected: Success

**Step 3: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "feat(provisioner): add Ports() method to MultiadminConfig"
```

---

### Task 9: Add Ports() Method to PgctldConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Add Ports() method after PgctldConfig struct (around line 123)**

```go
// Ports returns the typed port configuration for pgctld
func (c PgctldConfig) Ports() []provisioner.Port {
	return []provisioner.Port{
		{Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
		{Number: c.PgPort, Protocol: "postgres", Name: "pg"},
	}
}
```

**Step 2: Build to verify**

Run: `go build ./go/provisioner/local/...`
Expected: Success

**Step 3: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "feat(provisioner): add Ports() method to PgctldConfig"
```

---

### Task 10: Add Tests for Ports() Methods

**Files:**

- Create: `go/provisioner/local/config_test.go`

**Step 1: Create test file**

```go
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

package local

import (
	"testing"

	"github.com/multigres/multigres/go/provisioner"
	"github.com/stretchr/testify/assert"
)

func TestEtcdConfigPorts(t *testing.T) {
	t.Run("with explicit peer port", func(t *testing.T) {
		cfg := EtcdConfig{Port: 2379, PeerPort: 2380}
		ports := cfg.Ports()

		assert.Len(t, ports, 2)

		client, found := provisioner.FindPort(ports, "client")
		assert.True(t, found)
		assert.Equal(t, 2379, client.Number)
		assert.Equal(t, "http", client.Protocol)

		peer, found := provisioner.FindPort(ports, "peer")
		assert.True(t, found)
		assert.Equal(t, 2380, peer.Number)
		assert.Equal(t, "http", peer.Protocol)
	})

	t.Run("with default peer port", func(t *testing.T) {
		cfg := EtcdConfig{Port: 2379, PeerPort: 0}
		ports := cfg.Ports()

		peer, found := provisioner.FindPort(ports, "peer")
		assert.True(t, found)
		assert.Equal(t, 2380, peer.Number) // Port + 1
	})
}

func TestMultigatewayConfigPorts(t *testing.T) {
	cfg := MultigatewayConfig{HttpPort: 8080, GrpcPort: 9090, PgPort: 5432}
	ports := cfg.Ports()

	assert.Len(t, ports, 3)

	http, _ := provisioner.FindPort(ports, "http")
	assert.Equal(t, 8080, http.Number)
	assert.Equal(t, "http", http.Protocol)

	grpc, _ := provisioner.FindPort(ports, "grpc")
	assert.Equal(t, 9090, grpc.Number)
	assert.Equal(t, "grpc", grpc.Protocol)

	pg, _ := provisioner.FindPort(ports, "pg")
	assert.Equal(t, 5432, pg.Number)
	assert.Equal(t, "postgres", pg.Protocol)
}

func TestMultipoolerConfigPorts(t *testing.T) {
	cfg := MultipoolerConfig{HttpPort: 8081, GrpcPort: 9091, PgPort: 5433}
	ports := cfg.Ports()

	assert.Len(t, ports, 3)
	http, _ := provisioner.FindPort(ports, "http")
	assert.Equal(t, 8081, http.Number)
}

func TestMultiorchConfigPorts(t *testing.T) {
	cfg := MultiorchConfig{HttpPort: 8082, GrpcPort: 9092}
	ports := cfg.Ports()

	assert.Len(t, ports, 2)
	http, _ := provisioner.FindPort(ports, "http")
	assert.Equal(t, 8082, http.Number)
}

func TestMultiadminConfigPorts(t *testing.T) {
	cfg := MultiadminConfig{HttpPort: 8083, GrpcPort: 9093}
	ports := cfg.Ports()

	assert.Len(t, ports, 2)
	grpc, _ := provisioner.FindPort(ports, "grpc")
	assert.Equal(t, 9093, grpc.Number)
}

func TestPgctldConfigPorts(t *testing.T) {
	cfg := PgctldConfig{GrpcPort: 9094, PgPort: 5434}
	ports := cfg.Ports()

	assert.Len(t, ports, 2)

	grpc, _ := provisioner.FindPort(ports, "grpc")
	assert.Equal(t, 9094, grpc.Number)
	assert.Equal(t, "grpc", grpc.Protocol)

	pg, _ := provisioner.FindPort(ports, "pg")
	assert.Equal(t, 5434, pg.Number)
	assert.Equal(t, "postgres", pg.Protocol)
}
```

**Step 2: Run tests**

Run: `go test ./go/provisioner/local/... -v -run "TestEtcd|TestMulti|TestPgctld"`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add go/provisioner/local/config_test.go
git commit -m "test(provisioner): add tests for config Ports() methods"
```

---

## Phase 3: Update ProvisionResult and LocalProvisionedService

### Task 11: Update ProvisionResult.Ports Type

**Files:**

- Modify: `go/provisioner/provisioner.go:57`

**Step 1: Change Ports field type**

Change line 57 from:

```go
Ports map[string]int
```

to:

```go
Ports []Port
```

**Step 2: Attempt build to find all breakages**

Run: `go build ./go/...`
Expected: Multiple compile errors showing all places that need updating

Note the errors - these will guide Phase 4 tasks. Do not fix them yet.

**Step 3: Commit the type change (with broken build)**

```bash
git add go/provisioner/provisioner.go
git commit -m "refactor(provisioner): change ProvisionResult.Ports to []Port

BREAKING: This commit intentionally breaks the build.
Subsequent commits will fix all usages."
```

---

### Task 12: Update LocalProvisionedService.Ports Type

**Files:**

- Modify: `go/provisioner/local/state.go:36`

**Step 1: Add import for provisioner package if not present**

**Step 2: Change Ports field type**

Change line 36 from:

```go
Ports      map[string]int `json:"ports"`
```

to:

```go
Ports []provisioner.Port `json:"ports"`
```

**Step 3: Build to see additional breakages**

Run: `go build ./go/provisioner/local/...`
Expected: More compile errors

**Step 4: Commit**

```bash
git add go/provisioner/local/state.go
git commit -m "refactor(provisioner): change LocalProvisionedService.Ports to []Port

BREAKING: Build still broken, fixing usages next."
```

---

## Phase 4: Fix All Usages (largest phase)

### Task 13: Fix provisionEtcd in local.go

**Files:**

- Modify: `go/provisioner/local/local.go`

**Step 1: Update provisionEtcd function (around line 120-260)**

Find the function `provisionEtcd`. Make these changes:

1. Remove the line:

```go
etcdConfig := p.getServiceConfig("etcd")
```

2. Replace config access with direct struct access:

```go
cfg := p.config.Etcd
```

3. Replace port extraction (around line 147-157):

```go
// Get port from config or use default
port := cfg.Port
if port == 0 {
	port = ports.DefaultEtcdPort
}

// Get peer port from config, or default to port + 1
peerPort := cfg.PeerPort
if peerPort == 0 {
	peerPort = port + 1
}
```

4. Replace version check (around line 166):

```go
if cfg.Version != "" {
	if err := p.checkEtcdVersion(etcdBinary, cfg.Version); err != nil {
		return nil, fmt.Errorf("etcd version check failed: %w", err)
	}
}
```

5. Replace data dir access (around line 173):

```go
dataDir := cfg.DataDir
if dataDir == "" {
	return nil, fmt.Errorf("etcd data directory not configured")
}
```

6. Update the ProvisionResult return (around line 246-260) to use []Port:

```go
return &provisioner.ProvisionResult{
	ServiceName: "etcd",
	FQDN:        "localhost",
	Ports: []provisioner.Port{
		{Number: port, Protocol: "http", Name: "client"},
		{Number: peerPort, Protocol: "http", Name: "peer"},
	},
	Metadata: map[string]any{
		"service_id": serviceID,
		"log_file":   logFile,
	},
}, nil
```

7. Update the existing service return (around line 136-144):

```go
return &provisioner.ProvisionResult{
	ServiceName: "etcd",
	FQDN:        existingService.FQDN,
	Ports:       existingService.Ports,
	Metadata: map[string]any{
		"service_id": existingService.ID,
		"log_file":   existingService.LogFile,
	},
}, nil
```

8. Update saveServiceState call to use []Port for the Ports field in LocalProvisionedService.

**Step 2: Build to check progress**

Run: `go build ./go/provisioner/local/...`
Expected: Fewer errors (or new errors to address)

**Step 3: Commit**

```bash
git add go/provisioner/local/local.go
git commit -m "refactor(provisioner): update provisionEtcd to use typed config and ports"
```

---

### Task 14: Fix provisionMultigateway in local.go

**Files:**

- Modify: `go/provisioner/local/local.go`

**Step 1: Update provisionMultigateway function**

Similar pattern to Task 13:

1. Replace:

```go
multigatewayConfig, err := p.getCellServiceConfig(cell, "multigateway")
```

with:

```go
cellServices, exists := p.config.Cells[cell]
if !exists {
	return nil, fmt.Errorf("cell %s not found in configuration", cell)
}
cfg := cellServices.Multigateway
```

2. Replace all `multigatewayConfig["http_port"].(int)` style access with `cfg.HttpPort`

3. Update ProvisionResult.Ports to use []provisioner.Port:

```go
Ports: cfg.Ports(),
```

4. Update LocalProvisionedService construction similarly.

**Step 2: Build to check**

Run: `go build ./go/provisioner/local/...`

**Step 3: Commit**

```bash
git add go/provisioner/local/local.go
git commit -m "refactor(provisioner): update provisionMultigateway to use typed config"
```

---

### Task 15: Fix provisionMultiadmin in local.go

**Files:**

- Modify: `go/provisioner/local/local.go`

**Step 1: Update provisionMultiadmin function**

1. Replace:

```go
multiadminConfig := p.getServiceConfig("multiadmin")
```

with:

```go
cfg := p.config.Multiadmin
```

2. Update all config access and ProvisionResult.Ports

**Step 2: Build and commit**

```bash
git add go/provisioner/local/local.go
git commit -m "refactor(provisioner): update provisionMultiadmin to use typed config"
```

---

### Task 16: Fix provisionMultipooler in local.go

**Files:**

- Modify: `go/provisioner/local/local.go`

**Step 1: Update provisionMultipooler function**

Same pattern - replace getCellServiceConfig with direct config access, update Ports.

**Step 2: Build and commit**

```bash
git add go/provisioner/local/local.go
git commit -m "refactor(provisioner): update provisionMultipooler to use typed config"
```

---

### Task 17: Fix provisionMultiorch in local.go

**Files:**

- Modify: `go/provisioner/local/local.go`

**Step 1: Update provisionMultiorch function**

Same pattern.

**Step 2: Build and commit**

```bash
git add go/provisioner/local/local.go
git commit -m "refactor(provisioner): update provisionMultiorch to use typed config"
```

---

### Task 18: Fix pgctld.go

**Files:**

- Modify: `go/provisioner/local/pgctld.go`

**Step 1: Update provisionPgctld function**

1. Replace getCellServiceConfig call with direct config access
2. Update all port access
3. Update PgctldProvisionResult construction
4. Update LocalProvisionedService.Ports

**Step 2: Update deprovisionPgctld**

Change:

```go
grpcPort := service.Ports["grpc_port"]
```

to:

```go
grpcPort, found := provisioner.FindPort(service.Ports, "grpc")
if !found {
	return fmt.Errorf("grpc port not found in service ports")
}
// Use grpcPort.Number
```

**Step 3: Build and commit**

```bash
git add go/provisioner/local/pgctld.go
git commit -m "refactor(provisioner): update pgctld to use typed config and ports"
```

---

### Task 19: Fix healthcheck.go

**Files:**

- Modify: `go/provisioner/local/healthcheck.go`

**Step 1: Update waitForServiceReady signature**

Change:

```go
func (p *localProvisioner) waitForServiceReady(parentCtx context.Context, serviceName string, host string, servicePorts map[string]int, timeout time.Duration) error
```

to:

```go
func (p *localProvisioner) waitForServiceReady(parentCtx context.Context, serviceName string, host string, servicePorts []provisioner.Port, timeout time.Duration) error
```

**Step 2: Update TCP connectivity check (around line 54-66)**

```go
allPortsReady := true
for _, port := range servicePorts {
	address := net.JoinHostPort(host, fmt.Sprintf("%d", port.Number))
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		allPortsReady = false
		break
	}
	conn.Close()
}
```

**Step 3: Update checkMultigresServiceHealth signature and implementation**

```go
func (p *localProvisioner) checkMultigresServiceHealth(ctx context.Context, serviceName string, host string, servicePorts []provisioner.Port) error {
	for _, port := range servicePorts {
		switch port.Protocol {
		case "http":
			httpAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port.Number))
			if err := p.checkDebugConfigEndpoint(ctx, httpAddress); err != nil {
				return err
			}
		case "grpc":
			if serviceName == "pgctld" {
				grpcAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port.Number))
				if err := p.checkPgctldGrpcHealth(ctx, grpcAddress); err != nil {
					return err
				}
			}
		// "postgres" and "tcp" protocols: TCP check already done above
		}
	}
	return nil
}
```

**Step 4: Remove the etcd_port special case** (no longer needed - it's just "http" protocol now)

**Step 5: Build and commit**

```bash
git add go/provisioner/local/healthcheck.go
git commit -m "refactor(provisioner): update health checks to use []Port with protocol field"
```

---

### Task 20: Fix state.go Port Functions

**Files:**

- Modify: `go/provisioner/local/state.go`

**Step 1: Update getExpectedPortsForCellService**

This function currently returns `map[string]int`. Update to return `[]provisioner.Port`:

```go
func (p *localProvisioner) getExpectedPortsForCellService(cell, serviceName string) []provisioner.Port {
	cellServices, exists := p.config.Cells[cell]
	if !exists {
		return nil
	}

	switch serviceName {
	case "multigateway":
		return cellServices.Multigateway.Ports()
	case "multipooler":
		return cellServices.Multipooler.Ports()
	case "multiorch":
		return cellServices.Multiorch.Ports()
	case "pgctld":
		return cellServices.Pgctld.Ports()
	default:
		return nil
	}
}
```

**Step 2: Update getExpectedPortsForService**

```go
func (p *localProvisioner) getExpectedPortsForService(serviceName string) []provisioner.Port {
	switch serviceName {
	case "etcd":
		return p.config.Etcd.Ports()
	case "multiadmin":
		return p.config.Multiadmin.Ports()
	default:
		return nil
	}
}
```

**Step 3: Update any callers of these functions**

**Step 4: Build and commit**

```bash
git add go/provisioner/local/state.go
git commit -m "refactor(provisioner): update state.go port functions to return []Port"
```

---

### Task 21: Fix Bootstrap and Other Callers in local.go

**Files:**

- Modify: `go/provisioner/local/local.go`

**Step 1: Find and fix remaining usages**

Search for `.Ports[` to find map-style access that needs updating:

```bash
grep -n '\.Ports\[' go/provisioner/local/local.go
```

Update each occurrence to use `provisioner.FindPort()` or direct field access.

For example, around line 1241:

```go
tcpPort := etcdResult.Ports["tcp"]
```

becomes:

```go
clientPort, _ := provisioner.FindPort(etcdResult.Ports, "client")
tcpPort := clientPort.Number
```

**Step 2: Build until clean**

Run: `go build ./go/provisioner/local/...`
Expected: Success, no errors

**Step 3: Commit**

```bash
git add go/provisioner/local/local.go
git commit -m "refactor(provisioner): fix remaining Ports map access in local.go"
```

---

## Phase 5: Remove Deprecated Functions and Cleanup

### Task 22: Remove getServiceConfig and getCellServiceConfig

**Files:**

- Modify: `go/provisioner/local/config.go`

**Step 1: Verify no remaining callers**

```bash
grep -rn "getServiceConfig\|getCellServiceConfig" go/provisioner/
```

Expected: Only the function definitions, no callers

**Step 2: Delete the functions**

Remove `getServiceConfig` (lines ~319-339) and `getCellServiceConfig` (lines ~341-394)

**Step 3: Build to verify**

Run: `go build ./go/provisioner/...`
Expected: Success

**Step 4: Commit**

```bash
git add go/provisioner/local/config.go
git commit -m "refactor(provisioner): remove deprecated getServiceConfig functions"
```

---

### Task 23: Run Full Test Suite

**Files:** None (verification only)

**Step 1: Run all provisioner tests**

Run: `go test ./go/provisioner/... -v`
Expected: All tests pass

**Step 2: Run integration tests if available**

Check for integration tests and run them.

**Step 3: Commit any test fixes needed**

---

### Task 24: Update Design Doc Status

**Files:**

- Modify: `docs/plans/2025-11-25-provisioner-type-safety-design.md`

**Step 1: Add implementation status**

Add at the end of the design doc:

```markdown
## Implementation Status

**Completed:** 2025-11-XX

All phases implemented:

- [x] Phase 1: Add Port type and helpers
- [x] Phase 2: Add Ports() methods to config structs
- [x] Phase 3: Update ProvisionResult and LocalProvisionedService
- [x] Phase 4: Fix all usages
- [x] Phase 5: Remove deprecated functions
```

**Step 2: Commit**

```bash
git add docs/plans/2025-11-25-provisioner-type-safety-design.md
git commit -m "docs: mark provisioner type safety design as implemented"
```

---

## Summary

**Total Tasks:** 24
**Estimated Time:** 2-3 hours

**Key Files Modified:**

- `go/provisioner/provisioner.go` - Port type and helpers
- `go/provisioner/local/config.go` - Ports() methods, remove old functions
- `go/provisioner/local/local.go` - All provisioning functions
- `go/provisioner/local/state.go` - State types and port functions
- `go/provisioner/local/healthcheck.go` - Health check protocol handling
- `go/provisioner/local/pgctld.go` - Pgctld provisioning

**New Files:**

- `go/provisioner/port_test.go`
- `go/provisioner/local/config_test.go`
