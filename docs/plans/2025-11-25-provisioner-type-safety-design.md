# Local Provisioner Type Safety Improvements

## Problem

The local provisioner uses `map[string]any` extensively for configuration and port management. This leads to:

1. **Silent bugs from typos** - String keys like `"http_port"` vs `"http-port"` fail silently at runtime
2. **Forgotten fields** - The `getServiceConfig()` function for etcd is missing `PeerPort`, discovered only at runtime
3. **Lost type safety** - Code converts typed structs to maps, then immediately does type assertions to get values back
4. **Implicit protocol conventions** - Port protocols are inferred from key names (`"http_port"` implies HTTP), not explicit

### Example of Current Problem

```go
// config.go - typed struct exists
type EtcdConfig struct {
    Port     int `yaml:"port"`
    PeerPort int `yaml:"peer-port"`  // Exists in struct
}

// config.go - manual conversion loses PeerPort
func (p *localProvisioner) getServiceConfig(service string) map[string]any {
    case "etcd":
        return map[string]any{
            "port":     p.config.Etcd.Port,
            // BUG: PeerPort is missing!
        }
}

// local.go - caller does type assertions to recover typing
etcdConfig := p.getServiceConfig("etcd")
port := etcdConfig["port"].(int)  // Runtime type assertion
```

## Design

### Principle: Preserve Types as Long as Possible

- Use typed structs for all internal operations
- Only convert to maps at true serialization boundaries (external interfaces that require it)
- Make protocol information explicit, not convention-based

### 1. New Port Type

```go
// go/provisioner/provisioner.go

// Port represents a network port with its protocol and identifier
type Port struct {
    Number   int    `json:"number"`
    Protocol string `json:"protocol"` // "http", "grpc", "tcp", "postgres"
    Name     string `json:"name"`     // "client", "peer", "http_port", etc.
}
```

**Rationale:**

- `Number` - the actual port number
- `Protocol` - explicit protocol for health checks (no inferring from name)
- `Name` - human-readable identifier, allows multiple ports with same protocol

### 2. Update ProvisionResult

```go
// go/provisioner/provisioner.go

type ProvisionResult struct {
    ServiceName string
    FQDN        string
    Ports       []Port         // Changed from map[string]int
    Metadata    map[string]any
}
```

### 3. Update LocalProvisionedService

```go
// go/provisioner/local/state.go

type LocalProvisionedService struct {
    ID         string    `json:"id"`
    Service    string    `json:"service"`
    PID        int       `json:"pid,omitempty"`
    BinaryPath string    `json:"binary-path,omitempty"`
    DataDir    string    `json:"data-dir,omitempty"`
    LogFile    string    `json:"log-file,omitempty"`
    Ports      []Port    `json:"ports"`  // Changed from map[string]int
    FQDN       string    `json:"fqdn"`
    Runtime    string    `json:"runtime"`
    StartedAt  time.Time `json:"started-at"`
    Metadata   map[string]any `json:"metadata,omitempty"`
}
```

### 4. Config Structs Return Typed Ports

Each service config struct gets a `Ports()` method:

```go
// go/provisioner/local/config.go

func (c EtcdConfig) Ports() []Port {
    ports := []Port{
        {Number: c.Port, Protocol: "http", Name: "client"},
    }
    if c.PeerPort > 0 {
        ports = append(ports, Port{Number: c.PeerPort, Protocol: "http", Name: "peer"})
    }
    return ports
}

func (c MultigatewayConfig) Ports() []Port {
    return []Port{
        {Number: c.HttpPort, Protocol: "http", Name: "http"},
        {Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
        {Number: c.PgPort, Protocol: "postgres", Name: "pg"},
    }
}

func (c MultipoolerConfig) Ports() []Port {
    return []Port{
        {Number: c.HttpPort, Protocol: "http", Name: "http"},
        {Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
        {Number: c.PgPort, Protocol: "postgres", Name: "pg"},
    }
}

func (c MultiorchConfig) Ports() []Port {
    return []Port{
        {Number: c.HttpPort, Protocol: "http", Name: "http"},
        {Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
    }
}

func (c MultiadminConfig) Ports() []Port {
    return []Port{
        {Number: c.HttpPort, Protocol: "http", Name: "http"},
        {Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
    }
}

func (c PgctldConfig) Ports() []Port {
    return []Port{
        {Number: c.GrpcPort, Protocol: "grpc", Name: "grpc"},
        {Number: c.PgPort, Protocol: "postgres", Name: "pg"},
    }
}
```

### 5. Remove getServiceConfig / getCellServiceConfig

These functions currently convert typed structs to maps, losing type safety. Remove them and access config directly:

**Before:**

```go
func (p *localProvisioner) provisionEtcd(...) {
    etcdConfig := p.getServiceConfig("etcd")
    port := etcdConfig["port"].(int)
    peerPort := etcdConfig["peer-port"].(int)
    version := etcdConfig["version"].(string)
}
```

**After:**

```go
func (p *localProvisioner) provisionEtcd(...) {
    cfg := p.config.Etcd
    port := cfg.Port          // Compile-time checked
    peerPort := cfg.PeerPort  // Can't forget this
    version := cfg.Version
}
```

### 6. Update Health Checks

```go
// go/provisioner/local/healthcheck.go

func (p *localProvisioner) checkMultigresServiceHealth(
    ctx context.Context,
    serviceName string,
    host string,
    ports []Port,  // Changed from map[string]int
) error {
    for _, port := range ports {
        switch port.Protocol {
        case "http":
            if err := p.checkHTTPHealth(ctx, host, port.Number); err != nil {
                return err
            }
        case "grpc":
            if err := p.checkGRPCHealth(ctx, serviceName, host, port.Number); err != nil {
                return err
            }
        case "tcp":
            if err := p.checkTCPHealth(ctx, host, port.Number); err != nil {
                return err
            }
        // postgres protocol might not need active health check, just TCP
        }
    }
    return nil
}
```

### 7. Helper Functions

```go
// go/provisioner/provisioner.go

// FindPort finds a port by name in a slice of ports
func FindPort(ports []Port, name string) (Port, bool) {
    for _, p := range ports {
        if p.Name == name {
            return p, true
        }
    }
    return Port{}, false
}

// FindPortByProtocol finds the first port with a given protocol
func FindPortByProtocol(ports []Port, protocol string) (Port, bool) {
    for _, p := range ports {
        if p.Protocol == protocol {
            return p, true
        }
    }
    return Port{}, false
}
```

## Migration Path

### Phase 1: Add Port Type and Ports() Methods

- Add `Port` type to `provisioner.go`
- Add `Ports()` methods to all config structs
- Add helper functions

### Phase 2: Update State and Results

- Change `ProvisionResult.Ports` to `[]Port`
- Change `LocalProvisionedService.Ports` to `[]Port`
- Update all code that constructs these types

### Phase 3: Update Health Checks

- Update `waitForServiceReady` and `checkMultigresServiceHealth` to use `[]Port`
- Switch on `port.Protocol` instead of port name conventions

### Phase 4: Remove Map-Based Config Access

- Update provisioning functions to access config structs directly
- Remove `getServiceConfig()` and `getCellServiceConfig()`
- Update all call sites

### Phase 5: Cleanup

- Remove any remaining string-based port key constants
- Update tests

## Benefits

1. **Compile-time safety** - Missing fields are caught at build time, not runtime
2. **Single source of truth** - Config structs define what exists; `Ports()` methods ensure complete conversion
3. **Explicit protocols** - No convention-based inference; protocol is a field
4. **Flexibility** - Can have multiple ports with same protocol (e.g., admin HTTP + public HTTP)
5. **Self-describing state** - Disk state includes protocol, no reconstruction needed

## Non-Goals

- Changing protobuf definitions (out of scope)
- Changing the `Provisioner` interface signature for `DefaultConfig()` / `ValidateConfig()` (these can remain `map[string]any` at the interface boundary if needed)
- Performance optimization (local provisioner is not production code)

## Open Questions

None at this time.
