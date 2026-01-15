// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcfaultproxy

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds configuration for the gRPC fault injection proxy.
type Config struct {
	// HTTPAddr is the address for the HTTP CONNECT server (e.g., ":17000")
	HTTPAddr string

	// RulesFile is the path to the fault rules YAML file (optional, for Phase 2)
	RulesFile string
}

// FaultRule defines a fault injection rule.
// Supports wildcards ("*") for Source, Target, and Method fields.
type FaultRule struct {
	// Name is a human-readable identifier for this rule
	Name string `yaml:"name"`

	// Source is the service name pattern (e.g., "multipooler-zone1-0" or "*")
	Source string `yaml:"source"`

	// Target is the target service pattern (e.g., "multipooler-zone1-1" or "*")
	Target string `yaml:"target"`

	// Method is the gRPC method pattern (e.g., "/consensus.*/RequestVote" or "*")
	Method string `yaml:"method"`

	// FaultType is one of: "latency", "error", "drop"
	FaultType string `yaml:"fault_type"`

	// Probability is the chance of injecting this fault (0.0 to 1.0)
	Probability float64 `yaml:"probability"`

	// LatencyMs is the latency to inject in milliseconds (for "latency" faults)
	LatencyMs int `yaml:"latency_ms,omitempty"`

	// ErrorCode is the gRPC status code to return (for "error" faults)
	ErrorCode int `yaml:"error_code,omitempty"`

	// ErrorMsg is the error message to return (for "error" faults)
	ErrorMsg string `yaml:"error_msg,omitempty"`
}

// RulesConfig is the YAML structure for fault rules configuration.
type RulesConfig struct {
	Rules []FaultRule `yaml:"rules"`
}

// LoadRules loads fault rules from a YAML file.
// Returns an empty slice if the file doesn't exist or is empty.
func LoadRules(path string) ([]FaultRule, error) {
	if path == "" {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	var config RulesConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return config.Rules, nil
}
