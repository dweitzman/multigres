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
	"context"
	"math/rand/v2"
	"strings"
	"sync"
)

// Fault represents a fault to inject into a request.
type Fault struct {
	// Type is one of: "latency", "error", "drop"
	Type string

	// LatencyMs is the latency to inject in milliseconds
	LatencyMs int

	// ErrorCode is the gRPC status code to return
	ErrorCode int

	// ErrorMsg is the error message to return
	ErrorMsg string
}

// Engine evaluates fault injection rules and decides when to inject faults.
type Engine struct {
	mu    sync.RWMutex
	rules []FaultRule
	rng   *rand.Rand
}

// NewEngine creates a new fault injection engine.
func NewEngine(rules []FaultRule) *Engine {
	return &Engine{
		rules: rules,
		rng:   rand.New(rand.NewPCG(0, 0)), // Seeded for determinism in tests
	}
}

// Evaluate checks if a fault should be injected for this request.
// Returns nil if no fault should be injected.
func (e *Engine) Evaluate(ctx context.Context, req RequestInfo) *Fault {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rule := range e.rules {
		if !e.matches(rule, req) {
			continue
		}

		// Check probability
		if e.rng.Float64() < rule.Probability {
			return &Fault{
				Type:      rule.FaultType,
				LatencyMs: rule.LatencyMs,
				ErrorCode: rule.ErrorCode,
				ErrorMsg:  rule.ErrorMsg,
			}
		}
	}

	return nil // No fault
}

// matches checks if a rule matches the given request.
func (e *Engine) matches(rule FaultRule, req RequestInfo) bool {
	return matchPattern(rule.Source, req.Source) &&
		matchPattern(rule.Target, req.Target) &&
		matchPattern(rule.Method, req.Method)
}

// matchPattern supports "*" wildcard and exact matching.
// Also supports prefix matching with "*" at the end (e.g., "multipooler*").
func matchPattern(pattern, value string) bool {
	if pattern == "*" {
		return true
	}

	// Prefix matching: "multipooler*" matches "multipooler-zone1-0"
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(value, prefix)
	}

	// Exact matching
	return pattern == value
}

// UpdateRules updates the fault injection rules.
// This allows dynamic rule updates without restarting the proxy.
func (e *Engine) UpdateRules(rules []FaultRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = rules
}

// GetRules returns a copy of the current rules.
func (e *Engine) GetRules() []FaultRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]FaultRule(nil), e.rules...)
}
