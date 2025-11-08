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
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// StatusPrinter is an optional interface that resources can implement
// to provide custom status printing.
type StatusPrinter interface {
	// PrintStatus prints the status of this resource and its children.
	// It should:
	// 1. Print starting message immediately
	// 2. Recursively print children (blocks on each child)
	// 3. Wait for this resource to complete
	// 4. Print final status
	// The indent parameter controls the indentation level for hierarchical display.
	PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error
}

// DefaultStatusPrinter provides default status printing for resources that don't
// implement custom printing.
type DefaultStatusPrinter struct{}

// PrintStatus implements the default status printing behavior.
func (d *DefaultStatusPrinter) PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	// Print starting message immediately
	fmt.Fprintf(w, "%s⏳ Starting %s...\n", indentStr, node.Resource.DisplayName())

	// Print children recursively (this blocks on each child completing)
	for _, child := range node.Children {
		if err := PrintResourceStatus(ctx, w, child, indent+1); err != nil {
			return err
		}
	}

	// Wait for this resource to complete
	select {
	case <-node.completed:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Get final status and print result
	status := node.GetStatus()

	switch status.State {
	case StateDiscovered:
		fmt.Fprintf(w, "%s✓ %s (already exists)\n", indentStr, node.Resource.DisplayName())
		d.printMetadata(w, status, indent)

	case StateProvisioned:
		fmt.Fprintf(w, "%s✓ %s started\n", indentStr, node.Resource.DisplayName())
		d.printMetadata(w, status, indent)

	case StateFailed:
		fmt.Fprintf(w, "%s✗ %s failed", indentStr, node.Resource.DisplayName())
		if status.Error != nil {
			fmt.Fprintf(w, ": %v", status.Error)
		}
		fmt.Fprintln(w)

	default:
		fmt.Fprintf(w, "%s? %s (unknown state: %v)\n", indentStr, node.Resource.DisplayName(), status.State)
	}

	return nil
}

// printMetadata prints common metadata fields.
func (d *DefaultStatusPrinter) printMetadata(w io.Writer, status ResourceStatus, indent int) {
	indentStr := strings.Repeat("  ", indent)

	if addr, ok := status.Metadata["address"].(string); ok {
		fmt.Fprintf(w, "%s  └─ Address: %s\n", indentStr, addr)
	}
	if ports, ok := status.Metadata["ports"].(map[string]int); ok {
		for name, port := range ports {
			fmt.Fprintf(w, "%s  └─ %s port: %d\n", indentStr, name, port)
		}
	}
	if pid, ok := status.Metadata["pid"].(int); ok {
		fmt.Fprintf(w, "%s  └─ PID: %d\n", indentStr, pid)
	}
}

// PrintResourceStatus prints the status of a resource and its children.
// It checks if the resource implements StatusPrinter; if so, uses that, otherwise uses default.
func PrintResourceStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	if w == nil {
		w = os.Stdout
	}

	// Check if resource implements custom printing
	if printer, ok := node.Resource.(StatusPrinter); ok {
		return printer.PrintStatus(ctx, w, node, indent)
	}

	// Use default printer
	defaultPrinter := &DefaultStatusPrinter{}
	return defaultPrinter.PrintStatus(ctx, w, node, indent)
}

// PrintProvisioningStatus starts printing status updates for a provisioning operation.
// This should be called concurrently with orchestrator.Execute().
func PrintProvisioningStatus(ctx context.Context, w io.Writer, orchestrator *ResourceOrchestrator) error {
	return PrintResourceStatus(ctx, w, orchestrator.GetRootNode(), 0)
}

// CountResourcesByState counts resources by their state in the tree.
func CountResourcesByState(root *ResourceNode) map[ResourceState]int {
	counts := make(map[ResourceState]int)
	countRecursive(root, counts)
	return counts
}

func countRecursive(node *ResourceNode, counts map[ResourceState]int) {
	status := node.GetStatus()
	counts[status.State]++
	for _, child := range node.Children {
		countRecursive(child, counts)
	}
}

// FormatResourceState returns a human-readable string for a resource state with an icon.
func FormatResourceState(state ResourceState) string {
	switch state {
	case StateDiscovered:
		return "✓ Discovered"
	case StateProvisioned:
		return "✓ Provisioned"
	case StateProvisioning:
		return "⏳ Provisioning"
	case StateFailed:
		return "✗ Failed"
	case StateDeprovisioned:
		return "🗑 Deprovisioned"
	case StateNotFound:
		return "○ Not Found"
	default:
		return "? Unknown"
	}
}
