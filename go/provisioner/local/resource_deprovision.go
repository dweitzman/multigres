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

// DeprovisionResource deprovisions a resource and all its children in reverse order.
// Children are deprovisioned before their parents (post-order traversal).
// Optionally prints progress to the provided writer.
func DeprovisionResource(ctx context.Context, node *ResourceNode, w io.Writer, indent int) error {
	if w == nil {
		w = os.Stdout
	}

	indentStr := strings.Repeat("  ", indent)

	// First, deprovision all children (post-order)
	for _, child := range node.Children {
		if err := DeprovisionResource(ctx, child, w, indent+1); err != nil {
			return err
		}
	}

	// Get current status
	status := node.GetStatus()

	// Skip if the resource was never provisioned or doesn't exist
	if status.State == StateNotFound || status.State == StateUnknown {
		return nil
	}

	// Print deprovisioning message
	fmt.Fprintf(w, "%s🗑 Deprovisioning %s...\n", indentStr, node.Resource.DisplayName())

	// Mark as deprovisioning
	node.SetStatus(ResourceStatus{
		State:   StateDeprovisioning,
		Message: "deprovisioning",
	})

	// Call deprovision
	if err := node.Resource.Deprovision(ctx, status); err != nil {
		// Mark as failed
		node.SetStatus(ResourceStatus{
			State:   StateFailed,
			Message: "deprovision failed",
			Error:   err,
		})
		fmt.Fprintf(w, "%s✗ Failed to deprovision %s: %v\n", indentStr, node.Resource.DisplayName(), err)
		return fmt.Errorf("failed to deprovision %s: %w", ResourceIDString(node.Resource.ID()), err)
	}

	// Mark as deprovisioned
	node.SetStatus(ResourceStatus{
		State:   StateDeprovisioned,
		Message: "deprovisioned successfully",
	})

	fmt.Fprintf(w, "%s✓ Deprovisioned %s\n", indentStr, node.Resource.DisplayName())

	return nil
}

// DeprovisionAll deprovisions all resources in an orchestrator's graph.
// This is a convenience function that deprovisions starting from the root.
func DeprovisionAll(ctx context.Context, orchestrator *ResourceOrchestrator, w io.Writer) error {
	if orchestrator == nil || orchestrator.root == nil {
		return fmt.Errorf("orchestrator or root is nil")
	}

	if w == nil {
		w = os.Stdout
	}

	fmt.Fprintln(w, "=== Deprovisioning Resources ===")
	fmt.Fprintln(w, "")

	return DeprovisionResource(ctx, orchestrator.GetRootNode(), w, 0)
}
