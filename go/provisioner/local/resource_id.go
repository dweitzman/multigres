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
	"fmt"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// ResourceIDString returns a human-readable string representation of a resource ID.
// This is useful for logging, display, and error messages.
func ResourceIDString(id *clustermetadata.ID) string {
	if id == nil {
		return "<nil>"
	}

	componentName := id.Component.String()

	// For global resources, just return component and name
	if id.Cell == topo.GlobalCell {
		if id.Name == "" {
			return componentName
		}
		return fmt.Sprintf("%s:%s", componentName, id.Name)
	}

	// For cell-scoped resources, include all three parts
	if id.Name == "" {
		return fmt.Sprintf("%s:%s", componentName, id.Cell)
	}
	return fmt.Sprintf("%s:%s:%s", componentName, id.Cell, id.Name)
}

// ResourceIDsEqual compares two resource IDs for equality.
func ResourceIDsEqual(a, b *clustermetadata.ID) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Component == b.Component && a.Cell == b.Cell && a.Name == b.Name
}

// FindResourceByID searches for a resource with the given ID in the resource tree.
// Returns nil if not found.
func FindResourceByID(root Resource, targetID *clustermetadata.ID) Resource {
	if ResourceIDsEqual(root.ID(), targetID) {
		return root
	}

	for _, child := range root.Children() {
		if found := FindResourceByID(child, targetID); found != nil {
			return found
		}
	}

	return nil
}
