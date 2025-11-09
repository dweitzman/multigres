// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stringutil

import (
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ComponentTypeToString converts a ComponentType enum to its string representation.
// This function uses the generated name map to be resilient to refactors.
// It's not specific to any single component type and can be used across the topology system.
func ComponentTypeToString(component clustermetadatapb.ID_ComponentType) string {
	// Use the generated name map for resilience - this automatically updates when the proto changes
	if name, exists := clustermetadatapb.ID_ComponentType_name[int32(component)]; exists {
		// Convert the generated name (e.g., "MULTIPOOLER") to lowercase for consistency
		return strings.ToLower(name)
	}
	return "unknown"
}

// Random string generation utilities copied from Kubernetes codebase
var rng = struct {
	sync.Mutex
	rand *rand.Rand
}{
	rand: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
}

const (
	// We omit vowels from the set of available characters to reduce the chances
	// of "bad words" being formed.
	alphanums = "bcdfghjklmnpqrstvwxz2456789"
	// No. of bits required to index into alphanums string.
	alphanumsIdxBits = 5
	// Mask used to extract last alphanumsIdxBits of an int.
	alphanumsIdxMask = 1<<alphanumsIdxBits - 1
	// No. of random letters we can extract from a single int63.
	maxAlphanumsPerInt = 63 / alphanumsIdxBits
)

// generateString is the core string generation logic shared by RandomString and DeterministicString.
// It extracts random letters from int64s using bit-shifting and masking.
func generateString(n int, r *rand.Rand) string {
	b := make([]byte, n)
	randomInt64 := r.Int64()
	remaining := maxAlphanumsPerInt
	for i := 0; i < n; {
		if remaining == 0 {
			randomInt64, remaining = r.Int64(), maxAlphanumsPerInt
		}
		if idx := int(randomInt64 & alphanumsIdxMask); idx < len(alphanums) {
			b[i] = alphanums[idx]
			i++
		}
		randomInt64 >>= alphanumsIdxBits
		remaining--
	}
	return string(b)
}

// RandomString generates a random alphanumeric string, without vowels, which is n
// characters long.  This will panic if n is less than zero.
func RandomString(n int) string {
	rng.Lock()
	defer rng.Unlock()
	return generateString(n, rng.rand)
}

// DeterministicString generates a deterministic alphanumeric string based on a seed.
// The string will be n characters long and will always be the same for the same seed.
// This is useful for generating stable service IDs across restarts.
func DeterministicString(n int, seed uint64) string {
	r := rand.New(rand.NewPCG(seed, seed))
	return generateString(n, r)
}
