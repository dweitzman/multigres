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

package stringutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomString_Length(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"zero length", 0},
		{"single character", 1},
		{"short string", 5},
		{"medium string", 16},
		{"long string", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomString(tt.length)
			assert.Len(t, result, tt.length)
		})
	}
}

func TestRandomString_CharacterSet(t *testing.T) {
	// Generate a longer string to increase chance of hitting all characters
	result := RandomString(1000)

	// Verify all characters are from the allowed set (no vowels)
	for _, c := range result {
		assert.True(t, strings.ContainsRune(alphanums, c),
			"character %q should be in allowed set %q", c, alphanums)
	}

	// Verify no vowels are present
	vowels := "aeiouAEIOU"
	for _, v := range vowels {
		assert.False(t, strings.ContainsRune(result, v),
			"vowel %q should not appear in random string", v)
	}
}

func TestRandomString_Uniqueness(t *testing.T) {
	// Generate multiple strings and verify they're different
	// With 16 characters from 27-char alphabet, collision is extremely unlikely
	seen := make(map[string]bool)

	for range 100 {
		s := RandomString(16)
		require.False(t, seen[s], "duplicate string generated: %s", s)
		seen[s] = true
	}
}

func TestRandomString_Concurrency(t *testing.T) {
	// Verify thread safety - the implementation uses a mutex
	done := make(chan bool)

	for range 10 {
		go func() {
			for range 100 {
				s := RandomString(16)
				assert.Len(t, s, 16)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for range 10 {
		<-done
	}
}

func TestRandomString_NegativeLength(t *testing.T) {
	// According to the documentation, negative n should panic
	assert.Panics(t, func() {
		RandomString(-1)
	})
}
