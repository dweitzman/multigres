// Copyright 2023 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications Copyright 2025 Supabase, Inc.

package debug

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/multigres/multigres/go/common/web"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/pflag"
)

// HandlerFunc returns an http.HandlerFunc that renders the combined config
// registry (both static and dynamic) for debugging purposes.
//
// Example requests (assuming registered at /config):
//   - GET /config
//   - GET /config?format=json
//   - POST /config with form data: key=<config-key>&value=<new-value>
//
// The fs parameter is the flag set containing the registered flags, used for
// type-safe parsing when setting dynamic config values via POST.
func HandlerFunc(reg *viperutil.Registry, fs *pflag.FlagSet) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Handle POST requests to set dynamic config values
		if r.Method == http.MethodPost {
			handleSetConfig(reg, fs, w, r)
			return
		}

		v := reg.Combined()
		format := strings.ToLower(r.URL.Query().Get("format"))

		// Collect command-line flags
		type ConfigData struct {
			Title   string
			Options map[string]string
			Config  map[string]string
		}
		configData := ConfigData{
			Title:   os.Args[0],
			Options: make(map[string]string),
			Config:  make(map[string]string),
		}
		pflag.CommandLine.VisitAll(func(flag *pflag.Flag) {
			if flag.Changed {
				configData.Options[flag.Name] = flag.Value.String()
			}
		})

		// Handle default format (debug text)
		if format == "" {
			for _, k := range v.AllKeys() {
				value := v.Get(k)
				if value == nil {
					// should not happen
					continue
				}
				configData.Config[k] = fmt.Sprintf("%v", value)
			}
			_ = web.Templates.ExecuteTemplate(w, "config.html", configData)
			return
		}

		// Handle JSON format specially to include both cmdline flags and viper config
		if format == "json" {
			w.Header().Set("Content-Type", "application/json")

			response := map[string]any{
				"command_line_flags": configData.Options,
				"viper_config":       v.AllSettings(),
			}

			encoder := json.NewEncoder(w)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(response); err != nil {
				http.Error(w, fmt.Sprintf("failed to encode JSON: %v", err), http.StatusInternalServerError)
			}
			return
		}
	}
}

// handleSetConfig handles POST requests to set dynamic config values.
// It expects form data with "key" and "value" parameters.
// Only dynamic config values (registered with Dynamic=true) can be modified.
// The value is parsed using the same logic as command-line flags (via pflag).
func handleSetConfig(reg *viperutil.Registry, fs *pflag.FlagSet, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse form: %v", err), http.StatusBadRequest)
		return
	}

	key := r.FormValue("key")
	value := r.FormValue("value")

	if key == "" {
		http.Error(w, "missing 'key' parameter", http.StatusBadRequest)
		return
	}

	// Check if this is a dynamic config value
	if !reg.IsDynamic(key) {
		http.Error(w, fmt.Sprintf("key %s is not a dynamic config value (cannot be modified at runtime)", key), http.StatusBadRequest)
		return
	}

	// Look up the flag to get proper type parsing
	flag := fs.Lookup(key)
	if flag == nil {
		http.Error(w, fmt.Sprintf("unknown config key: %s (no flag defined)", key), http.StatusBadRequest)
		return
	}

	// Use the flag's Value.Set() to parse and update
	// The viper binding (via BindPFlag) will make this visible through Get()
	if err := flag.Value.Set(value); err != nil {
		http.Error(w, fmt.Sprintf("invalid value for %s (%s): %v", key, flag.Value.Type(), err), http.StatusBadRequest)
		return
	}

	// Notify subscribers that a config change has occurred.
	// This wakes up any goroutines waiting for config changes (e.g., recovery loop).
	reg.NotifyConfigChange()

	// Read back the parsed value to return in response
	parsedValue := reg.Combined().Get(key)

	w.Header().Set("Content-Type", "application/json")
	response := map[string]any{
		"key":   key,
		"value": parsedValue,
		"type":  flag.Value.Type(),
	}
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
	}
}
