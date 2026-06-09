// Copyright 2026 Supabase, Inc.
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

package pgquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestParseAndRedactPrimaryConnInfo(t *testing.T) {
	tests := []struct {
		name        string
		connInfo    string
		expected    *multipoolermanagerdatapb.PrimaryConnInfo
		expectError bool
	}{
		{
			name:     "Complete connection string",
			connInfo: "host=localhost port=5432 user=postgres application_name=test_cell_standby1",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "test_cell_standby1",
				Raw:             "host=localhost port=5432 user=postgres application_name=test_cell_standby1",
			},
		},
		{
			name:     "Missing application_name",
			connInfo: "host=primary.example.com port=5433 user=replicator",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "primary.example.com",
				Port:            5433,
				User:            "replicator",
				ApplicationName: "",
				Raw:             "host=primary.example.com port=5433 user=replicator",
			},
		},
		{
			name:     "Missing port",
			connInfo: "host=localhost user=postgres application_name=test_app",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            0,
				User:            "postgres",
				ApplicationName: "test_app",
				Raw:             "host=localhost user=postgres application_name=test_app",
			},
		},
		{
			name:     "Empty string",
			connInfo: "",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "",
				Port:            0,
				User:            "",
				ApplicationName: "",
				Raw:             "",
			},
		},
		{
			name:     "Extra parameters ignored",
			connInfo: "host=localhost port=5432 user=postgres application_name=test keepalives_idle=30 keepalives_interval=10",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "test",
				Raw:             "host=localhost port=5432 user=postgres application_name=test keepalives_idle=30 keepalives_interval=10",
			},
		},
		{
			name:     "Invalid port ignored",
			connInfo: "host=localhost port=invalid user=postgres",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            0,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=invalid user=postgres",
			},
		},
		{
			name:     "Connection with sslmode",
			connInfo: "host=localhost port=5432 user=postgres sslmode=require",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=require",
			},
		},
		{
			name:     "Connection with password (redacted)",
			connInfo: "host=localhost port=5432 user=postgres password=secret123",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres password=[REDACTED]",
			},
		},
		{
			name:     "Connection with passfile",
			connInfo: "host=localhost port=5432 user=postgres passfile=/home/user/.pgpass",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres passfile=/home/user/.pgpass",
			},
		},
		{
			name:     "Connection with multiple SSL parameters",
			connInfo: "host=localhost port=5432 user=postgres sslmode=verify-full sslcert=/path/to/cert.pem sslkey=/path/to/key.pem sslrootcert=/path/to/ca.pem",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=verify-full sslcert=/path/to/cert.pem sslkey=/path/to/key.pem sslrootcert=/path/to/ca.pem",
			},
		},
		{
			name:     "Connection with keepalive and timeout parameters",
			connInfo: "host=localhost port=5432 user=postgres keepalives_idle=30 keepalives_interval=10 keepalives_count=5 connect_timeout=10",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres keepalives_idle=30 keepalives_interval=10 keepalives_count=5 connect_timeout=10",
			},
		},
		{
			name:     "Connection with channel_binding",
			connInfo: "host=localhost port=5432 user=postgres channel_binding=require",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres channel_binding=require",
			},
		},
		{
			name:     "Connection with gssencmode",
			connInfo: "host=localhost port=5432 user=postgres gssencmode=prefer",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres gssencmode=prefer",
			},
		},
		{
			name:     "Complex connection with all parsed and unparsed fields",
			connInfo: "host=primary.db.local port=5433 user=replicator application_name=zone1_standby2 sslmode=require keepalives_idle=60 connect_timeout=30",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "primary.db.local",
				Port:            5433,
				User:            "replicator",
				ApplicationName: "zone1_standby2",
				Raw:             "host=primary.db.local port=5433 user=replicator application_name=zone1_standby2 sslmode=require keepalives_idle=60 connect_timeout=30",
			},
		},
		{
			name:     "Connection with hostaddr",
			connInfo: "hostaddr=172.28.40.9 port=5432 user=postgres",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "hostaddr=172.28.40.9 port=5432 user=postgres",
			},
		},
		{
			name:     "Connection with dbname",
			connInfo: "host=localhost port=5432 dbname=mydb user=postgres",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 dbname=mydb user=postgres",
			},
		},
		{
			name:     "Connection with client_encoding",
			connInfo: "host=localhost port=5432 user=postgres client_encoding=UTF8",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres client_encoding=UTF8",
			},
		},
		{
			name:     "Connection with options parameter",
			connInfo: "host=localhost port=5432 user=postgres options=-c\\ geqo=off",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres options=-c\\ geqo=off",
			},
		},
		{
			name:     "Connection with replication mode",
			connInfo: "host=localhost port=5432 user=postgres replication=database",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres replication=database",
			},
		},
		{
			name:     "Connection with target_session_attrs",
			connInfo: "host=localhost port=5432 user=postgres target_session_attrs=read-write",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres target_session_attrs=read-write",
			},
		},
		{
			name:     "Connection with sslcrl and sslcompression",
			connInfo: "host=localhost port=5432 user=postgres sslmode=verify-full sslcrl=/path/to/crl.pem sslcompression=1",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=verify-full sslcrl=/path/to/crl.pem sslcompression=1",
			},
		},
		{
			name:     "Connection with requirepeer",
			connInfo: "host=localhost port=5432 user=postgres requirepeer=postgres",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres requirepeer=postgres",
			},
		},
		{
			name:     "Connection with krbsrvname and gsslib",
			connInfo: "host=localhost port=5432 user=postgres krbsrvname=postgres gsslib=gssapi",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres krbsrvname=postgres gsslib=gssapi",
			},
		},
		{
			name:     "Connection with service",
			connInfo: "service=myservice",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "",
				Port:            0,
				User:            "",
				ApplicationName: "",
				Raw:             "service=myservice",
			},
		},
		{
			name:     "Comprehensive connection with password redaction",
			connInfo: "host=prod.db.com port=5433 user=repl_user password=SuperSecret123 application_name=standby1 sslmode=verify-full sslcert=/certs/client.pem sslkey=/certs/client.key keepalives_idle=30 connect_timeout=10",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "prod.db.com",
				Port:            5433,
				User:            "repl_user",
				ApplicationName: "standby1",
				Raw:             "host=prod.db.com port=5433 user=repl_user password=[REDACTED] application_name=standby1 sslmode=verify-full sslcert=/certs/client.pem sslkey=/certs/client.key keepalives_idle=30 connect_timeout=10",
			},
		},
		{
			name:        "Invalid format - space-separated without equals",
			connInfo:    "host localhost port 5432",
			expectError: true,
		},
		{
			name:        "Invalid format - missing equals sign in one parameter",
			connInfo:    "host=localhost port 5432 user=postgres",
			expectError: true,
		},
		{
			name:        "Invalid format - empty key",
			connInfo:    "host=localhost =5432 user=postgres",
			expectError: true,
		},
		{
			name:     "Connection with multiple spaces between parameters",
			connInfo: "host=localhost   port=5432  user=postgres",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres", // Spaces normalized due to split/join
			},
		},
		{
			name:     "Connection with leading and trailing spaces",
			connInfo: "  host=localhost port=5432 user=postgres  ",
			expected: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres", // Leading/trailing spaces trimmed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseAndRedactPrimaryConnInfo(tt.connInfo)

			if tt.expectError {
				// Expect parsing to fail for invalid formats
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				// Expect parsing to succeed
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Host, result.Host, "Host should match")
				assert.Equal(t, tt.expected.Port, result.Port, "Port should match")
				assert.Equal(t, tt.expected.User, result.User, "User should match")
				assert.Equal(t, tt.expected.ApplicationName, result.ApplicationName, "ApplicationName should match")
				assert.Equal(t, tt.expected.Raw, result.Raw, "Raw should match")
			}
		})
	}
}
