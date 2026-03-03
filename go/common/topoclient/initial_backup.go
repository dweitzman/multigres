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

package topoclient

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const InitialBackupFile = "InitialBackup"

// pathForInitialBackup returns the etcd path for the canonical bootstrap
// backup of a shard: databases/{db}/{tablegroup}/{shard}/InitialBackup
func pathForInitialBackup(shardKey types.ShardKey) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s",
		DatabasesPath, shardKey.Database, shardKey.TableGroup, shardKey.Shard,
		InitialBackupFile)
}

// ClaimInitialBackup atomically registers the canonical bootstrap backup for
// a shard. Uses conn.Create() which is create-if-not-exists. Returns
// (true, nil) when this caller wins, (false, nil) when another pooler already
// claimed the key (NodeExists). All other failures return a non-nil error.
func (ts *store) ClaimInitialBackup(ctx context.Context, shardKey types.ShardKey, backupID string) (bool, error) {
	record := &clustermetadatapb.InitialBackup{BackupId: backupID}
	contents, err := proto.Marshal(record)
	if err != nil {
		return false, err
	}

	filePath := pathForInitialBackup(shardKey)
	_, err = ts.globalTopo.Create(ctx, filePath, contents)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, &TopoError{Code: NodeExists}) {
		return false, nil
	}
	return false, err
}

// GetInitialBackup returns the canonical bootstrap backup for a shard, or nil
// if no claim has been registered yet.
func (ts *store) GetInitialBackup(ctx context.Context, shardKey types.ShardKey) (*clustermetadatapb.InitialBackup, error) {
	filePath := pathForInitialBackup(shardKey)
	contents, _, err := ts.globalTopo.Get(ctx, filePath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			return nil, nil
		}
		return nil, err
	}

	record := &clustermetadatapb.InitialBackup{}
	if err := proto.Unmarshal(contents, record); err != nil {
		return nil, err
	}
	return record, nil
}
