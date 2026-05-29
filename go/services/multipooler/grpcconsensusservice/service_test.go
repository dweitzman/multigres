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

package grpcconsensusservice

// The consensus gRPC service is a thin wrapper that forwards to manager
// methods. Behavioral tests live alongside those manager methods in
// go/services/multipooler/manager (Recruit, Propose, SetTermPrimary,
// UpdateConsensusRule). This file used to host a Status-RPC test, but
// Status was removed from MultiPoolerConsensus — ConsensusStatus is
// delivered via MultiPoolerManager.Status / ManagerHealthStream instead.
