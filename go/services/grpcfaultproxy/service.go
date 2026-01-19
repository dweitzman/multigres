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

package grpcfaultproxy

import (
	"context"

	pb "github.com/multigres/multigres/go/pb/grpcfaultproxyservice"
)

// Service implements the GrpcFaultProxyService gRPC API for controlling fault injection.
type Service struct {
	pb.UnimplementedGrpcFaultProxyServiceServer
	proxy *Proxy
}

// NewService creates a new GrpcFaultProxyService implementation.
func NewService(proxy *Proxy) *Service {
	return &Service{proxy: proxy}
}

// SetRules replaces all fault injection rules with the provided rules.
func (s *Service) SetRules(ctx context.Context, req *pb.SetRulesRequest) (*pb.SetRulesResponse, error) {
	s.proxy.logger.InfoContext(ctx, "SetRules called", "num_rules", len(req.Rules))

	// Convert protobuf rules to internal FaultRule format
	rules := make([]FaultRule, len(req.Rules))
	for i, pbRule := range req.Rules {
		rules[i] = FaultRule{
			Name:        pbRule.Name,
			Source:      pbRule.Source,
			Target:      pbRule.Target,
			Method:      pbRule.Method,
			FaultType:   pbRule.FaultType,
			Probability: pbRule.Probability,
			LatencyMs:   int(pbRule.LatencyMs),
			ErrorCode:   int(pbRule.ErrorCode),
			ErrorMsg:    pbRule.ErrorMsg,
		}
	}

	// Update engine rules
	s.proxy.engine.UpdateRules(rules)

	s.proxy.logger.InfoContext(ctx, "fault injection rules updated",
		"rules_count", len(rules))

	return &pb.SetRulesResponse{
		RulesCount: int32(len(rules)),
	}, nil
}

// GetRules returns the currently active fault injection rules.
func (s *Service) GetRules(ctx context.Context, req *pb.GetRulesRequest) (*pb.GetRulesResponse, error) {
	rules := s.proxy.engine.GetRules()

	// Convert internal FaultRule format to protobuf
	pbRules := make([]*pb.FaultRule, len(rules))
	for i, rule := range rules {
		pbRules[i] = &pb.FaultRule{
			Name:        rule.Name,
			Source:      rule.Source,
			Target:      rule.Target,
			Method:      rule.Method,
			FaultType:   rule.FaultType,
			Probability: rule.Probability,
			LatencyMs:   int32(rule.LatencyMs),
			ErrorCode:   int32(rule.ErrorCode),
			ErrorMsg:    rule.ErrorMsg,
		}
	}

	return &pb.GetRulesResponse{
		Rules: pbRules,
	}, nil
}
