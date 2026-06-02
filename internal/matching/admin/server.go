package admin

import (
	"context"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/presence"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server 实现 matching.v1.MatchingAdminService。
type Server struct {
	matchingv1.UnimplementedMatchingAdminServiceServer
	Engine *recovery.Engine
}

func (s *Server) GetOrderPresence(ctx context.Context, req *matchingv1.GetOrderPresenceRequest) (*matchingv1.GetOrderPresenceResponse, error) {
	_ = ctx
	if s == nil || s.Engine == nil {
		return nil, status.Error(codes.Unavailable, "matching engine not configured")
	}
	if req == nil || req.GetSymbol() == "" || req.GetOrderId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "symbol and order_id are required")
	}
	return &matchingv1.GetOrderPresenceResponse{
		Presence: toPB(s.Engine.LookupOrderPresence(req.GetSymbol(), req.GetOrderId())),
	}, nil
}

func (s *Server) ReconcileOrders(ctx context.Context, req *matchingv1.ReconcileOrdersRequest) (*matchingv1.ReconcileOrdersResponse, error) {
	_ = ctx
	if s == nil || s.Engine == nil {
		return nil, status.Error(codes.Unavailable, "matching engine not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	results := make([]*matchingv1.OrderPresenceResult, 0, len(req.GetOrders()))
	for _, ref := range req.GetOrders() {
		if ref == nil || ref.GetOrderId() == 0 {
			continue
		}
		results = append(results, &matchingv1.OrderPresenceResult{
			OrderId:  ref.GetOrderId(),
			Presence: toPB(s.Engine.LookupOrderPresence(ref.GetSymbol(), ref.GetOrderId())),
		})
	}
	return &matchingv1.ReconcileOrdersResponse{Results: results}, nil
}

func toPB(k presence.Kind) matchingv1.OrderPresence {
	switch k {
	case presence.InOrderbook:
		return matchingv1.OrderPresence_ORDER_PRESENCE_IN_ORDERBOOK
	case presence.KnownNotInOrderbook:
		return matchingv1.OrderPresence_ORDER_PRESENCE_KNOWN_NOT_IN_ORDERBOOK
	default:
		return matchingv1.OrderPresence_ORDER_PRESENCE_UNKNOWN
	}
}
