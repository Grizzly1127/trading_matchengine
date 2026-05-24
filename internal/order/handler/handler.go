package handler

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Grizzly1127/trading_matchengine/internal/order/service"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// Server 实现 order.v1.OrderService gRPC。
type Server struct {
	orderv1.UnimplementedOrderServiceServer
	Svc *service.Service
}

// PlaceOrder 创建订单并发布撮合命令。
func (s *Server) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	if s == nil || s.Svc == nil {
		return nil, status.Error(codes.Internal, "service unavailable")
	}
	resp, err := s.Svc.PlaceOrder(ctx, req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}
