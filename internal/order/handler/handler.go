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

// PlaceOrder 创建订单。
func (s *Server) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	return invoke(s, req, func() (*orderv1.PlaceOrderResponse, error) {
		return s.Svc.PlaceOrder(ctx, req)
	})
}

// CancelOrder 撤销订单。
func (s *Server) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	return invoke(s, req, func() (*orderv1.CancelOrderResponse, error) {
		return s.Svc.CancelOrder(ctx, req)
	})
}

// GetOrder 查询订单。
func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	return invoke(s, req, func() (*orderv1.GetOrderResponse, error) {
		return s.Svc.GetOrder(ctx, req)
	})
}

// ListOrders 查询订单列表。
func (s *Server) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	return invoke(s, req, func() (*orderv1.ListOrdersResponse, error) {
		return s.Svc.ListOrders(ctx, req)
	})
}

func invoke[T any](s *Server, req any, fn func() (T, error)) (T, error) {
	var zero T
	if s == nil || s.Svc == nil {
		return zero, status.Error(codes.Internal, "service unavailable")
	}
	if req == nil {
		return zero, status.Error(codes.InvalidArgument, "request is nil")
	}
	resp, err := fn()
	if err != nil {
		return zero, mapError(err)
	}
	return resp, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
