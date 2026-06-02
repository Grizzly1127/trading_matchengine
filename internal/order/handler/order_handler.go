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
type OrderServer struct {
	orderv1.UnimplementedOrderServiceServer
	Svc *service.OrderService
}

// PlaceOrder 创建订单。
func (s *OrderServer) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	return invokeOrder(s, req, func() (*orderv1.PlaceOrderResponse, error) {
		return s.Svc.PlaceOrder(ctx, req)
	})
}

// CancelOrder 撤销订单。
func (s *OrderServer) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	return invokeOrder(s, req, func() (*orderv1.CancelOrderResponse, error) {
		return s.Svc.CancelOrder(ctx, req)
	})
}

// GetOrder 查询订单。
func (s *OrderServer) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	return invokeOrder(s, req, func() (*orderv1.GetOrderResponse, error) {
		return s.Svc.GetOrder(ctx, req)
	})
}

// ListOrders 查询订单列表。
func (s *OrderServer) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	return invokeOrder(s, req, func() (*orderv1.ListOrdersResponse, error) {
		return s.Svc.ListOrders(ctx, req)
	})
}

// ListTrades 查询成交列表。
func (s *OrderServer) ListTrades(ctx context.Context, req *orderv1.ListTradesRequest) (*orderv1.ListTradesResponse, error) {
	return invokeOrder(s, req, func() (*orderv1.ListTradesResponse, error) {
		return s.Svc.ListTrades(ctx, req)
	})
}

func invokeOrder[T any](s *OrderServer, req any, fn func() (T, error)) (T, error) {
	var zero T
	if s == nil || s.Svc == nil {
		return zero, status.Error(codes.Internal, "service unavailable")
	}
	if req == nil {
		return zero, status.Error(codes.InvalidArgument, "request is nil")
	}
	resp, err := fn()
	if err != nil {
		return zero, mapOrderError(err)
	}
	return resp, nil
}

func mapOrderError(err error) error {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, service.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
