package handler

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/order/service"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// BalanceServer 实现 order.v1.BalanceService gRPC。
type BalanceServer struct {
	orderv1.UnimplementedBalanceServiceServer
	Svc *service.BalanceService
}

// GetBalance 查询单资产余额。
func (s *BalanceServer) GetBalance(ctx context.Context, req *orderv1.GetBalanceRequest) (*orderv1.GetBalanceResponse, error) {
	return invokeBalance(ctx, s, req, func() (*orderv1.GetBalanceResponse, error) {
		return s.Svc.GetBalance(ctx, req)
	})
}

// ListBalances 查询全部资产余额。
func (s *BalanceServer) ListBalances(ctx context.Context, req *orderv1.ListBalancesRequest) (*orderv1.ListBalancesResponse, error) {
	return invokeBalance(ctx, s, req, func() (*orderv1.ListBalancesResponse, error) {
		return s.Svc.ListBalances(ctx, req)
	})
}

// UpdateBalance 调整可用余额。
func (s *BalanceServer) UpdateBalance(ctx context.Context, req *orderv1.UpdateBalanceRequest) (*orderv1.UpdateBalanceResponse, error) {
	return invokeBalance(ctx, s, req, func() (*orderv1.UpdateBalanceResponse, error) {
		return s.Svc.UpdateBalance(ctx, req)
	})
}

func invokeBalance[T any](ctx context.Context, s *BalanceServer, req any, fn func() (T, error)) (T, error) {
	var zero T
	if s == nil || s.Svc == nil {
		return zero, status.Error(codes.Internal, "balance service unavailable")
	}
	if req == nil {
		return zero, status.Error(codes.InvalidArgument, "request is nil")
	}
	resp, err := fn()
	if err != nil {
		return zero, mapBalanceError(err)
	}
	return resp, nil
}

func mapBalanceError(err error) error {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, repository.ErrInsufficientBalance):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
