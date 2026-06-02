package handler

import (
	"context"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdminServer 实现 order.v1.OrderAdminService（内网对账）。
type AdminServer struct {
	orderv1.UnimplementedOrderAdminServiceServer
	Repo *repository.Repository
}

// ListReconcileOrders 返回 PENDING/PARTIAL 订单。
func (s *AdminServer) ListReconcileOrders(ctx context.Context, req *orderv1.ListReconcileOrdersRequest) (*orderv1.ListReconcileOrdersResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, status.Error(codes.Unavailable, "repository not configured")
	}
	symbol := strings.TrimSpace(req.GetSymbol())
	if symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	rows, err := s.Repo.ListReconcileOrders(ctx, symbol)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list reconcile orders: %v", err)
	}
	out := make([]*orderv1.ReconcileOrder, 0, len(rows))
	for _, row := range rows {
		out = append(out, &orderv1.ReconcileOrder{
			OrderId: row.OrderID,
			Status:  row.Status,
		})
	}
	return &orderv1.ListReconcileOrdersResponse{Orders: out}, nil
}

// GetOrderStatuses 批量查询订单状态。
func (s *AdminServer) GetOrderStatuses(ctx context.Context, req *orderv1.GetOrderStatusesRequest) (*orderv1.GetOrderStatusesResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, status.Error(codes.Unavailable, "repository not configured")
	}
	symbol := strings.TrimSpace(req.GetSymbol())
	if symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	statuses, err := s.Repo.GetOrderStatuses(ctx, symbol, req.GetOrderIds())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get order statuses: %v", err)
	}
	out := make([]*orderv1.OrderStatusRef, 0, len(req.GetOrderIds()))
	for _, id := range req.GetOrderIds() {
		st, ok := statuses[id]
		ref := &orderv1.OrderStatusRef{OrderId: id, Found: ok}
		if ok {
			ref.Status = st
		}
		out = append(out, ref)
	}
	return &orderv1.GetOrderStatusesResponse{Orders: out}, nil
}
