package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// ErrInvalidArgument 表示客户端参数错误。
var ErrInvalidArgument = errors.New("invalid argument")

// ErrNotFound 表示资源不存在。
var ErrNotFound = errors.New("not found")

// ErrFailedPrecondition 表示业务前置条件不满足。
var ErrFailedPrecondition = errors.New("failed precondition")

// OrderStore 订单持久化接口（便于测试）。
type OrderStore interface {
	FindByClientOrderID(ctx context.Context, userID uint64, clientOrderID string) (*repository.Order, error)
	InsertPending(ctx context.Context, in repository.InsertPendingInput) (*repository.Order, error)
	GetOrderByUser(ctx context.Context, userID, orderID uint64) (*repository.Order, error)
	BeginCancel(ctx context.Context, userID, orderID uint64, outboxTopic string) (*repository.Order, error)
}

// Service 订单业务逻辑。
type Service struct {
	Repo        OrderStore
	OutboxTopic string
}

// PlaceOrder 落库 PENDING、冻结余额并写入 order_outbox。
func (s *Service) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("order service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}

	in, err := validatePlaceOrder(req)
	if err != nil {
		return nil, err
	}
	in.OutboxTopic = s.OutboxTopic

	existing, err := s.Repo.FindByClientOrderID(ctx, in.UserID, in.ClientOrderID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return toPlaceOrderResponse(existing, true), nil
	}

	order, err := s.Repo.InsertPending(ctx, in)
	if err != nil {
		if errors.Is(err, repository.ErrInsufficientBalance) {
			return nil, fmt.Errorf("%w: %v", ErrFailedPrecondition, err)
		}
		return nil, err
	}

	return toPlaceOrderResponse(order, false), nil
}

// CancelOrder 将订单置为 CANCELING 并写入撤单 Outbox。
func (s *Service) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("order service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}
	if req.GetUserId() == 0 {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	if req.GetOrderId() == 0 {
		return nil, fmt.Errorf("%w: order_id is required", ErrInvalidArgument)
	}

	order, err := s.Repo.BeginCancel(ctx, req.GetUserId(), req.GetOrderId(), s.OutboxTopic)
	if err != nil {
		if errors.Is(err, repository.ErrOrderNotFound) {
			return nil, fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		if errors.Is(err, repository.ErrOrderNotCancelable) {
			return nil, fmt.Errorf("%w: %v", ErrFailedPrecondition, err)
		}
		return nil, err
	}

	return &orderv1.CancelOrderResponse{
		OrderId: order.ID,
		Symbol:  order.Symbol,
		Status:  order.Status,
	}, nil
}

// GetOrder 查询订单详情。
func (s *Service) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("order service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}
	if req.GetUserId() == 0 {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	if req.GetOrderId() == 0 {
		return nil, fmt.Errorf("%w: order_id is required", ErrInvalidArgument)
	}

	order, err := s.Repo.GetOrderByUser(ctx, req.GetUserId(), req.GetOrderId())
	if err != nil {
		if errors.Is(err, repository.ErrOrderNotFound) {
			return nil, fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		return nil, err
	}
	return toGetOrderResponse(order), nil
}

func validatePlaceOrder(req *orderv1.PlaceOrderRequest) (repository.InsertPendingInput, error) {
	if req.GetUserId() == 0 {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}

	clientOrderID := strings.TrimSpace(req.GetClientOrderId())
	if clientOrderID == "" {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: client_order_id is required", ErrInvalidArgument)
	}
	if len(clientOrderID) > 64 {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: client_order_id too long", ErrInvalidArgument)
	}

	symbol := strings.TrimSpace(req.GetSymbol())
	if symbol == "" {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: symbol is required", ErrInvalidArgument)
	}

	side := req.GetSide()
	if side != commonv1.Side_SIDE_BUY && side != commonv1.Side_SIDE_SELL {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: side must be BUY or SELL", ErrInvalidArgument)
	}

	typ := req.GetType()
	if typ != commonv1.OrderType_ORDER_TYPE_LIMIT && typ != commonv1.OrderType_ORDER_TYPE_MARKET {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: type must be LIMIT or MARKET", ErrInvalidArgument)
	}

	qtyStr := strings.TrimSpace(req.GetQuantity().GetValue())
	qty, err := decimal.NewFromString(qtyStr)
	if err != nil || !qty.IsPositive() {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: quantity must be positive decimal", ErrInvalidArgument)
	}

	var pricePtr *string
	if typ == commonv1.OrderType_ORDER_TYPE_LIMIT {
		priceStr := strings.TrimSpace(req.GetPrice().GetValue())
		price, err := decimal.NewFromString(priceStr)
		if err != nil || !price.IsPositive() {
			return repository.InsertPendingInput{}, fmt.Errorf("%w: price must be positive decimal for LIMIT", ErrInvalidArgument)
		}
		s := price.String()
		pricePtr = &s
	} else if side == commonv1.Side_SIDE_BUY {
		// 临时：市价买必须带 price 作保护价。方案 C（行情估算）见 docs/design/market-buy-freeze.md。
		return repository.InsertPendingInput{}, fmt.Errorf("%w: MARKET buy requires price for balance freeze", ErrInvalidArgument)
	}

	return repository.InsertPendingInput{
		UserID:        req.GetUserId(),
		ClientOrderID: clientOrderID,
		Symbol:        symbol,
		Side:          int16(side),
		OrderType:     int16(typ),
		Price:         pricePtr,
		Quantity:      qty.String(),
	}, nil
}

func toPlaceOrderResponse(order *repository.Order, idempotentHit bool) *orderv1.PlaceOrderResponse {
	return &orderv1.PlaceOrderResponse{
		OrderId:       order.ID,
		ClientOrderId: order.ClientOrderID,
		Symbol:        order.Symbol,
		Status:        order.Status,
		CreatedAt:     timestamppb.New(order.CreatedAt),
		IdempotentHit: idempotentHit,
	}
}

func toGetOrderResponse(order *repository.Order) *orderv1.GetOrderResponse {
	resp := &orderv1.GetOrderResponse{
		OrderId:       order.ID,
		ClientOrderId: order.ClientOrderID,
		Symbol:        order.Symbol,
		Side:          commonv1.Side(order.Side),
		Type:          commonv1.OrderType(order.OrderType),
		Quantity:      &commonv1.Decimal{Value: order.Quantity},
		FilledQuantity: &commonv1.Decimal{Value: order.FilledQuantity},
		Status:        order.Status,
		CreatedAt:     timestamppb.New(order.CreatedAt),
		UpdatedAt:     timestamppb.New(order.UpdatedAt),
	}
	if order.Price != nil {
		resp.Price = &commonv1.Decimal{Value: *order.Price}
	}
	return resp
}
