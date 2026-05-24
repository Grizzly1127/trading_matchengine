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

// OrderStore 订单持久化接口（便于测试）。
type OrderStore interface {
	FindByClientOrderID(ctx context.Context, userID uint64, clientOrderID string) (*repository.Order, error)
	InsertPending(ctx context.Context, in repository.InsertPendingInput) (*repository.Order, error)
}

// Service 订单业务逻辑。
type Service struct {
	Repo        OrderStore
	OutboxTopic string
}

// PlaceOrder 落库 PENDING 订单并在同事务写入 order_outbox；Kafka 由 Relay 异步投递。
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
		return nil, err
	}

	return toPlaceOrderResponse(order, false), nil
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
