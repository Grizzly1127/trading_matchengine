package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// ErrInvalidArgument 表示客户端参数错误。
var ErrInvalidArgument = errors.New("invalid argument")

// ErrNotFound 表示资源不存在。
var ErrNotFound = errors.New("not found")

// ErrFailedPrecondition 表示业务前置条件不满足。
var ErrFailedPrecondition = errors.New("failed precondition")
var ErrUnavailable = errors.New("unavailable")

type ReferencePriceClient interface {
	GetReferencePrice(ctx context.Context, symbol string) (string, error)
}

// OrderStore 订单持久化接口（便于测试）。
type OrderStore interface {
	FindByClientOrderID(ctx context.Context, userID uint64, clientOrderID string) (*repository.Order, error)
	InsertPending(ctx context.Context, in repository.InsertPendingInput) (*repository.Order, error)
	GetOrderByUser(ctx context.Context, userID, orderID uint64) (*repository.Order, error)
	BeginCancel(ctx context.Context, userID, orderID uint64, outboxTopic string) (*repository.Order, error)
	ListOrders(ctx context.Context, filter repository.ListOrdersFilter) ([]repository.Order, error)
}

// OrderService 订单业务逻辑。
type OrderService struct {
	Repo           OrderStore
	OutboxTopic    string
	MarketData     ReferencePriceClient
	SlippageBuffer decimal.Decimal
	Symbols        *symbolrules.Registry
}

func (s *OrderService) symbolRules(symbolName string) (symbolrules.Spec, error) {
	if s != nil && s.Symbols != nil {
		return s.Symbols.Lookup(symbolName)
	}
	reg, err := symbolrules.DefaultRegistry()
	if err != nil {
		return symbolrules.Spec{}, err
	}
	return reg.Lookup(symbolName)
}

// PlaceOrder 落库 PENDING、冻结余额并写入 order_outbox。
func (s *OrderService) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("order service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}

	in, err := s.validatePlaceOrder(ctx, req)
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
func (s *OrderService) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
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
func (s *OrderService) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
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

// ListOrders 查询订单列表（筛选条件均可选，仅 user_id 必填）。
func (s *OrderService) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("order service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}
	if req.GetUserId() == 0 {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}

	filter, err := buildListOrdersFilter(req)
	if err != nil {
		return nil, err
	}

	orders, err := s.Repo.ListOrders(ctx, filter)
	if err != nil {
		return nil, err
	}

	out := make([]*orderv1.OrderInfo, 0, len(orders))
	for i := range orders {
		out = append(out, toOrderInfoPB(&orders[i]))
	}
	return &orderv1.ListOrdersResponse{Orders: out}, nil
}

func buildListOrdersFilter(req *orderv1.ListOrdersRequest) (repository.ListOrdersFilter, error) {
	filter := repository.ListOrdersFilter{
		UserID:   req.GetUserId(),
		Symbol:   strings.TrimSpace(req.GetSymbol()),
		Status:   strings.TrimSpace(strings.ToUpper(req.GetStatus())),
		Page:     int(req.GetPage()),
		PageSize: int(req.GetPageSize()),
	}

	side := req.GetSide()
	if side != commonv1.Side_SIDE_UNSPECIFIED {
		if side != commonv1.Side_SIDE_BUY && side != commonv1.Side_SIDE_SELL {
			return filter, fmt.Errorf("%w: invalid side filter", ErrInvalidArgument)
		}
		filter.Side = int16(side)
	}

	typ := req.GetType()
	if typ != commonv1.OrderType_ORDER_TYPE_UNSPECIFIED {
		if typ != commonv1.OrderType_ORDER_TYPE_LIMIT && typ != commonv1.OrderType_ORDER_TYPE_MARKET {
			return filter, fmt.Errorf("%w: invalid type filter", ErrInvalidArgument)
		}
		filter.OrderType = int16(typ)
	}

	if filter.Status != "" && !isValidOrderStatus(filter.Status) {
		return filter, fmt.Errorf("%w: invalid status filter", ErrInvalidArgument)
	}

	if req.GetCreatedAtFrom() != nil {
		t := req.GetCreatedAtFrom().AsTime()
		filter.CreatedAtFrom = &t
	}
	if req.GetCreatedAtTo() != nil {
		t := req.GetCreatedAtTo().AsTime()
		filter.CreatedAtTo = &t
	}
	if filter.CreatedAtFrom != nil && filter.CreatedAtTo != nil && filter.CreatedAtFrom.After(*filter.CreatedAtTo) {
		return filter, fmt.Errorf("%w: created_at_from must be before created_at_to", ErrInvalidArgument)
	}

	return filter, nil
}

func isValidOrderStatus(status string) bool {
	switch status {
	case "PENDING", "ACCEPTED", "PARTIAL", "CANCELING", "FILLED", "CANCELED", "REJECTED":
		return true
	default:
		return false
	}
}

func (s *OrderService) validatePlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (repository.InsertPendingInput, error) {
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

	spec, err := s.symbolRules(symbol)
	if err != nil {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}

	qtyStr := strings.TrimSpace(req.GetQuantity().GetValue())
	qty, err := decimal.NewFromString(qtyStr)
	if err != nil || !qty.IsPositive() {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: quantity must be positive decimal", ErrInvalidArgument)
	}
	qtyNorm, err := spec.ValidateQuantity(qty)
	if err != nil {
		return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	qty, _ = decimal.NewFromString(qtyNorm)

	var pricePtr *string
	var freezePricePtr *string
	var freezeSlippagePtr *string
	var frozenAmountPtr *string
	if typ == commonv1.OrderType_ORDER_TYPE_LIMIT {
		priceStr := strings.TrimSpace(req.GetPrice().GetValue())
		price, err := decimal.NewFromString(priceStr)
		if err != nil || !price.IsPositive() {
			return repository.InsertPendingInput{}, fmt.Errorf("%w: price must be positive decimal for LIMIT", ErrInvalidArgument)
		}
		priceNorm, err := spec.ValidatePrice(price)
		if err != nil {
			return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
		price, _ = decimal.NewFromString(priceNorm)
		if err := spec.CheckMinNotional(price, qty); err != nil {
			return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
		pricePtr = &priceNorm
		if side == commonv1.Side_SIDE_BUY {
			amt := price.Mul(qty).String()
			frozenAmountPtr = &amt
			freezePricePtr = &priceNorm
		}
	} else if side == commonv1.Side_SIDE_BUY {
		// 市价买：优先使用用户保护价；未提供时调用 Market Data 参考价。
		var freezePrice decimal.Decimal
		if req.GetPrice() != nil && strings.TrimSpace(req.GetPrice().GetValue()) != "" {
			p, err := decimal.NewFromString(strings.TrimSpace(req.GetPrice().GetValue()))
			if err != nil || !p.IsPositive() {
				return repository.InsertPendingInput{}, fmt.Errorf("%w: invalid price", ErrInvalidArgument)
			}
			priceNorm, err := spec.ValidatePrice(p)
			if err != nil {
				return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
			}
			freezePrice, _ = decimal.NewFromString(priceNorm)
		} else {
			if s == nil || s.MarketData == nil {
				return repository.InsertPendingInput{}, fmt.Errorf("%w: market data unavailable", ErrUnavailable)
			}
			ref, err := s.MarketData.GetReferencePrice(ctx, symbol)
			if err != nil {
				return repository.InsertPendingInput{}, fmt.Errorf("%w: market data reference price: %v", ErrUnavailable, err)
			}
			p, err := decimal.NewFromString(strings.TrimSpace(ref))
			if err != nil || !p.IsPositive() {
				return repository.InsertPendingInput{}, fmt.Errorf("%w: invalid market data reference price", ErrUnavailable)
			}
			priceNorm, err := spec.CeilPrice(p)
			if err != nil {
				return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
			}
			freezePrice, _ = decimal.NewFromString(priceNorm)
		}
		if err := spec.CheckMinNotional(freezePrice, qty); err != nil {
			return repository.InsertPendingInput{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
		slippage := s.SlippageBuffer
		if slippage.IsNegative() {
			slippage = decimal.Zero
		}
		freezeAmount := freezePrice.Mul(qty).Mul(decimal.NewFromInt(1).Add(slippage))
		fp := freezePrice.String()
		fs := slippage.String()
		fa := freezeAmount.String()
		freezePricePtr = &fp
		freezeSlippagePtr = &fs
		frozenAmountPtr = &fa
	}

	return repository.InsertPendingInput{
		UserID:         req.GetUserId(),
		ClientOrderID:  clientOrderID,
		Symbol:         symbol,
		Side:           int16(side),
		OrderType:      int16(typ),
		Price:          pricePtr,
		FreezePrice:    freezePricePtr,
		FreezeSlippage: freezeSlippagePtr,
		FrozenAmount:   frozenAmountPtr,
		Quantity:       qtyNorm,
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
	return &orderv1.GetOrderResponse{
		Order: toOrderInfoPB(order),
	}
}

func toOrderInfoPB(order *repository.Order) *orderv1.OrderInfo {
	orderInfo := &orderv1.OrderInfo{
		OrderId:        order.ID,
		ClientOrderId:  order.ClientOrderID,
		Symbol:         order.Symbol,
		Side:           commonv1.Side(order.Side),
		Type:           commonv1.OrderType(order.OrderType),
		Quantity:       &commonv1.Decimal{Value: order.Quantity},
		FilledQuantity: &commonv1.Decimal{Value: order.FilledQuantity},
		Status:         order.Status,
		CreatedAt:      timestamppb.New(order.CreatedAt),
		UpdatedAt:      timestamppb.New(order.UpdatedAt),
	}
	if order.Price != nil {
		orderInfo.Price = &commonv1.Decimal{Value: *order.Price}
	}
	return orderInfo
}
