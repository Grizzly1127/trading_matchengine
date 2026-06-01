package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

type fakeStore struct {
	byClient map[string]*repository.Order
	byID     map[uint64]*repository.Order
	nextID   uint64
	lastIn   repository.InsertPendingInput
}

func (f *fakeStore) FindByClientOrderID(_ context.Context, userID uint64, clientOrderID string) (*repository.Order, error) {
	key := idemKey(userID, clientOrderID)
	if o, ok := f.byClient[key]; ok {
		return o, nil
	}
	return nil, nil
}

func (f *fakeStore) InsertPending(_ context.Context, in repository.InsertPendingInput) (*repository.Order, error) {
	f.lastIn = in
	f.nextID++
	o := &repository.Order{
		ID:            f.nextID,
		UserID:        in.UserID,
		ClientOrderID: in.ClientOrderID,
		Symbol:        in.Symbol,
		Side:          in.Side,
		OrderType:     in.OrderType,
		Price:         in.Price,
		Quantity:      in.Quantity,
		Status:        "PENDING",
	}
	f.byClient[idemKey(in.UserID, in.ClientOrderID)] = o
	f.byID[o.ID] = o
	return o, nil
}

type fakeMarketData struct {
	price string
	err   error
}

func (f *fakeMarketData) GetReferencePrice(_ context.Context, _ string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.price, nil
}

func (f *fakeStore) GetOrderByUser(_ context.Context, userID, orderID uint64) (*repository.Order, error) {
	o, ok := f.byID[orderID]
	if !ok || o.UserID != userID {
		return nil, repository.ErrOrderNotFound
	}
	return o, nil
}

func (f *fakeStore) ListOrders(_ context.Context, filter repository.ListOrdersFilter) ([]repository.Order, error) {
	var out []repository.Order
	for _, o := range f.byID {
		if o.UserID != filter.UserID {
			continue
		}
		if filter.Symbol != "" && o.Symbol != filter.Symbol {
			continue
		}
		if filter.Side != 0 && o.Side != filter.Side {
			continue
		}
		if filter.OrderType != 0 && o.OrderType != filter.OrderType {
			continue
		}
		if filter.Status != "" && o.Status != filter.Status {
			continue
		}
		out = append(out, *o)
	}
	return out, nil
}

func (f *fakeStore) ListTrades(_ context.Context, _ repository.ListTradesFilter) ([]repository.Trade, error) {
	return nil, nil
}

func (f *fakeStore) BeginCancel(_ context.Context, userID, orderID uint64, _ string) (*repository.Order, error) {
	o, ok := f.byID[orderID]
	if !ok || o.UserID != userID {
		return nil, repository.ErrOrderNotFound
	}
	if o.Status == "FILLED" {
		return nil, repository.ErrOrderNotCancelable
	}
	o.Status = "CANCELING"
	return o, nil
}

func idemKey(userID uint64, clientOrderID string) string {
	return fmt.Sprintf("%d:%s", userID, clientOrderID)
}

func TestPlaceOrder_Success(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)}
	svc := &OrderService{Repo: store, OutboxTopic: "order.commands", SlippageBuffer: decimal.Zero}

	req := &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "c1",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "100"},
		Quantity:      &commonv1.Decimal{Value: "1"},
	}

	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetOrderId() != 1 {
		t.Fatalf("order_id=%d want 1", resp.GetOrderId())
	}
	if resp.GetStatus() != "PENDING" {
		t.Fatalf("status=%q want PENDING", resp.GetStatus())
	}
}

func TestPlaceOrder_Idempotent(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)}
	svc := &OrderService{Repo: store, OutboxTopic: "order.commands", SlippageBuffer: decimal.Zero}

	req := &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "c1",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "100"},
		Quantity:      &commonv1.Decimal{Value: "1"},
	}

	if _, err := svc.PlaceOrder(context.Background(), req); err != nil {
		t.Fatalf("first PlaceOrder: %v", err)
	}

	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("second PlaceOrder: %v", err)
	}
	if !resp.GetIdempotentHit() {
		t.Fatal("expected idempotent_hit=true")
	}
}

func TestPlaceOrder_PrecisionRejected(t *testing.T) {
	reg, err := symbolrules.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	svc := &OrderService{
		Repo:           &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)},
		OutboxTopic:    "order.commands",
		SlippageBuffer: decimal.Zero,
		Symbols:        reg,
	}
	_, err = svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "bad-qty",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "100"},
		Quantity:      &commonv1.Decimal{Value: "0.0000001"},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("quantity precision: got %v", err)
	}
	_, err = svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "bad-price",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "100.001"},
		Quantity:      &commonv1.Decimal{Value: "1"},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("price precision: got %v", err)
	}
}

func TestPlaceOrder_InvalidArgument(t *testing.T) {
	svc := &OrderService{
		Repo:           &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)},
		OutboxTopic:    "order.commands",
		SlippageBuffer: decimal.Zero,
	}

	_, err := svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		ClientOrderId: "x",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "1"},
		Quantity:      &commonv1.Decimal{Value: "1"},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCancelOrder_Success(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)}
	store.byID[1] = &repository.Order{ID: 1, UserID: 1, Symbol: "BTC-USDT", Status: "ACCEPTED"}
	svc := &OrderService{Repo: store, OutboxTopic: "order.commands"}

	resp, err := svc.CancelOrder(context.Background(), &orderv1.CancelOrderRequest{UserId: 1, OrderId: 1})
	if err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if resp.GetStatus() != "CANCELING" {
		t.Fatalf("status=%q", resp.GetStatus())
	}
}

func TestGetOrder_Success(t *testing.T) {
	store := &fakeStore{byID: map[uint64]*repository.Order{
		1: {ID: 1, UserID: 1, ClientOrderID: "c1", Symbol: "BTC-USDT", Side: 1, OrderType: 1, Quantity: "1", FilledQuantity: "0", Status: "PENDING"},
	}}
	svc := &OrderService{Repo: store}

	resp, err := svc.GetOrder(context.Background(), &orderv1.GetOrderRequest{UserId: 1, OrderId: 1})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if resp.GetOrder().GetClientOrderId() != "c1" {
		t.Fatalf("client_order_id=%q", resp.GetOrder().GetClientOrderId())
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	svc := &OrderService{Repo: &fakeStore{byID: make(map[uint64]*repository.Order)}}
	_, err := svc.GetOrder(context.Background(), &orderv1.GetOrderRequest{UserId: 1, OrderId: 99})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPlaceOrder_MarketBuyWithoutPrice_UsesMarketData(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)}
	svc := &OrderService{
		Repo:           store,
		OutboxTopic:    "order.commands",
		MarketData:     &fakeMarketData{price: "100"},
		SlippageBuffer: decimal.RequireFromString("0.01"),
	}

	req := &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "m1",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_MARKET,
		Quantity:      &commonv1.Decimal{Value: "2"},
	}
	_, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if store.lastIn.Price != nil {
		t.Fatalf("market buy should not persist order price, got %v", *store.lastIn.Price)
	}
	if store.lastIn.FreezePrice == nil || *store.lastIn.FreezePrice != "100" {
		t.Fatalf("freeze_price=%v", store.lastIn.FreezePrice)
	}
	if store.lastIn.FrozenAmount == nil || *store.lastIn.FrozenAmount != "202" {
		t.Fatalf("frozen_amount=%v", store.lastIn.FrozenAmount)
	}
	if store.lastIn.FreezeSlippage == nil || *store.lastIn.FreezeSlippage != "0.01" {
		t.Fatalf("freeze_slippage=%v", store.lastIn.FreezeSlippage)
	}
}

func TestPlaceOrder_MarketDataUnavailable_ReturnsUnavailable(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)}
	svc := &OrderService{
		Repo:           store,
		OutboxTopic:    "order.commands",
		MarketData:     &fakeMarketData{err: errors.New("dial timeout")},
		SlippageBuffer: decimal.Zero,
	}
	req := &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "m2",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_MARKET,
		Quantity:      &commonv1.Decimal{Value: "2"},
	}
	_, err := svc.PlaceOrder(context.Background(), req)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}
