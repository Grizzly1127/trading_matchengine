package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
)

type fakeStore struct {
	byClient map[string]*repository.Order
	byID     map[uint64]*repository.Order
	nextID   uint64
}

func (f *fakeStore) FindByClientOrderID(_ context.Context, userID uint64, clientOrderID string) (*repository.Order, error) {
	key := idemKey(userID, clientOrderID)
	if o, ok := f.byClient[key]; ok {
		return o, nil
	}
	return nil, nil
}

func (f *fakeStore) InsertPending(_ context.Context, in repository.InsertPendingInput) (*repository.Order, error) {
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

func (f *fakeStore) GetOrderByUser(_ context.Context, userID, orderID uint64) (*repository.Order, error) {
	o, ok := f.byID[orderID]
	if !ok || o.UserID != userID {
		return nil, repository.ErrOrderNotFound
	}
	return o, nil
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
	svc := &Service{Repo: store, OutboxTopic: "order.commands"}

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
	svc := &Service{Repo: store, OutboxTopic: "order.commands"}

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

func TestPlaceOrder_InvalidArgument(t *testing.T) {
	svc := &Service{
		Repo:        &fakeStore{byClient: make(map[string]*repository.Order), byID: make(map[uint64]*repository.Order)},
		OutboxTopic: "order.commands",
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
	svc := &Service{Repo: store, OutboxTopic: "order.commands"}

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
	svc := &Service{Repo: store}

	resp, err := svc.GetOrder(context.Background(), &orderv1.GetOrderRequest{UserId: 1, OrderId: 1})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if resp.GetClientOrderId() != "c1" {
		t.Fatalf("client_order_id=%q", resp.GetClientOrderId())
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	svc := &Service{Repo: &fakeStore{byID: make(map[uint64]*repository.Order)}}
	_, err := svc.GetOrder(context.Background(), &orderv1.GetOrderRequest{UserId: 1, OrderId: 99})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
