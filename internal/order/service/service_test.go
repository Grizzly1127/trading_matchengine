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
	return o, nil
}

type fakePublisher struct {
	orders []*repository.Order
	err    error
}

func (f *fakePublisher) PublishNewOrder(_ context.Context, order *repository.Order) error {
	if f.err != nil {
		return f.err
	}
	f.orders = append(f.orders, order)
	return nil
}

func idemKey(userID uint64, clientOrderID string) string {
	return fmt.Sprintf("%d:%s", userID, clientOrderID)
}

func TestPlaceOrder_Success(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order)}
	pub := &fakePublisher{}
	svc := &Service{Repo: store, Publisher: pub}

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
	if resp.GetIdempotentHit() {
		t.Fatal("expected idempotent_hit=false")
	}
	if len(pub.orders) != 1 {
		t.Fatalf("published %d orders want 1", len(pub.orders))
	}
}

func TestPlaceOrder_Idempotent(t *testing.T) {
	store := &fakeStore{byClient: make(map[string]*repository.Order)}
	pub := &fakePublisher{}
	svc := &Service{Repo: store, Publisher: pub}

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
	if len(pub.orders) != 1 {
		t.Fatalf("published %d orders want 1 (no duplicate kafka)", len(pub.orders))
	}
}

func TestPlaceOrder_InvalidArgument(t *testing.T) {
	svc := &Service{
		Repo:      &fakeStore{byClient: make(map[string]*repository.Order)},
		Publisher: &fakePublisher{},
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

func TestPlaceOrder_KafkaError(t *testing.T) {
	svc := &Service{
		Repo:      &fakeStore{byClient: make(map[string]*repository.Order)},
		Publisher: &fakePublisher{err: errors.New("kafka down")},
	}

	req := &orderv1.PlaceOrderRequest{
		UserId:        1,
		ClientOrderId: "c1",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "100"},
		Quantity:      &commonv1.Decimal{Value: "1"},
	}

	_, err := svc.PlaceOrder(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when kafka fails")
	}
}
