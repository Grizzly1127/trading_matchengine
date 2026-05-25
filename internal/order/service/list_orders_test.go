package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

func TestListOrders_UserRequired(t *testing.T) {
	svc := &Service{Repo: &fakeStore{byID: make(map[uint64]*repository.Order)}}
	_, err := svc.ListOrders(context.Background(), &orderv1.ListOrdersRequest{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestListOrders_DefaultPagination(t *testing.T) {
	store := &fakeStore{byID: map[uint64]*repository.Order{
		1: {ID: 1, UserID: 1, ClientOrderID: "c1", Symbol: "BTC-USDT", Status: "PENDING"},
	}}
	svc := &Service{Repo: store}

	resp, err := svc.ListOrders(context.Background(), &orderv1.ListOrdersRequest{UserId: 1})
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	if len(resp.GetOrders()) != 1 {
		t.Fatalf("orders=%d want 1", len(resp.GetOrders()))
	}
}
