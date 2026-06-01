package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeOrderClient struct {
	placeFn      func(context.Context, *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error)
	getFn        func(context.Context, *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error)
	listFn       func(context.Context, *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error)
	cancelFn     func(context.Context, *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error)
	listTradesFn func(context.Context, *orderv1.ListTradesRequest) (*orderv1.ListTradesResponse, error)
}

func (f *fakeOrderClient) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest, _ ...grpc.CallOption) (*orderv1.PlaceOrderResponse, error) {
	if f.placeFn != nil {
		return f.placeFn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "place")
}

func (f *fakeOrderClient) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	if f.getFn != nil {
		return f.getFn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "get")
}

func (f *fakeOrderClient) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest, _ ...grpc.CallOption) (*orderv1.ListOrdersResponse, error) {
	if f.listFn != nil {
		return f.listFn(ctx, req)
	}
	return &orderv1.ListOrdersResponse{}, nil
}

func (f *fakeOrderClient) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest, _ ...grpc.CallOption) (*orderv1.CancelOrderResponse, error) {
	if f.cancelFn != nil {
		return f.cancelFn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "cancel")
}

func (f *fakeOrderClient) ListTrades(ctx context.Context, req *orderv1.ListTradesRequest, _ ...grpc.CallOption) (*orderv1.ListTradesResponse, error) {
	if f.listTradesFn != nil {
		return f.listTradesFn(ctx, req)
	}
	return &orderv1.ListTradesResponse{}, nil
}

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) map[string]json.RawMessage {
	t.Helper()
	var env map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v body=%s", err, rec.Body.String())
	}
	return env
}

func TestPlaceOrder_Created(t *testing.T) {
	fake := &fakeOrderClient{
		placeFn: func(_ context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
			if req.GetUserId() != 7 || req.GetSymbol() != "BTC-USDT" {
				t.Fatalf("unexpected request: %+v", req)
			}
			return &orderv1.PlaceOrderResponse{
				OrderId:       1001,
				ClientOrderId: "c1",
				Symbol:        "BTC-USDT",
				Status:        "PENDING",
				CreatedAt:     timestamppb.Now(),
			}, nil
		},
	}
	h := newTestOrdersHandler(fake)
	body := `{"user_id":7,"client_order_id":"c1","symbol":"BTC-USDT","side":"BUY","type":"LIMIT","price":"100","quantity":"1"}`
	req := authedRequest(http.MethodPost, "/v1/orders", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	env := decodeEnvelope(t, rec)
	if string(env["code"]) != "0" {
		t.Fatalf("code=%s", env["code"])
	}
}

func TestPlaceOrder_MissingUserID(t *testing.T) {
	h := newTestOrdersHandler(&fakeOrderClient{})
	body := `{"client_order_id":"c1","symbol":"BTC-USDT","side":"BUY","type":"MARKET","quantity":"1"}`
	req := authedRequest(http.MethodPost, "/v1/orders", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestListOrders_ForwardsUserAndSymbol(t *testing.T) {
	var got *orderv1.ListOrdersRequest
	fake := &fakeOrderClient{
		listFn: func(_ context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
			got = req
			return &orderv1.ListOrdersResponse{
				Orders: []*orderv1.OrderInfo{{
					OrderId:  9,
					Symbol:   "BTC-USDT",
					Side:     commonv1.Side_SIDE_BUY,
					Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
					Quantity: &commonv1.Decimal{Value: "1"},
					Status:   "FILLED",
				}},
			}, nil
		},
	}
	h := newTestOrdersHandler(fake)
	req := authedRequest(http.MethodGet, "/v1/orders?user_id=3&symbol=BTC-USDT&limit=10", "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got.GetUserId() != 3 || got.GetSymbol() != "BTC-USDT" || got.GetPageSize() != 10 {
		t.Fatalf("list request: %+v", got)
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	fake := &fakeOrderClient{
		getFn: func(context.Context, *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
			return nil, status.Error(codes.NotFound, "order not found")
		},
	}
	h := newTestOrdersHandler(fake)
	req := authedRequest(http.MethodGet, "/v1/orders/42?user_id=1", "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestListTrades_Success(t *testing.T) {
	var got *orderv1.ListTradesRequest
	fake := &fakeOrderClient{
		listTradesFn: func(_ context.Context, req *orderv1.ListTradesRequest) (*orderv1.ListTradesResponse, error) {
			got = req
			return &orderv1.ListTradesResponse{
				Trades: []*orderv1.TradeInfo{{
					TradeId:  500,
					Symbol:   "BTC-USDT",
					Price:    &commonv1.Decimal{Value: "65000"},
					Quantity: &commonv1.Decimal{Value: "0.01"},
					OrderId:  42,
					Side:     "BUY",
					IsMaker:  false,
					CreatedAt: timestamppb.Now(),
				}},
			}, nil
		},
	}
	h := newTestOrdersHandler(fake)
	req := authedRequest(http.MethodGet, "/v1/trades?user_id=1&symbol=BTC-USDT&order_id=42&limit=20", "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got.GetUserId() != 1 || got.GetSymbol() != "BTC-USDT" || got.GetOrderId() != 42 || got.GetPageSize() != 20 {
		t.Fatalf("list trades request: %+v", got)
	}
	var data struct {
		Items []struct {
			TradeID string `json:"trade_id"`
			OrderID string `json:"order_id"`
			Side    string `json:"side"`
		} `json:"items"`
	}
	env := decodeEnvelope(t, rec)
	if err := json.Unmarshal(env["data"], &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Items) != 1 || data.Items[0].TradeID != "500" || data.Items[0].OrderID != "42" || data.Items[0].Side != "BUY" {
		t.Fatalf("items=%+v", data.Items)
	}
}

func TestListTrades_InvalidOrderID(t *testing.T) {
	h := newTestOrdersHandler(&fakeOrderClient{})
	req := authedRequest(http.MethodGet, "/v1/trades?user_id=1&order_id=bad", "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestListTrades_Unauthorized(t *testing.T) {
	h := newTestOrdersHandler(&fakeOrderClient{})
	req := httptest.NewRequest(http.MethodGet, "/v1/trades?user_id=1", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", rec.Code)
	}
}
