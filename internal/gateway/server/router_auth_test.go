package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeMDClient struct{}

func (fakeMDClient) GetOrderBook(context.Context, *marketdatav1.GetOrderBookRequest, ...grpc.CallOption) (*marketdatav1.GetOrderBookResponse, error) {
	return nil, status.Error(codes.Unimplemented, "orderbook")
}

func (fakeMDClient) GetTicker(context.Context, *marketdatav1.GetTickerRequest, ...grpc.CallOption) (*marketdatav1.GetTickerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ticker")
}

func (fakeMDClient) GetReferencePrice(context.Context, *marketdatav1.GetReferencePriceRequest, ...grpc.CallOption) (*marketdatav1.GetReferencePriceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "reference")
}

func (fakeMDClient) GetTickerAllSnapshot(context.Context, *marketdatav1.GetTickerAllSnapshotRequest, ...grpc.CallOption) (*marketdatav1.GetTickerAllSnapshotResponse, error) {
	return &marketdatav1.GetTickerAllSnapshotResponse{SnapshotId: "snap-1", Count: 0}, nil
}

func TestTickerAllForbiddenWithoutMarketMakerScope(t *testing.T) {
	verifier, err := auth.NewVerifier(context.Background(), auth.Config{
		Mode:         "static",
		StaticToken:  "retail",
		StaticScopes: []string{auth.ScopeMarketRead, auth.ScopePushConnect},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.Close()

	r := NewRouter(Deps{
		Log:        zerolog.Nop(),
		Config:     config.Config{HTTPListen: ":0"},
		Verifier:   verifier,
		MarketData: fakeMDClient{},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/market/ticker/all?quote_asset=USDT", nil)
	req.Header.Set("Authorization", "Bearer retail")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
