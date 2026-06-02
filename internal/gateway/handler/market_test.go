package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeMarketDataClient struct {
	tickerAllFn func(context.Context, *marketdatav1.GetTickerAllSnapshotRequest) (*marketdatav1.GetTickerAllSnapshotResponse, error)
}

func (f *fakeMarketDataClient) GetOrderBook(context.Context, *marketdatav1.GetOrderBookRequest, ...grpc.CallOption) (*marketdatav1.GetOrderBookResponse, error) {
	return nil, nil
}

func (f *fakeMarketDataClient) GetTicker(context.Context, *marketdatav1.GetTickerRequest, ...grpc.CallOption) (*marketdatav1.GetTickerResponse, error) {
	return nil, nil
}

func (f *fakeMarketDataClient) GetReferencePrice(context.Context, *marketdatav1.GetReferencePriceRequest, ...grpc.CallOption) (*marketdatav1.GetReferencePriceResponse, error) {
	return nil, nil
}

func (f *fakeMarketDataClient) GetTickerAllSnapshot(ctx context.Context, req *marketdatav1.GetTickerAllSnapshotRequest, _ ...grpc.CallOption) (*marketdatav1.GetTickerAllSnapshotResponse, error) {
	if f.tickerAllFn != nil {
		return f.tickerAllFn(ctx, req)
	}
	return &marketdatav1.GetTickerAllSnapshotResponse{}, nil
}

func TestMarketTickerAllNotModified(t *testing.T) {
	const snapID = "snap-abc"
	h := &Market{MarketData: &fakeMarketDataClient{
		tickerAllFn: func(_ context.Context, req *marketdatav1.GetTickerAllSnapshotRequest) (*marketdatav1.GetTickerAllSnapshotResponse, error) {
			if req.GetSnapshotId() != snapID {
				t.Fatalf("snapshot_id=%q", req.GetSnapshotId())
			}
			return &marketdatav1.GetTickerAllSnapshotResponse{
				SnapshotId:   snapID,
				SnapshotTime: timestamppb.Now(),
				NotModified:  true,
			}, nil
		},
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/market/ticker/all?quote_asset=USDT", nil)
	req.Header.Set("If-None-Match", snapID)
	w := httptest.NewRecorder()
	h.TickerAll(w, req)
	if w.Code != http.StatusNotModified {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Snapshot-Id") != snapID {
		t.Fatalf("headers=%v", w.Header())
	}
	if w.Body.Len() != 0 {
		t.Fatalf("expected empty body, got %q", w.Body.String())
	}
}

func TestMarketTickerAllOKFiltersStatus(t *testing.T) {
	rules, err := symbolrules.NewRegistry(
		symbolrules.Spec{Symbol: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT", Status: "TRADING"},
		symbolrules.Spec{Symbol: "OLD-USDT", BaseAsset: "OLD", QuoteAsset: "USDT", Status: "HALT"},
	)
	if err != nil {
		t.Fatal(err)
	}
	updated := timestamppb.Now()
	h := &Market{
		MarketData: &fakeMarketDataClient{
			tickerAllFn: func(_ context.Context, req *marketdatav1.GetTickerAllSnapshotRequest) (*marketdatav1.GetTickerAllSnapshotResponse, error) {
				if req.GetQuoteAsset() != "USDT" {
					t.Fatalf("quote=%q", req.GetQuoteAsset())
				}
				return &marketdatav1.GetTickerAllSnapshotResponse{
					SnapshotId:   "snap-1",
					SnapshotTime: updated,
					Count:        2,
					Items: []*marketdatav1.TickerAllItem{
						{Symbol: "BTC-USDT", LastPrice: &commonv1.Decimal{Value: "1"}, Volume: &commonv1.Decimal{Value: "2"}, QuoteVolume: &commonv1.Decimal{Value: "3"}, PriceChangePercent: &commonv1.Decimal{Value: "0.1"}, Timestamp: updated},
						{Symbol: "OLD-USDT", LastPrice: &commonv1.Decimal{Value: "9"}, Volume: &commonv1.Decimal{Value: "9"}, QuoteVolume: &commonv1.Decimal{Value: "9"}, PriceChangePercent: &commonv1.Decimal{Value: "0"}, Timestamp: updated},
					},
				}, nil
			},
		},
		SymbolRules: rules,
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/market/ticker/all?quote_asset=USDT", nil)
	w := httptest.NewRecorder()
	h.TickerAll(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Data struct {
			Count int `json:"count"`
			Items []struct {
				Symbol string `json:"symbol"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Count != 1 || len(body.Data.Items) != 1 || body.Data.Items[0].Symbol != "BTC-USDT" {
		t.Fatalf("data=%+v", body.Data)
	}
}
