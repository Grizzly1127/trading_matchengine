package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeIndexPriceClient struct {
	getFn func(context.Context, *indexv1.GetIndexPriceRequest) (*indexv1.GetIndexPriceResponse, error)
}

func (f *fakeIndexPriceClient) GetIndexPrice(ctx context.Context, req *indexv1.GetIndexPriceRequest, _ ...grpc.CallOption) (*indexv1.GetIndexPriceResponse, error) {
	if f.getFn != nil {
		return f.getFn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "get index price")
}

func TestIndexPriceGetMissingSymbol(t *testing.T) {
	h := &IndexPrice{IndexPrice: &fakeIndexPriceClient{}}
	req := httptest.NewRequest(http.MethodGet, "/v1/index-price", nil)
	w := httptest.NewRecorder()
	h.GetIndexPrice(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestIndexPriceGetOK(t *testing.T) {
	updated := timestamppb.Now()
	h := &IndexPrice{IndexPrice: &fakeIndexPriceClient{
		getFn: func(_ context.Context, req *indexv1.GetIndexPriceRequest) (*indexv1.GetIndexPriceResponse, error) {
			if req.GetSymbol() != "BTC-USDT" {
				t.Fatalf("symbol=%q", req.GetSymbol())
			}
			return &indexv1.GetIndexPriceResponse{
				IndexPrice: &indexv1.IndexPrice{
					Symbol:    "BTC-USDT",
					Price:     &commonv1.Decimal{Value: "65000.12"},
					UpdatedAt: updated,
					Sources:   []string{"binance", "okx"},
					Stale:     false,
				},
			}, nil
		},
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/index-price?symbol=BTC-USDT", nil)
	w := httptest.NewRecorder()
	h.GetIndexPrice(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Code int `json:"code"`
		Data struct {
			Symbol    string   `json:"symbol"`
			Price     string   `json:"price"`
			Timestamp string   `json:"timestamp"`
			Sources   []string `json:"sources"`
			Stale     bool     `json:"stale"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != 0 || body.Data.Symbol != "BTC-USDT" || body.Data.Price != "65000.12" {
		t.Fatalf("body=%+v", body)
	}
	if body.Data.Timestamp == "" || len(body.Data.Sources) != 2 {
		t.Fatalf("data=%+v", body.Data)
	}
}
