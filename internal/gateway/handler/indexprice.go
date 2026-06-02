package handler

import (
	"net/http"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IndexPrice struct {
	IndexPrice indexv1.IndexPriceServiceClient
	Log        zerolog.Logger
}

func (h *IndexPrice) GetIndexPrice(w http.ResponseWriter, r *http.Request) {
	symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
	if symbol == "" {
		grpcerr.Write(w, r, grpcerr.BadRequest("symbol is required"))
		return
	}
	req := &indexv1.GetIndexPriceRequest{
		Symbol: symbol,
	}
	resp, err := h.IndexPrice.GetIndexPrice(r.Context(), req)
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}
	ip := resp.GetIndexPrice()
	if ip == nil {
		grpcerr.Write(w, r, status.Error(codes.NotFound, "index price not available"))
		return
	}
	response.WriteOK(w, r, http.StatusOK, pbIndexPriceToJSON(ip))
}

func pbIndexPriceToJSON(ip *indexv1.IndexPrice) map[string]any {
	out := map[string]any{
		"symbol":  ip.GetSymbol(),
		"price":   dec(ip.GetPrice()),
		"sources": ip.GetSources(),
		"stale":   ip.GetStale(),
	}
	if ts := ip.GetUpdatedAt(); ts != nil {
		out["timestamp"] = ts.AsTime().UTC().Format(timeLayoutMilli)
	}
	return out
}
