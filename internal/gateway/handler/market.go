package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"github.com/rs/zerolog"
)

type Market struct {
	MarketData marketdatav1.MarketDataServiceClient
	Log        zerolog.Logger
}

func (h *Market) Depth(w http.ResponseWriter, r *http.Request) {
	symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
	if symbol == "" {
		grpcerr.Write(w, r, grpcerr.BadRequest("symbol is required"))
		return
	}
	limit := int32(20)
	if s := strings.TrimSpace(r.URL.Query().Get("limit")); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			grpcerr.Write(w, r, grpcerr.BadRequest("limit must be positive integer"))
			return
		}
		if v > 100 {
			v = 100
		}
		limit = int32(v)
	}

	resp, err := h.MarketData.GetOrderBook(r.Context(), &marketdatav1.GetOrderBookRequest{
		Symbol: symbol,
		Limit:  limit,
	})
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}
	response.WriteOK(w, r, http.StatusOK, map[string]interface{}{
		"symbol":         resp.GetSymbol(),
		"last_update_id": strconv.FormatUint(resp.GetLastUpdateId(), 10),
		"bids":           pbLevelsToJSON(resp.GetBids()),
		"asks":           pbLevelsToJSON(resp.GetAsks()),
		"timestamp":      resp.GetTimestamp().AsTime().UTC().Format(timeLayoutMilli),
	})
}

func (h *Market) Ticker(w http.ResponseWriter, r *http.Request) {
	symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
	symbols := strings.TrimSpace(r.URL.Query().Get("symbols"))
	if symbol == "" && symbols == "" {
		grpcerr.Write(w, r, grpcerr.BadRequest("symbol or symbols is required"))
		return
	}
	req := &marketdatav1.GetTickerRequest{}
	if symbol != "" {
		req.Symbol = symbol
	}
	if symbols != "" {
		parts := strings.Split(symbols, ",")
		req.Symbols = make([]string, 0, len(parts))
		for _, p := range parts {
			s := strings.TrimSpace(p)
			if s != "" {
				req.Symbols = append(req.Symbols, s)
			}
		}
	}

	resp, err := h.MarketData.GetTicker(r.Context(), req)
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}
	if symbol != "" {
		response.WriteOK(w, r, http.StatusOK, pbTickerToJSON(resp.GetTicker()))
		return
	}
	items := make([]map[string]interface{}, 0, len(resp.GetItems()))
	for _, item := range resp.GetItems() {
		items = append(items, pbTickerToJSON(item))
	}
	response.WriteOK(w, r, http.StatusOK, map[string]interface{}{"items": items})
}

const timeLayoutMilli = "2006-01-02T15:04:05.000Z07:00"

func pbLevelsToJSON(in []*marketdatav1.PriceLevel) [][]string {
	out := make([][]string, 0, len(in))
	for _, lv := range in {
		out = append(out, []string{dec(lv.GetPrice()), dec(lv.GetQuantity())})
	}
	return out
}

func pbTickerToJSON(t *marketdatav1.Ticker) map[string]interface{} {
	return map[string]interface{}{
		"symbol":               t.GetSymbol(),
		"last_price":           dec(t.GetLastPrice()),
		"open_price":           dec(t.GetOpenPrice()),
		"high_price":           dec(t.GetHighPrice()),
		"low_price":            dec(t.GetLowPrice()),
		"volume":               dec(t.GetVolume()),
		"quote_volume":         dec(t.GetQuoteVolume()),
		"price_change_percent": dec(t.GetPriceChangePercent()),
		"timestamp":            t.GetTimestamp().AsTime().UTC().Format(timeLayoutMilli),
	}
}

func dec(d *commonv1.Decimal) string {
	if d == nil {
		return ""
	}
	return strings.TrimSpace(d.GetValue())
}
