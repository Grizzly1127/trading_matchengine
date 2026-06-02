package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Market struct {
	MarketData   marketdatav1.MarketDataServiceClient
	SymbolRules  *symbolrules.Registry
	AssetRules   *symbolrules.AssetRegistry
	Log          zerolog.Logger
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
	response.WriteOK(w, r, http.StatusOK, map[string]any{
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
	items := make([]map[string]any, 0, len(resp.GetItems()))
	for _, item := range resp.GetItems() {
		items = append(items, pbTickerToJSON(item))
	}
	response.WriteOK(w, r, http.StatusOK, map[string]any{"items": items})
}

// TickerAll 全市场 Ticker 冷启动快照（rest-api §4.2.1）。
func (h *Market) TickerAll(w http.ResponseWriter, r *http.Request) {
	quoteAsset := strings.TrimSpace(r.URL.Query().Get("quote_asset"))
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	if statusFilter == "" {
		statusFilter = "TRADING"
	}
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format == "" {
		format = "json"
	}
	if format != "json" {
		grpcerr.Write(w, r, grpcerr.BadRequest("only format=json is supported"))
		return
	}

	snapshotID := strings.TrimSpace(r.URL.Query().Get("snapshot_id"))
	if snapshotID == "" {
		snapshotID = strings.Trim(strings.TrimSpace(r.Header.Get("If-None-Match")), `"`)
	}

	resp, err := h.MarketData.GetTickerAllSnapshot(r.Context(), &marketdatav1.GetTickerAllSnapshotRequest{
		QuoteAsset: quoteAsset,
		SnapshotId: snapshotID,
	})
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	setTickerAllSnapshotHeaders(w, resp.GetSnapshotId(), resp.GetSnapshotTime())
	if resp.GetNotModified() {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	items := filterTickerAllItems(resp.GetItems(), h.SymbolRules, statusFilter)
	data := map[string]any{
		"snapshot_id": resp.GetSnapshotId(),
		"count":       len(items),
		"items":       items,
	}
	if ts := resp.GetSnapshotTime(); ts != nil {
		data["snapshot_time"] = ts.AsTime().UTC().Format(timeLayoutMilli)
	}
	response.WriteOK(w, r, http.StatusOK, data)
}

// Symbols 返回可交易对元数据（与 configs/symbols.json 一致）。
func (h *Market) Symbols(w http.ResponseWriter, r *http.Request) {
	if h.SymbolRules == nil {
		grpcerr.Write(w, r, grpcerr.BadRequest("symbols not configured"))
		return
	}
	items := make([]map[string]any, 0)
	for _, sp := range h.SymbolRules.All() {
		items = append(items, symbolSpecToJSON(sp, h.AssetRules))
	}
	response.WriteOK(w, r, http.StatusOK, map[string]any{"items": items})
}

func symbolSpecToJSON(sp symbolrules.Spec, assets *symbolrules.AssetRegistry) map[string]any {
	out := map[string]any{
		"symbol":             sp.Symbol,
		"base_asset":         sp.BaseAsset,
		"quote_asset":        sp.QuoteAsset,
		"price_precision":    sp.PricePrecision,
		"quantity_precision": sp.QuantityPrecision,
		"min_quantity":       sp.MinQuantity.String(),
		"min_notional":       sp.MinNotional.String(),
		"status":             sp.Status,
	}
	if assets != nil {
		out["base_asset_precision"] = assets.Precision(sp.BaseAsset)
		out["quote_asset_precision"] = assets.Precision(sp.QuoteAsset)
	}
	return out
}

const timeLayoutMilli = "2006-01-02T15:04:05.000Z07:00"

func pbLevelsToJSON(in []*marketdatav1.PriceLevel) [][]string {
	out := make([][]string, 0, len(in))
	for _, lv := range in {
		out = append(out, []string{dec(lv.GetPrice()), dec(lv.GetQuantity())})
	}
	return out
}

func setTickerAllSnapshotHeaders(w http.ResponseWriter, snapshotID string, snapshotTime *timestamppb.Timestamp) {
	if snapshotID != "" {
		w.Header().Set("X-Snapshot-Id", snapshotID)
	}
	if snapshotTime != nil {
		w.Header().Set("X-Snapshot-Time", snapshotTime.AsTime().UTC().Format(timeLayoutMilli))
	}
}

func filterTickerAllItems(items []*marketdatav1.TickerAllItem, rules *symbolrules.Registry, status string) []map[string]any {
	if len(items) == 0 {
		return nil
	}
	wantStatus := strings.ToUpper(strings.TrimSpace(status))
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		if rules != nil && wantStatus != "" {
			sp, err := rules.Lookup(item.GetSymbol())
			if err != nil || strings.ToUpper(sp.Status) != wantStatus {
				continue
			}
		}
		out = append(out, pbTickerAllItemToJSON(item))
	}
	return out
}

func pbTickerAllItemToJSON(item *marketdatav1.TickerAllItem) map[string]any {
	out := map[string]any{
		"symbol":               item.GetSymbol(),
		"last_price":           dec(item.GetLastPrice()),
		"volume":               dec(item.GetVolume()),
		"quote_volume":         dec(item.GetQuoteVolume()),
		"price_change_percent": dec(item.GetPriceChangePercent()),
	}
	if ts := item.GetTimestamp(); ts != nil {
		out["timestamp"] = ts.AsTime().UTC().Format(timeLayoutMilli)
	}
	return out
}

func pbTickerToJSON(t *marketdatav1.Ticker) map[string]any {
	return map[string]any{
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
