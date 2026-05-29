package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Kline 提供 K 线 REST 查询。
type Kline struct {
	Client klinev1.KlineServiceClient
	Log    zerolog.Logger
}

func (h *Kline) List(w http.ResponseWriter, r *http.Request) {
	symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
	interval := strings.TrimSpace(r.URL.Query().Get("interval"))
	if symbol == "" || interval == "" {
		grpcerr.Write(w, r, grpcerr.BadRequest("symbol and interval are required"))
		return
	}
	req := &klinev1.GetKlinesRequest{
		Symbol:   symbol,
		Interval: interval,
	}
	if s := strings.TrimSpace(r.URL.Query().Get("start_time")); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			grpcerr.Write(w, r, grpcerr.BadRequest("invalid start_time"))
			return
		}
		req.StartTime = timestamppb.New(t.UTC())
	}
	if s := strings.TrimSpace(r.URL.Query().Get("end_time")); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			grpcerr.Write(w, r, grpcerr.BadRequest("invalid end_time"))
			return
		}
		req.EndTime = timestamppb.New(t.UTC())
	}
	if s := strings.TrimSpace(r.URL.Query().Get("limit")); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			grpcerr.Write(w, r, grpcerr.BadRequest("limit must be positive integer"))
			return
		}
		if v > 1500 {
			v = 1500
		}
		req.Limit = int32(v)
	}

	resp, err := h.Client.GetKlines(r.Context(), req)
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}
	items := make([]map[string]interface{}, 0, len(resp.GetKlines()))
	for _, k := range resp.GetKlines() {
		items = append(items, pbKlineToJSON(k))
	}
	response.WriteOK(w, r, http.StatusOK, map[string]interface{}{"items": items})
}

func pbKlineToJSON(k *klinev1.Kline) map[string]interface{} {
	return map[string]interface{}{
		"open_time":  k.GetOpenTime().AsTime().UTC().Format(timeLayoutMilli),
		"close_time": k.GetCloseTime().AsTime().UTC().Format(timeLayoutMilli),
		"open":       dec(k.GetOpen()),
		"high":       dec(k.GetHigh()),
		"low":        dec(k.GetLow()),
		"close":      dec(k.GetClose()),
		"volume":     dec(k.GetVolume()),
		"is_closed":  k.GetIsClosed(),
	}
}
