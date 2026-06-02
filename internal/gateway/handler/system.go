package handler

import (
	"net/http"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
)

// Health 返回 Gateway 存活状态（不探测下游）。
func Health(w http.ResponseWriter, r *http.Request) {
	response.WriteOK(w, r, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

// Time 返回服务器 UTC 时间与毫秒时间戳。
func Time(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	response.WriteOK(w, r, http.StatusOK, map[string]any{
		"server_time": formatISO8601UTC(now),
		"unix_ms":     now.UnixMilli(),
	})
}

func formatISO8601UTC(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z")
}
