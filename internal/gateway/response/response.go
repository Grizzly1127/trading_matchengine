package response

import (
	"encoding/json"
	"net/http"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/requestctx"
)

// Envelope 统一 JSON 响应结构（rest-api §2.3）。
type Envelope struct {
	Code      int         `json:"code"`
	Message   string      `json:"message"`
	RequestID string      `json:"request_id"`
	Data      interface{} `json:"data"`
}

// WriteOK 写入成功响应，HTTP 状态为 status（通常 200）。
func WriteOK(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	write(w, r, status, 0, "ok", data)
}

// WriteError 写入失败响应。
func WriteError(w http.ResponseWriter, r *http.Request, httpStatus, code int, message string) {
	write(w, r, httpStatus, code, message, nil)
}

func write(w http.ResponseWriter, r *http.Request, httpStatus, code int, message string, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(Envelope{
		Code:      code,
		Message:   message,
		RequestID: requestctx.RequestID(r.Context()),
		Data:      data,
	})
}
