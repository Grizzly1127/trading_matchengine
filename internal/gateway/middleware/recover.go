package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	"github.com/rs/zerolog"
)

// Recover 捕获 panic，返回 500 并记录堆栈。
func Recover(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error().
						Str("request_id", RequestIDFromContext(r.Context())).
						Str("method", r.Method).
						Str("path", r.URL.Path).
						Interface("panic", rec).
						Bytes("stack", debug.Stack()).
						Msg("http panic")
					response.WriteError(w, r, http.StatusInternalServerError, 50000, "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
