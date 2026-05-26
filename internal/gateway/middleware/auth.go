package middleware

import (
	"net/http"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
)

// Auth Phase 1：校验 Bearer token，将 static_user_id 写入 context。
func Auth(cfg config.AuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPublicRoute(r.Method, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			token, ok := bearerToken(r.Header.Get("Authorization"))
			if !ok || token != cfg.StaticToken {
				response.WriteError(w, r, http.StatusUnauthorized, 40100, "unauthorized")
				return
			}

			ctx := WithUserID(r.Context(), cfg.StaticUserID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func isPublicRoute(method, path string) bool {
	if method != http.MethodGet {
		return false
	}
	switch path {
	case "/v1/health", "/v1/time":
		return true
	default:
		return false
	}
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}
