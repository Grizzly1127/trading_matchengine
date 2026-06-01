package middleware

import (
	"context"
	"net/http"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
)

// Authenticate 校验 Bearer（static / JWT），将 Claims 写入 context。
func Authenticate(v *auth.Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPublicRoute(r.Method, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			bearer, ok := auth.BearerFromHeader(r.Header.Get("Authorization"))
			if !ok {
				response.WriteError(w, r, http.StatusUnauthorized, 40100, "unauthorized")
				return
			}
			claims, err := v.VerifyBearer(r.Context(), bearer)
			if err != nil {
				response.WriteError(w, r, http.StatusUnauthorized, 40100, "unauthorized")
				return
			}

			ctx := auth.WithClaims(r.Context(), claims)
			if id, err := parseUserIDHeaderOrQuery(r); err == nil {
				ctx = WithUserID(ctx, id)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireScopes 在 Authenticate 之后检查服务 scope。
func RequireScopes(scopes ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPublicRoute(r.Method, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			claims, ok := auth.ClaimsFromContext(r.Context())
			if !ok || !auth.HasScopes(claims, scopes...) {
				response.WriteError(w, r, http.StatusForbidden, 40300, "insufficient scope")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Auth 兼容单测：static 模式 + 不在此中间件做 scope 细分。
func Auth(cfg auth.Config) func(http.Handler) http.Handler {
	v, err := auth.NewVerifier(context.Background(), cfg)
	if err != nil {
		panic(err)
	}
	return Authenticate(v)
}

func isPublicRoute(method, path string) bool {
	if method != http.MethodGet {
		return false
	}
	switch path {
	case "/v1/health", "/v1/time", "/v1/market/depth", "/v1/market/ticker", "/v1/market/symbols":
		return true
	default:
		return false
	}
}
