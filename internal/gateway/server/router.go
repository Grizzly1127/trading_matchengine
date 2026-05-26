package server

import (
	"net/http"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/handler"
	gwmw "github.com/Grizzly1127/trading_matchengine/internal/gateway/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// NewRouter 注册路由与中间件链：RequestID → Auth → Recover → AccessLog。
func NewRouter(log zerolog.Logger, cfg config.Config) http.Handler {
	r := chi.NewRouter()
	r.Use(gwmw.RequestID)
	r.Use(gwmw.Auth(cfg.Auth))
	r.Use(gwmw.Recover(log))
	r.Use(gwmw.AccessLog(log))

	r.Get("/v1/health", handler.Health)
	r.Get("/v1/time", handler.Time)

	return r
}
