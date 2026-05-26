package server

import (
	"net/http"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/handler"
	gwmw "github.com/Grizzly1127/trading_matchengine/internal/gateway/middleware"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

type Deps struct {
	Log     zerolog.Logger
	Config  config.Config
	Order   orderv1.OrderServiceClient
	Balance orderv1.BalanceServiceClient
}

// NewRouter 注册路由与中间件链：RequestID → Auth → Recover → AccessLog。
func NewRouter(deps Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(gwmw.RequestID)
	r.Use(gwmw.Auth(deps.Config.Auth))
	r.Use(gwmw.Recover(deps.Log))
	r.Use(gwmw.AccessLog(deps.Log))

	r.Get("/v1/health", handler.Health)
	r.Get("/v1/time", handler.Time)

	orderH := &handler.Orders{Orders: deps.Order, Log: deps.Log}
	r.Post("/v1/orders", orderH.PlaceOrder)
	r.Get("/v1/orders", orderH.ListOrders)
	r.Get("/v1/orders/{order_id}", orderH.GetOrder)
	r.Delete("/v1/orders/{order_id}", orderH.CancelOrder)

	balanceH := &handler.Balances{Balances: deps.Balance, Log: deps.Log}
	r.Post("/v1/balances", balanceH.UpdateBalance)
	r.Get("/v1/balances", balanceH.ListBalances)
	r.Get("/v1/balances/{asset}", balanceH.GetBalance)

	return r
}
