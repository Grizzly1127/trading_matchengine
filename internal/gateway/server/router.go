package server

import (
	"net/http"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/handler"
	gwmw "github.com/Grizzly1127/trading_matchengine/internal/gateway/middleware"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

type Deps struct {
	Log        zerolog.Logger
	Config     config.Config
	Verifier   *auth.Verifier
	Order      orderv1.OrderServiceClient
	Balance    orderv1.BalanceServiceClient
	MarketData marketdatav1.MarketDataServiceClient
	Kline      klinev1.KlineServiceClient
	IndexPrice indexv1.IndexPriceServiceClient
	Symbols    *symbolrules.Registry
	Assets     *symbolrules.AssetRegistry
}

// NewRouter 注册路由与中间件链：RequestID → Auth → Recover → AccessLog。
func NewRouter(deps Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(gwmw.RequestID)
	r.Use(gwmw.Recover(deps.Log))
	r.Use(gwmw.AccessLog(deps.Log))

	r.Get("/v1/health", handler.Health)
	r.Get("/v1/time", handler.Time)

	authenticate := gwmw.Authenticate(deps.Verifier)

	orderH := &handler.Orders{Orders: deps.Order, Log: deps.Log}
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeOrdersWrite))
		r.Post("/v1/orders", orderH.PlaceOrder)
	})
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeOrdersRead))
		r.Get("/v1/orders", orderH.ListOrders)
		r.Get("/v1/orders/{order_id}", orderH.GetOrder)
		r.Get("/v1/trades", orderH.ListTrades)
	})
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeOrdersWrite))
		r.Delete("/v1/orders/{order_id}", orderH.CancelOrder)
	})

	balanceH := &handler.Balances{Balances: deps.Balance, Log: deps.Log}
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeBalancesAdmin))
		r.Post("/v1/balances", balanceH.UpdateBalance)
	})
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeBalancesRead))
		r.Get("/v1/balances", balanceH.ListBalances)
		r.Get("/v1/balances/{asset}", balanceH.GetBalance)
	})

	indexPriceH := &handler.IndexPrice{IndexPrice: deps.IndexPrice, Log: deps.Log}
	r.Get("/v1/index-price", indexPriceH.GetIndexPrice)

	marketH := &handler.Market{MarketData: deps.MarketData, SymbolRules: deps.Symbols, AssetRules: deps.Assets, Log: deps.Log}
	r.Get("/v1/market/depth", marketH.Depth)
	r.Get("/v1/market/ticker", marketH.Ticker)
	r.Get("/v1/market/symbols", marketH.Symbols)
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopePushTickerAll))
		r.Use(middleware.Compress(5))
		r.Get("/v1/market/ticker/all", marketH.TickerAll)
	})

	if deps.Kline != nil {
		klineH := &handler.Kline{Client: deps.Kline, Log: deps.Log}
		r.Group(func(r chi.Router) {
			r.Use(authenticate, gwmw.RequireScopes(auth.ScopeMarketRead))
			r.Get("/v1/klines", klineH.List)
		})
	}

	return r
}
