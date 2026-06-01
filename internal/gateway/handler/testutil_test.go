package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"

	gwmw "github.com/Grizzly1127/trading_matchengine/internal/gateway/middleware"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

const testStaticToken = "test-secret"

func newTestOrdersHandler(client orderv1.OrderServiceClient) http.Handler {
	v, err := auth.NewVerifier(context.Background(), auth.Config{
		Mode:        "static",
		StaticToken: testStaticToken,
	})
	if err != nil {
		panic(err)
	}
	h := &Orders{Orders: client, Log: zerolog.Nop()}
	r := chi.NewRouter()
	authenticate := gwmw.Authenticate(v)
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeOrdersWrite))
		r.Post("/v1/orders", h.PlaceOrder)
		r.Delete("/v1/orders/{order_id}", h.CancelOrder)
	})
	r.Group(func(r chi.Router) {
		r.Use(authenticate, gwmw.RequireScopes(auth.ScopeOrdersRead))
		r.Get("/v1/orders", h.ListOrders)
		r.Get("/v1/orders/{order_id}", h.GetOrder)
		r.Get("/v1/trades", h.ListTrades)
	})
	return r
}

func authedRequest(method, target string, body string) *http.Request {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	req.Header.Set("Authorization", "Bearer "+testStaticToken)
	return req
}
