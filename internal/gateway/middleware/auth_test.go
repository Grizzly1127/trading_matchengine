package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
)

func TestAuth_PublicHealth(t *testing.T) {
	var called bool
	h := Auth(auth.Config{Mode: "static", StaticToken: "secret"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("handler not called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestAuth_PublicMarketDepth(t *testing.T) {
	var called bool
	h := Auth(auth.Config{Mode: "static", StaticToken: "secret"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/market/depth?symbol=BTC-USDT", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("handler not called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestAuth_MissingToken(t *testing.T) {
	h := Auth(auth.Config{Mode: "static", StaticToken: "secret"})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("should not reach handler")
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/orders", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rec.Code)
	}
}

func TestAuth_ValidTokenSetsUserIDFromHeader(t *testing.T) {
	var userID uint64
	h := Auth(auth.Config{Mode: "static", StaticToken: "secret"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID = UserIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/orders", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set(HeaderUserID, "42")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if userID != 42 {
		t.Fatalf("user_id=%d", userID)
	}
}
