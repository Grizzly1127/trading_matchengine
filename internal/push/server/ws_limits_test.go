package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/limits"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/alicebob/miniredis/v2"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

type testJWT struct {
	Retail string
	MM     string
}

func newTestWSServer(t *testing.T, lim limits.Config) (*WSServer, testJWT) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Close() })

	secret := []byte("push-test-hs256-secret")
	issuer := "trading-test"
	aud := []string{"trading-push"}
	secretFile := t.TempDir() + "/hs256.secret"
	if err := os.WriteFile(secretFile, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(context.Background(), auth.Config{
		Mode: "jwt",
		JWT: auth.JWTConfig{
			Audience: aud,
			Issuers: []auth.IssuerConfig{{
				Issuer:          issuer,
				HS256SecretFile: secretFile,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { verifier.Close() })

	retail, err := auth.SignHS256(secret, issuer, "retail-user", aud, []string{auth.ScopePushConnect}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	mm, err := auth.SignHS256(secret, issuer, "mm-user", aud, []string{auth.ScopePushConnect, auth.ScopePushTickerAll}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	srv := &WSServer{
		Hub:      hub.NewWithLimits(lim),
		Redis:    rdb,
		Verifier: verifier,
		Limits:   lim,
		Log:      zerolog.Nop(),
	}
	return srv, testJWT{Retail: retail, MM: mm}
}

func dialWS(t *testing.T, tsURL, token string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(tsURL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Authorization": {"Bearer " + token},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_, _, _ = conn.ReadMessage() // connected
	return conn
}

func TestWSRetailRejectsTickerAll(t *testing.T) {
	srv, tok := newTestWSServer(t, limits.Config{
		RetailMaxConnections:          2,
		RetailMaxSymbolsPerConnection: 50,
		MarketMakerMaxConnections:     3,
	})
	ts := httptest.NewServer(http.HandlerFunc(srv.HandleWS))
	defer ts.Close()

	conn := dialWS(t, ts.URL, tok.Retail)
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": []string{"ticker@all:USDT"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "market maker") {
		t.Fatalf("msg=%s", string(msg))
	}
}

func TestWSRetailSymbolLimit(t *testing.T) {
	srv, tok := newTestWSServer(t, limits.Config{
		RetailMaxConnections:          2,
		RetailMaxSymbolsPerConnection: 2,
		MarketMakerMaxConnections:     3,
	})
	ts := httptest.NewServer(http.HandlerFunc(srv.HandleWS))
	defer ts.Close()

	conn := dialWS(t, ts.URL, tok.Retail)
	defer conn.Close()

	args := []string{"ticker:BTC-USDT", "ticker:ETH-USDT", "ticker:SOL-USDT"}
	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": args}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "symbol subscription limit") {
		t.Fatalf("msg=%s", string(msg))
	}
}

func TestWSTickerAllSnapshotOnSubscribe(t *testing.T) {
	srv, tok := newTestWSServer(t, limits.Config{
		RetailMaxConnections:          5,
		RetailMaxSymbolsPerConnection: 50,
		MarketMakerMaxConnections:     3,
	})
	ctx := context.Background()
	rest := `{"snapshot_id":"snap-x","snapshot_time":1,"count":1,"items":[{"symbol":"BTC-USDT","last_price":"1","volume":"2","quote_volume":"3","price_change_percent":"0.5"}]}`
	if err := srv.Redis.Set(ctx, "ticker:all:USDT", rest, 0); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(srv.HandleWS))
	defer ts.Close()

	conn := dialWS(t, ts.URL, tok.MM)
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": []string{"ticker@all:USDT"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, snapMsg, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var frame struct {
		Stream string `json:"stream"`
		Type   string `json:"type"`
	}
	if err := json.Unmarshal(snapMsg, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "snapshot" || frame.Stream != "ticker@all:USDT" {
		t.Fatalf("snap=%s", string(snapMsg))
	}
	_, subMsg, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(subMsg), "subscribed") {
		t.Fatalf("sub=%s", string(subMsg))
	}
}

func TestWSConnectionLimitPerSubject(t *testing.T) {
	lim := limits.Config{
		RetailMaxConnections:          2,
		RetailMaxSymbolsPerConnection: 50,
		MarketMakerMaxConnections:     2,
	}
	srv, tok := newTestWSServer(t, lim)
	ts := httptest.NewServer(http.HandlerFunc(srv.HandleWS))
	defer ts.Close()

	c1 := dialWS(t, ts.URL, tok.Retail)
	defer c1.Close()
	c2 := dialWS(t, ts.URL, tok.Retail)
	defer c2.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Authorization": {"Bearer " + tok.Retail},
	})
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected third connection rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		if resp != nil {
			t.Fatalf("status=%d want 429", resp.StatusCode)
		}
		t.Fatal("nil response")
	}
}
