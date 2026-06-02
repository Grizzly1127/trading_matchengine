package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/subscriber"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/Grizzly1127/trading_matchengine/pkg/tickerall"
	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"
)

func TestWSTickerAllReceivesDeltaFrame(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	verifier, err := auth.NewVerifier(context.Background(), auth.Config{
		Mode:                   "static",
		StaticToken:            "retail",
		StaticScopes:           []string{auth.ScopePushConnect},
		MarketMakerStaticToken: "mm",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.Close()

	h := hub.New()
	wsServer := &WSServer{Hub: h, Redis: rdb, Verifier: verifier, Log: zerolog.Nop()}
	fanout := &subscriber.RedisFanout{Redis: rdb, Hub: h, Log: zerolog.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = fanout.Run(ctx) }()

	ts := httptest.NewServer(http.HandlerFunc(wsServer.HandleWS))
	defer ts.Close()

	conn := dialWS(t, ts.URL, "mm")
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": []string{"ticker@all:USDT"}}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = conn.ReadMessage() // subscribed
	// 无 redis 快照时可能只有 subscribed

	delta, err := tickerall.MarshalDelta("ticker@all:USDT", "snap-1", time.Now().UnixMilli(), []tickerall.CompactItem{
		{S: "BTC-USDT", P: "65001.00"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Publish(context.Background(), "ticker@all:USDT", string(delta)); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var f tickerall.Frame
		if err := json.Unmarshal(msg, &f); err != nil {
			continue
		}
		if f.Type == tickerall.TypeDelta && f.Stream == "ticker@all:USDT" {
			return
		}
	}
}
