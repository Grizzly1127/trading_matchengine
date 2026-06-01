package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/subscriber"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

func TestWSReceiveFromRedisPublish(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rdb, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	h := hub.New()
	verifier, err := auth.NewVerifier(context.Background(), auth.Config{
		Mode:        "static",
		StaticToken: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.Close()
	wsServer := &WSServer{
		Hub:      h,
		Redis:    rdb,
		Verifier: verifier,
		Log:      zerolog.Nop(),
	}
	fanout := &subscriber.RedisFanout{
		Redis: rdb,
		Hub:   h,
		Log:   zerolog.Nop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = fanout.Run(ctx) }()

	ts := httptest.NewServer(http.HandlerFunc(wsServer.HandleWS))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Authorization": {"Bearer secret"},
	})
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	// connected
	_, _, _ = conn.ReadMessage()
	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": []string{"ticker:BTC-USDT"}}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// subscribed ack
	_, _, _ = conn.ReadMessage()

	payload := `{"symbol":"BTC-USDT","last_price":"65000"}`
	if err := rdb.Publish(context.Background(), "ticker:BTC-USDT", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read pushed msg: %v", err)
	}
	if string(msg) != payload {
		t.Fatalf("unexpected payload: %s", string(msg))
	}
}
