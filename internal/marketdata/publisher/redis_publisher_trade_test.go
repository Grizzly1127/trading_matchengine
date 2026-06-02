package publisher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/publisher"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

func TestPublishTrade(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sub, err := rdb.Subscribe(ctx, "trade:BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	type recvResult struct {
		msg redis.Message
		err error
	}
	done := make(chan recvResult, 1)
	go func() {
		msg, recvErr := sub.Receive(ctx)
		done <- recvResult{msg, recvErr}
	}()

	time.Sleep(10 * time.Millisecond)

	pub := publisher.NewRedisPublisher(rdb)
	payload := publisher.TradePayload(9, "BTC-USDT", "100", "0.1", 1, 2, 123)
	if err := pub.PublishTrade(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatal(res.err)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(res.msg.Payload), &got); err != nil {
			t.Fatal(err)
		}
		if got["trade_id"] != "9" || got["symbol"] != "BTC-USDT" {
			t.Fatalf("payload=%v", got)
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}
