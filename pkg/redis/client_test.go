package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

func TestClient_GetSet(t *testing.T) {
	t.Parallel()
	mr, c := newTestClient(t)
	defer mr.Close()
	defer c.Close()

	ctx := context.Background()
	if err := c.Set(ctx, "ticker:BTC-USDT", `{"last":"65000"}`, 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := c.Get(ctx, "ticker:BTC-USDT")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != `{"last":"65000"}` {
		t.Fatalf("got %q", got)
	}
}

func TestClient_Get_NotFound(t *testing.T) {
	t.Parallel()
	mr, c := newTestClient(t)
	defer mr.Close()
	defer c.Close()

	_, err := c.Get(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, goredis.Nil) {
		t.Fatalf("want redis.Nil, got %v", err)
	}
}

func TestClient_PublishSubscribe(t *testing.T) {
	t.Parallel()
	mr, c := newTestClient(t)
	defer mr.Close()
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sub, err := c.Subscribe(ctx, "depth:BTC-USDT")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	done := make(chan MessageResult, 1)
	go func() {
		msg, recvErr := sub.Receive(ctx)
		done <- MessageResult{msg, recvErr}
	}()

	// miniredis 需要短暂等待订阅就绪。
	time.Sleep(10 * time.Millisecond)
	payload := `{"type":"snapshot","symbol":"BTC-USDT"}`
	if err := c.Publish(ctx, "depth:BTC-USDT", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("receive: %v", res.err)
		}
		if res.msg.Channel != "depth:BTC-USDT" {
			t.Fatalf("channel: %q", res.msg.Channel)
		}
		if res.msg.Payload != payload {
			t.Fatalf("payload: %q", res.msg.Payload)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestClient_Ping(t *testing.T) {
	t.Parallel()
	mr, c := newTestClient(t)
	defer mr.Close()
	defer c.Close()

	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestConfig_Defaults(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	cfg := redis.Config{Addr: mr.Addr()}
	c, err := redis.NewClient(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

type MessageResult struct {
	msg redis.Message
	err error
}

func newTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	c, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return mr, c
}
