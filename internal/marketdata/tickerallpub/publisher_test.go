package tickerallpub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/Grizzly1127/trading_matchengine/pkg/tickerall"
	"github.com/alicebob/miniredis/v2"
)

func TestPublisherDeltaAfterPriceChange(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	pub := publisher.NewRedisPublisher(rdb)
	st := store.New()
	if err := st.ApplyTrade("BTC-USDT", "100", "1", 1000); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := rdb.Subscribe(ctx, "ticker@all:USDT")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	p := &Publisher{
		Store:          st,
		Redis:          pub,
		Interval:       20 * time.Millisecond,
		HeartbeatEvery: time.Hour,
		QuoteAssets:    []string{"USDT"},
	}
	go p.Run(ctx)
	time.Sleep(5 * time.Millisecond)

	first, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var f1 tickerall.Frame
	if err := json.Unmarshal([]byte(first.Payload), &f1); err != nil {
		t.Fatal(err)
	}
	if f1.Type != tickerall.TypeSnapshot {
		t.Fatalf("first type=%s", f1.Type)
	}

	if err := st.ApplyTrade("BTC-USDT", "101", "0.5", 2000); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting delta")
		}
		second, err := sub.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var f2 tickerall.Frame
		if err := json.Unmarshal([]byte(second.Payload), &f2); err != nil {
			t.Fatal(err)
		}
		if f2.Type == tickerall.TypeDelta {
			return
		}
	}
}
