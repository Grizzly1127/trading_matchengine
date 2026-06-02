package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

func TestRedisCacheLookupRemember(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb, err := redis.NewClient(redis.Config{Addr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	cache := NewRedisCache(rdb, time.Hour)
	ctx := context.Background()

	id, ok, err := cache.Lookup(ctx, 1, "c1")
	if err != nil || ok || id != 0 {
		t.Fatalf("miss: id=%d ok=%v err=%v", id, ok, err)
	}

	if err := cache.Remember(ctx, 1, "c1", 42); err != nil {
		t.Fatal(err)
	}

	id, ok, err = cache.Lookup(ctx, 1, "c1")
	if err != nil || !ok || id != 42 {
		t.Fatalf("hit: id=%d ok=%v err=%v", id, ok, err)
	}

	key := cacheKey(1, "c1")
	if mr.TTL(key) <= 0 {
		t.Fatalf("expected ttl on %s", key)
	}
}
