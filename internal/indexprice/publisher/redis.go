package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/shopspring/decimal"
)

// RedisPublisher 写 index:{symbol} 并 Pub/Sub。
type RedisPublisher struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewRedisPublisher(rdb *redis.Client, ttl time.Duration) *RedisPublisher {
	return &RedisPublisher{rdb: rdb, ttl: ttl}
}

type indexJSON struct {
	Symbol  string   `json:"symbol"`
	Price   string   `json:"price"`
	Ts      int64    `json:"ts"`
	Sources []string `json:"sources"`
}

// Publish 写入 Redis 并发布频道 index:{symbol}。
func (p *RedisPublisher) Publish(ctx context.Context, symbol string, price decimal.Decimal, ts time.Time, sources []string) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	payload, err := json.Marshal(indexJSON{
		Symbol:  symbol,
		Price:   price.String(),
		Ts:      ts.UnixMilli(),
		Sources: sources,
	})
	if err != nil {
		return fmt.Errorf("redis publisher: marshal: %w", err)
	}
	key := "index:" + symbol
	ch := "index:" + symbol
	if err := p.rdb.Set(ctx, key, string(payload), p.ttl); err != nil {
		return err
	}
	_ = p.rdb.Publish(ctx, ch, string(payload))
	return nil
}
