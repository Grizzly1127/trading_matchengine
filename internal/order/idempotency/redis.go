package idempotency

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

const keyPrefix = "idempotent:order:"

// RedisCache 下单幂等热缓存；权威数据仍在 PostgreSQL `client_order_idempotency`。
type RedisCache struct {
	rdb *redis.Client
	ttl time.Duration
}

// NewRedisCache 创建缓存；ttl<=0 时默认 24h。
func NewRedisCache(rdb *redis.Client, ttl time.Duration) *RedisCache {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &RedisCache{rdb: rdb, ttl: ttl}
}

func cacheKey(userID uint64, clientOrderID string) string {
	return fmt.Sprintf("%s%d:%s", keyPrefix, userID, clientOrderID)
}

// Lookup 命中时返回已存在的 order_id。
func (c *RedisCache) Lookup(ctx context.Context, userID uint64, clientOrderID string) (orderID uint64, ok bool, err error) {
	if c == nil || c.rdb == nil || userID == 0 || strings.TrimSpace(clientOrderID) == "" {
		return 0, false, nil
	}
	val, err := c.rdb.Get(ctx, cacheKey(userID, clientOrderID))
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	id, err := strconv.ParseUint(strings.TrimSpace(val), 10, 64)
	if err != nil || id == 0 {
		return 0, false, nil
	}
	return id, true, nil
}

// Remember 写入 order_id；失败不阻断主流程（由调用方决定是否忽略 error）。
func (c *RedisCache) Remember(ctx context.Context, userID uint64, clientOrderID string, orderID uint64) error {
	if c == nil || c.rdb == nil || userID == 0 || orderID == 0 || strings.TrimSpace(clientOrderID) == "" {
		return nil
	}
	return c.rdb.Set(ctx, cacheKey(userID, clientOrderID), strconv.FormatUint(orderID, 10), c.ttl)
}
