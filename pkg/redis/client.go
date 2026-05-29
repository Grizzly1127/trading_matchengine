package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Client 封装 go-redis，提供行情场景常用的 KV 与 Pub/Sub。
type Client struct {
	rdb *goredis.Client
}

// NewClient 创建 Redis 客户端；调用方负责 Close。
func NewClient(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	return &Client{rdb: rdb}, nil
}

// NewClientFrom 供测试注入底层客户端。
func NewClientFrom(rdb *goredis.Client) *Client {
	if rdb == nil {
		panic("redis: nil client")
	}
	return &Client{rdb: rdb}
}

// Ping 检查连通性。
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis: client is nil")
	}
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping: %w", err)
	}
	return nil
}

// Get 读取字符串值；key 不存在时返回 goredis.Nil 包装错误。
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if c == nil || c.rdb == nil {
		return "", fmt.Errorf("redis: client is nil")
	}
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("redis: get %q: %w", key, err)
	}
	return val, nil
}

// Set 写入字符串；ttl<=0 表示不过期。
func (c *Client) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis: client is nil")
	}
	if err := c.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redis: set %q: %w", key, err)
	}
	return nil
}

// Del 删除一个或多个 key。
func (c *Client) Del(ctx context.Context, keys ...string) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis: client is nil")
	}
	if len(keys) == 0 {
		return nil
	}
	if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis: del: %w", err)
	}
	return nil
}

// LPush 从列表左侧入队。
func (c *Client) LPush(ctx context.Context, key string, values ...string) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis: client is nil")
	}
	if len(values) == 0 {
		return nil
	}
	args := make([]interface{}, len(values))
	for i, v := range values {
		args[i] = v
	}
	if err := c.rdb.LPush(ctx, key, args...).Err(); err != nil {
		return fmt.Errorf("redis: lpush %q: %w", key, err)
	}
	return nil
}

// RPop 从列表右侧弹出；列表为空时返回 ("", nil)。
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	if c == nil || c.rdb == nil {
		return "", fmt.Errorf("redis: client is nil")
	}
	val, err := c.rdb.RPop(ctx, key).Result()
	if err == goredis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("redis: rpop %q: %w", key, err)
	}
	return val, nil
}

// LLen 返回列表长度。
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, fmt.Errorf("redis: client is nil")
	}
	n, err := c.rdb.LLen(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: llen %q: %w", key, err)
	}
	return n, nil
}

// ScanKeys 使用 SCAN 遍历匹配 key，对每个 key 调用 fn（fn 返回 error 则中止）。
func (c *Client) ScanKeys(ctx context.Context, pattern string, fn func(key string) error) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis: client is nil")
	}
	if fn == nil {
		return fmt.Errorf("redis: scan fn is required")
	}
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("redis: scan %q: %w", pattern, err)
		}
		for _, key := range keys {
			if err := fn(key); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// Publish 向频道发布消息。
func (c *Client) Publish(ctx context.Context, channel string, message string) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis: client is nil")
	}
	if err := c.rdb.Publish(ctx, channel, message).Err(); err != nil {
		return fmt.Errorf("redis: publish %q: %w", channel, err)
	}
	return nil
}

// Subscribe 订阅一个或多个频道；调用方须 Close Subscriber。
func (c *Client) Subscribe(ctx context.Context, channels ...string) (*Subscriber, error) {
	if c == nil || c.rdb == nil {
		return nil, fmt.Errorf("redis: client is nil")
	}
	if len(channels) == 0 {
		return nil, fmt.Errorf("redis: subscribe: at least one channel required")
	}
	pubsub := c.rdb.Subscribe(ctx, channels...)
	if err := pubsub.Ping(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("redis: subscribe ping: %w", err)
	}
	return &Subscriber{pubsub: pubsub}, nil
}

// PSubscribe 按模式订阅频道（如 depth:*）。
func (c *Client) PSubscribe(ctx context.Context, patterns ...string) (*Subscriber, error) {
	if c == nil || c.rdb == nil {
		return nil, fmt.Errorf("redis: client is nil")
	}
	if len(patterns) == 0 {
		return nil, fmt.Errorf("redis: psubscribe: at least one pattern required")
	}
	pubsub := c.rdb.PSubscribe(ctx, patterns...)
	if err := pubsub.Ping(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("redis: psubscribe ping: %w", err)
	}
	return &Subscriber{pubsub: pubsub}, nil
}

// Close 关闭连接。
func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

// Underlying 返回底层 go-redis 客户端（仅测试或高级用法）。
func (c *Client) Underlying() *goredis.Client {
	if c == nil {
		return nil
	}
	return c.rdb
}
