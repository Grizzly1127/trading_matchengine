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
