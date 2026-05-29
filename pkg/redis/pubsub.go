package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Message Pub/Sub 收到的消息。
type Message struct {
	Channel string
	Payload string
}

// Subscriber 封装 Pub/Sub 订阅；非并发安全。
type Subscriber struct {
	pubsub *goredis.PubSub
}

// Receive 阻塞直到收到下一条消息或 ctx 取消。
func (s *Subscriber) Receive(ctx context.Context) (Message, error) {
	if s == nil || s.pubsub == nil {
		return Message{}, fmt.Errorf("redis: subscriber is nil")
	}
	for {
		raw, err := s.pubsub.Receive(ctx)
		if err != nil {
			return Message{}, fmt.Errorf("redis: receive: %w", err)
		}
		switch msg := raw.(type) {
		case *goredis.Message:
			return Message{Channel: msg.Channel, Payload: msg.Payload}, nil
		case *goredis.Subscription:
			// 订阅确认，继续等待业务消息。
			continue
		default:
			continue
		}
	}
}

// Close 关闭订阅。
func (s *Subscriber) Close() error {
	if s == nil || s.pubsub == nil {
		return nil
	}
	return s.pubsub.Close()
}
