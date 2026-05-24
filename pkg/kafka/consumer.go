package kafka

import "context"

// Consumer 消费 Kafka 消息并支持手动提交 offset。
type Consumer interface {
	Read(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msg Message) error
	Close() error
}

// Producer 向 Kafka 写入消息。
type Producer interface {
	Write(ctx context.Context, topic string, key, value []byte) error
	Close() error
}
