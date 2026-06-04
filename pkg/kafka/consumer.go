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
	// WriteBatch 同一 topic、同一 key 批量写入（单条命令的多条 match/trade 事件应走此路径）。
	WriteBatch(ctx context.Context, topic string, key []byte, values [][]byte) error
	Close() error
}
