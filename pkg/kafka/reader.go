package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// ReaderConfig 配置命令 topic 消费者。
type ReaderConfig struct {
	Brokers   []string
	Topic     string
	GroupID   string
	Partition int
	// StartOffset 为 -1 表示从最新开始；否则为显式 offset。
	StartOffset int64
}

// CommandReader 封装 kafka-go Reader，固定 partition、手动 commit。
type CommandReader struct {
	reader *kafkago.Reader
}

// NewCommandReader 创建消费者；StartOffset 指定恢复位点（含已提交的下一条 offset）。
func NewCommandReader(cfg ReaderConfig) (*CommandReader, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka: topic required")
	}

	start := cfg.StartOffset
	if start < 0 {
		start = kafkago.LastOffset
	}

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		Partition:   cfg.Partition,
		StartOffset: start,
	})
	return &CommandReader{reader: r}, nil
}

// Read 拉取下一条消息。
func (c *CommandReader) Read(ctx context.Context) (Message, error) {
	m, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
	}, nil
}

// Commit 提交已处理消息的 offset（kafka-go 提交的是 offset+1）。
func (c *CommandReader) Commit(ctx context.Context, msg Message) error {
	return c.reader.CommitMessages(ctx, kafkago.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	})
}

// Close 关闭 reader。
func (c *CommandReader) Close() error {
	return c.reader.Close()
}
