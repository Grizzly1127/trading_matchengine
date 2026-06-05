package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// WriterConfig 配置事件发布 Writer。
type WriterConfig struct {
	Brokers []string
	// RequiredAcks 默认 RequireAll。
	RequiredAcks kafkago.RequiredAcks
	// BatchSize 默认 100；压测工具可加大以减少 round-trip。
	BatchSize int
	// BatchTimeout 默认 1s；单条 WriteMessages 在未满批时会等待至超时，压测宜调小。
	BatchTimeout time.Duration
	// Compression 默认不压缩；dev 可试 lz4 降低带宽（CPU 换延迟）。
	Compression kafkago.Compression
}

// EventWriter 向多个 topic 写入（按 topic 懒创建 writer）。
type EventWriter struct {
	brokers       []string
	requiredAcks  kafkago.RequiredAcks
	batchSize     int
	batchTimeout  time.Duration
	compression   kafkago.Compression
	writers       map[string]*kafkago.Writer
}

// NewEventWriter 创建 Producer。
func NewEventWriter(cfg WriterConfig) *EventWriter {
	acks := cfg.RequiredAcks
	if acks == 0 {
		acks = kafkago.RequireAll
	}
	return &EventWriter{
		brokers:      cfg.Brokers,
		requiredAcks: acks,
		batchSize:    cfg.BatchSize,
		batchTimeout: cfg.BatchTimeout,
		compression:  cfg.Compression,
		writers:      make(map[string]*kafkago.Writer),
	}
}

// WriteBatchAt 批量写入同一 topic/partition（压测与 Outbox 批量投递）。
func (w *EventWriter) WriteBatchAt(ctx context.Context, topic string, partition int, key []byte, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}
	writer, err := w.writerFor(topic)
	if err != nil {
		return err
	}
	msgs := make([]kafkago.Message, len(values))
	for i, v := range values {
		msgs[i] = kafkago.Message{Key: key, Value: v}
		if partition >= 0 {
			msgs[i].Partition = partition
		}
	}
	return writer.WriteMessages(ctx, msgs...)
}

func (w *EventWriter) writerFor(topic string) (*kafkago.Writer, error) {
	writer, ok := w.writers[topic]
	if !ok {
		if len(w.brokers) == 0 {
			return nil, fmt.Errorf("kafka: brokers required")
		}
		batchSize := 100
		// 默认 10ms：避免 Publish 循环逐条 Write 时每条等满 1s（L2 压测曾出现 ~2s/命令）。
		batchTimeout := 10 * time.Millisecond
		if w.batchSize > 0 {
			batchSize = w.batchSize
		}
		if w.batchTimeout > 0 {
			batchTimeout = w.batchTimeout
		}
		writer = &kafkago.Writer{
			Addr:                   kafkago.TCP(w.brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.LeastBytes{},
			RequiredAcks:           w.requiredAcks,
			BatchSize:              batchSize,
			BatchTimeout:           batchTimeout,
			Compression:            w.compression,
			AllowAutoTopicCreation: false,
		}
		w.writers[topic] = writer
	}
	return writer, nil
}

// Write 写入指定 topic（由 broker 选择 partition）。
func (w *EventWriter) Write(ctx context.Context, topic string, key, value []byte) error {
	return w.WriteAt(ctx, topic, -1, key, value)
}

// WriteBatch 实现 Producer；一次 WriteMessages 刷出多条事件。
func (w *EventWriter) WriteBatch(ctx context.Context, topic string, key []byte, values [][]byte) error {
	return w.WriteBatchAt(ctx, topic, -1, key, values)
}

// WriteAt 写入指定 topic 与 partition；-1 表示由 broker 选择 partition。
func (w *EventWriter) WriteAt(ctx context.Context, topic string, partition int, key, value []byte) error {
	writer, err := w.writerFor(topic)
	if err != nil {
		return err
	}
	msg := kafkago.Message{Key: key, Value: value}
	if partition >= 0 {
		msg.Partition = partition
	}
	return writer.WriteMessages(ctx, msg)
}

// Close 关闭所有 writer。
func (w *EventWriter) Close() error {
	var first error
	for _, wr := range w.writers {
		if err := wr.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
