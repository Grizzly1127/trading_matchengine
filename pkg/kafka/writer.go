package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// WriterConfig 配置事件发布 Writer。
type WriterConfig struct {
	Brokers []string
	// RequiredAcks 默认 RequireAll。
	RequiredAcks kafkago.RequiredAcks
}

// EventWriter 向多个 topic 写入（按 topic 懒创建 writer）。
type EventWriter struct {
	brokers      []string
	requiredAcks kafkago.RequiredAcks
	writers      map[string]*kafkago.Writer
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
		writers:      make(map[string]*kafkago.Writer),
	}
}

// Write 写入指定 topic（由 broker 选择 partition）。
func (w *EventWriter) Write(ctx context.Context, topic string, key, value []byte) error {
	return w.WriteAt(ctx, topic, -1, key, value)
}

// WriteAt 写入指定 topic 与 partition；-1 表示由 broker 选择 partition。
func (w *EventWriter) WriteAt(ctx context.Context, topic string, partition int, key, value []byte) error {
	writer, ok := w.writers[topic]
	if !ok {
		if len(w.brokers) == 0 {
			return fmt.Errorf("kafka: brokers required")
		}
		writer = &kafkago.Writer{
			Addr:         kafkago.TCP(w.brokers...),
			Topic:        topic,
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: w.requiredAcks,
		}
		w.writers[topic] = writer
	}
	msg := kafkago.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}
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
