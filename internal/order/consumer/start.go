package consumer

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// TopicConsumerConfig 单 topic 消费者启动参数。
type TopicConsumerConfig struct {
	Brokers     []string
	Topic       string
	GroupID     string
	Partition   int
	StartOffset int64
}

type loggingProcessor struct {
	log   zerolog.Logger
	inner Processor
}

func (p *loggingProcessor) Process(ctx context.Context, msg kafka.Message) error {
	err := p.inner.Process(ctx, msg)
	if err != nil && StaleEventError(err) {
		p.log.Warn().Err(err).Int64("offset", msg.Offset).Msg("skip stale kafka event")
	}
	return err
}

// RunTopic 创建 reader 并阻塞消费直至 ctx 取消或出错。
func RunTopic(ctx context.Context, log zerolog.Logger, cfg TopicConsumerConfig, h Processor) error {
	reader, err := kafka.NewCommandReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		Partition:   cfg.Partition,
		StartOffset: cfg.StartOffset,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: create reader: %w", cfg.Topic, err)
	}
	defer reader.Close()

	log.Info().
		Str("topic", cfg.Topic).
		Str("group_id", cfg.GroupID).
		Int("partition", cfg.Partition).
		Int64("start_offset", cfg.StartOffset).
		Msg("kafka consumer starting")

	if err := Run(ctx, reader, &loggingProcessor{log: log, inner: h}); err != nil {
		return fmt.Errorf("consumer %s: %w", cfg.Topic, err)
	}
	return nil
}
