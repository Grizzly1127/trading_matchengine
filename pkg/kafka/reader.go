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
	// StartOffset：-1 从最新；0 从最早（consumer group）；>0 为下一条待消费的绝对 offset（WAL 恢复，固定分区，不用 group）。
	StartOffset int64
}

// CommandReader 封装 kafka-go Reader，固定 partition、手动 commit。
type CommandReader struct {
	reader *kafkago.Reader
	// partitionMode 为 true 时不向 broker 提交 offset（位点由 WAL 等外部状态维护）。
	partitionMode bool
}

// planReaderStart 将业务 StartOffset 映射为 kafka-go 参数。
func planReaderStart(cfg ReaderConfig) (groupID string, readerStart int64, seekTo int64) {
	groupID = cfg.GroupID
	readerStart = cfg.StartOffset
	seekTo = -1
	if cfg.StartOffset > 0 {
		// consumer group 仅支持 FirstOffset/LastOffset，显式位点必须用分区 reader + SetOffset。
		groupID = ""
		seekTo = cfg.StartOffset
		return groupID, kafkago.FirstOffset, seekTo
	}
	if readerStart < 0 {
		readerStart = kafkago.LastOffset
	}
	return groupID, readerStart, seekTo
}

// NewCommandReader 创建消费者；StartOffset 指定恢复位点（下一条待消费的 kafka offset）。
func NewCommandReader(cfg ReaderConfig) (*CommandReader, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka: topic required")
	}

	groupID, readerStart, seekTo := planReaderStart(cfg)

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     groupID,
		Partition:   cfg.Partition,
		StartOffset: readerStart,
	})
	cr := &CommandReader{
		reader:        r,
		partitionMode: groupID == "",
	}
	if seekTo >= 0 {
		if err := r.SetOffset(seekTo); err != nil {
			_ = r.Close()
			return nil, fmt.Errorf("kafka: seek offset %d: %w", seekTo, err)
		}
	}
	return cr, nil
}

// Read 拉取下一条消息（不自动 commit，由调用方 Process 成功后 Commit）。
func (c *CommandReader) Read(ctx context.Context) (Message, error) {
	m, err := c.reader.FetchMessage(ctx)
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

// Commit 提交已处理消息的 offset（仅 consumer group 模式；分区模式由 WAL 维护位点，此处为 no-op）。
func (c *CommandReader) Commit(ctx context.Context, msg Message) error {
	if c.partitionMode {
		return nil
	}
	return c.reader.CommitMessages(ctx, kafkago.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	})
}

// ReadLag 返回当前分区消费 lag（kafka-go：high watermark - 当前读位置）。
func (c *CommandReader) ReadLag(ctx context.Context) (int64, error) {
	return c.reader.ReadLag(ctx)
}

// Close 关闭 reader。
func (c *CommandReader) Close() error {
	return c.reader.Close()
}
