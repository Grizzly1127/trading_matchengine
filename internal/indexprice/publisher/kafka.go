package publisher

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
)

// KafkaPublisher 发布 index.price topic。
type KafkaPublisher struct {
	writer *kafka.EventWriter
	topic  string
}

func NewKafkaPublisher(writer *kafka.EventWriter, topic string) *KafkaPublisher {
	return &KafkaPublisher{writer: writer, topic: topic}
}

// Publish 写入 Kafka（key=symbol）。
func (p *KafkaPublisher) Publish(ctx context.Context, symbol string, price decimal.Decimal, tsMs int64, sources []string) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("kafka publisher: not configured")
	}
	ev := &indexv1.IndexPriceEvent{
		Symbol:  symbol,
		Price:   &commonv1.Decimal{Value: price.String()},
		TsMs:    tsMs,
		Sources: sources,
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		return fmt.Errorf("kafka publisher: marshal: %w", err)
	}
	return p.writer.Write(ctx, p.topic, []byte(symbol), b)
}
