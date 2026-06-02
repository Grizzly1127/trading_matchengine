package publisher

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	"google.golang.org/protobuf/proto"
)

// KafkaPublisher 发布 kline.raw（闭合 bar 通知）。
type KafkaPublisher struct {
	writer *kafka.EventWriter
	topic  string
}

func NewKafkaPublisher(writer *kafka.EventWriter, topic string) *KafkaPublisher {
	return &KafkaPublisher{writer: writer, topic: topic}
}

// PublishClosed 写入 Kafka（key=symbol）。
func (p *KafkaPublisher) PublishClosed(ctx context.Context, ev aggregator.ClosedEvent) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("kline kafka publisher: not configured")
	}
	if ev.Symbol == "" {
		return fmt.Errorf("kline kafka publisher: symbol is required")
	}
	closeMs := interval.Interval(ev.Interval).CloseTimeMs(ev.Bar.OpenTimeMs)
	msg := &klinev1.KlineClosedEvent{
		Symbol:      ev.Symbol,
		Interval:    string(ev.Interval),
		OpenTimeMs:  ev.Bar.OpenTimeMs,
		CloseTimeMs: closeMs,
		Open:        &commonv1.Decimal{Value: ev.Bar.Open.String()},
		High:        &commonv1.Decimal{Value: ev.Bar.High.String()},
		Low:         &commonv1.Decimal{Value: ev.Bar.Low.String()},
		Close:       &commonv1.Decimal{Value: ev.Bar.Close.String()},
		Volume:      &commonv1.Decimal{Value: ev.Bar.Volume.String()},
		TsMs:        ev.Bar.UpdatedAtMs,
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kline kafka publisher: marshal: %w", err)
	}
	return p.writer.Write(ctx, p.topic, []byte(ev.Symbol), b)
}

// PublishClosedFromJSON 从闭合 bar JSON 发布（恢复重放场景）。
func (p *KafkaPublisher) PublishClosedFromJSON(ctx context.Context, j bar.JSON) error {
	iv, err := interval.Parse(j.Interval)
	if err != nil {
		return err
	}
	open, err := bar.ParseDecimal(j.Open)
	if err != nil {
		return err
	}
	high, err := bar.ParseDecimal(j.High)
	if err != nil {
		return err
	}
	low, err := bar.ParseDecimal(j.Low)
	if err != nil {
		return err
	}
	closeP, err := bar.ParseDecimal(j.Close)
	if err != nil {
		return err
	}
	vol, err := bar.ParseDecimal(j.Volume)
	if err != nil {
		return err
	}
	return p.PublishClosed(ctx, aggregator.ClosedEvent{
		Symbol:   j.Symbol,
		Interval: iv,
		Bar: bar.OHLCV{
			OpenTimeMs:  j.OpenTimeMs,
			Open:        open,
			High:        high,
			Low:         low,
			Close:       closeP,
			Volume:      vol,
			UpdatedAtMs: j.TimestampMs,
		},
	})
}
