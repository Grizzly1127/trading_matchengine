package publisher

import (
	"context"
	"fmt"
	"sync"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"google.golang.org/protobuf/proto"
)

// Publisher 将事件写入 Kafka。
type Publisher interface {
	Publish(ctx context.Context, out Outbound) error
}

// KafkaPublisher 使用 Producer 发布 protobuf 事件。
type KafkaPublisher struct {
	Producer   kafka.Producer
	MatchTopic string
	TradeTopic string
}

// Publish 写入 match.events 与 trade.events。
func (p *KafkaPublisher) Publish(ctx context.Context, out Outbound) error {
	for _, ev := range out.MatchEvents {
		b, err := proto.Marshal(ev)
		if err != nil {
			return fmt.Errorf("publisher: marshal match event: %w", err)
		}
		key := []byte(ev.GetSymbol())
		if err := p.Producer.Write(ctx, p.MatchTopic, key, b); err != nil {
			return err
		}
	}
	for _, ev := range out.TradeEvents {
		b, err := proto.Marshal(ev)
		if err != nil {
			return fmt.Errorf("publisher: marshal trade event: %w", err)
		}
		key := []byte(ev.GetTrade().GetSymbol())
		if err := p.Producer.Write(ctx, p.TradeTopic, key, b); err != nil {
			return err
		}
	}
	return nil
}

// MemoryPublisher 测试用内存发布器。
type MemoryPublisher struct {
	mu    sync.Mutex
	match []*matchingv1.MatchEvent
	trade []*matchingv1.TradeEvent
}

// NewMemory 创建内存 Publisher。
func NewMemory() *MemoryPublisher {
	return &MemoryPublisher{}
}

func (m *MemoryPublisher) Publish(_ context.Context, out Outbound) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.match = append(m.match, out.MatchEvents...)
	m.trade = append(m.trade, out.TradeEvents...)
	return nil
}

func (m *MemoryPublisher) MatchEvents() []*matchingv1.MatchEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*matchingv1.MatchEvent(nil), m.match...)
}

func (m *MemoryPublisher) TradeEvents() []*matchingv1.TradeEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*matchingv1.TradeEvent(nil), m.trade...)
}

func (m *MemoryPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.match = nil
	m.trade = nil
}
