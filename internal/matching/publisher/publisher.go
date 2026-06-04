package publisher

import (
	"context"
	"sync"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/metrics"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// Publisher 将事件写入 Kafka。
type Publisher interface {
	Publish(ctx context.Context, out Outbound) error
	// PublishBatch 聚合多条命令的 outbound，按 symbol 分组后各 topic 批量 WriteBatch（组提交路径）。
	PublishBatch(ctx context.Context, outs []Outbound) error
}

// KafkaPublisher 使用 Producer 发布 protobuf 事件。
type KafkaPublisher struct {
	Producer   kafka.Producer
	MatchTopic string
	TradeTopic string
	Metrics    *metrics.Metrics
}

// Publish 写入 match.events 与 trade.events。
func (p *KafkaPublisher) Publish(ctx context.Context, out Outbound) error {
	return p.PublishBatch(ctx, []Outbound{out})
}

// PublishBatch 合并多条 outbound 后发布；单 symbol 时整批各 topic 一次 WriteBatch。
// match 与 trade 均有数据时并行刷盘，墙钟约为 max(match, trade)。
func (p *KafkaPublisher) PublishBatch(ctx context.Context, outs []Outbound) error {
	merged := mergeOutbound(outs)
	if len(merged.MatchEvents) == 0 && len(merged.TradeEvents) == 0 {
		return nil
	}

	matchDur, tradeDur, err := p.publishMerged(ctx, merged)
	if err != nil {
		return err
	}
	if p.Metrics != nil {
		p.Metrics.ObservePublish(matchDur, tradeDur, len(merged.MatchEvents), len(merged.TradeEvents))
	}
	return nil
}

func mergeOutbound(outs []Outbound) Outbound {
	var merged Outbound
	for _, o := range outs {
		merged.MatchEvents = append(merged.MatchEvents, o.MatchEvents...)
		merged.TradeEvents = append(merged.TradeEvents, o.TradeEvents...)
	}
	return merged
}

func (p *KafkaPublisher) publishMerged(ctx context.Context, merged Outbound) (matchDur, tradeDur time.Duration, err error) {
	hasMatch := len(merged.MatchEvents) > 0
	hasTrade := len(merged.TradeEvents) > 0

	switch {
	case hasMatch && hasTrade:
		return p.publishMatchTradeParallel(ctx, merged)
	case hasMatch:
		matchDur, err = p.publishMatchAll(ctx, merged.MatchEvents)
		return matchDur, 0, err
	case hasTrade:
		tradeDur, err = p.publishTradeAll(ctx, merged.TradeEvents)
		return 0, tradeDur, err
	default:
		return 0, 0, nil
	}
}

func (p *KafkaPublisher) publishMatchAll(ctx context.Context, events []*matchingv1.MatchEvent) (time.Duration, error) {
	start := time.Now()
	if err := p.writeMatchBySymbol(ctx, events); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func (p *KafkaPublisher) publishTradeAll(ctx context.Context, events []*matchingv1.TradeEvent) (time.Duration, error) {
	start := time.Now()
	if err := p.writeTradeBySymbol(ctx, events); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func (p *KafkaPublisher) publishMatchTradeParallel(ctx context.Context, merged Outbound) (matchDur, tradeDur time.Duration, err error) {
	var wg sync.WaitGroup
	var matchErr, tradeErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		start := time.Now()
		matchErr = p.writeMatchBySymbol(ctx, merged.MatchEvents)
		matchDur = time.Since(start)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		tradeErr = p.writeTradeBySymbol(ctx, merged.TradeEvents)
		tradeDur = time.Since(start)
	}()
	wg.Wait()

	if matchErr != nil {
		return matchDur, tradeDur, matchErr
	}
	if tradeErr != nil {
		return matchDur, tradeDur, tradeErr
	}
	return matchDur, tradeDur, nil
}

func (p *KafkaPublisher) writeMatchBySymbol(ctx context.Context, events []*matchingv1.MatchEvent) error {
	return writeEventsBySymbol(ctx, p.Producer, p.MatchTopic, events, matchSymbol, marshalMatchBatch)
}

func (p *KafkaPublisher) writeTradeBySymbol(ctx context.Context, events []*matchingv1.TradeEvent) error {
	return writeEventsBySymbol(ctx, p.Producer, p.TradeTopic, events, tradeSymbol, marshalTradeBatch)
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

func (m *MemoryPublisher) Publish(ctx context.Context, out Outbound) error {
	return m.PublishBatch(ctx, []Outbound{out})
}

func (m *MemoryPublisher) PublishBatch(_ context.Context, outs []Outbound) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, out := range outs {
		m.match = append(m.match, out.MatchEvents...)
		m.trade = append(m.trade, out.TradeEvents...)
	}
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
