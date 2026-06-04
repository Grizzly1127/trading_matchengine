package publisher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/metrics"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
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
	Metrics    *metrics.Metrics
}

// Publish 写入 match.events 与 trade.events。
// 当 match 与 trade 均有事件时并行 WriteBatch，端到端 Publish 耗时约为 max(match, trade) 而非二者之和。
func (p *KafkaPublisher) Publish(ctx context.Context, out Outbound) error {
	hasMatch := len(out.MatchEvents) > 0
	hasTrade := len(out.TradeEvents) > 0

	var matchDur, tradeDur time.Duration
	var err error

	switch {
	case hasMatch && hasTrade:
		matchDur, tradeDur, err = p.publishMatchTradeParallel(ctx, out)
	case hasMatch:
		matchDur, err = p.publishMatch(ctx, out)
	case hasTrade:
		tradeDur, err = p.publishTrade(ctx, out)
	}

	if err != nil {
		return err
	}
	if p.Metrics != nil {
		p.Metrics.ObservePublish(matchDur, tradeDur, len(out.MatchEvents), len(out.TradeEvents))
	}
	return nil
}

func (p *KafkaPublisher) publishMatch(ctx context.Context, out Outbound) (time.Duration, error) {
	key := []byte(out.MatchEvents[0].GetSymbol())
	vals, err := marshalMatchBatch(out.MatchEvents)
	if err != nil {
		return 0, fmt.Errorf("publisher: marshal match events: %w", err)
	}
	start := time.Now()
	if err := p.Producer.WriteBatch(ctx, p.MatchTopic, key, vals); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func (p *KafkaPublisher) publishTrade(ctx context.Context, out Outbound) (time.Duration, error) {
	key := []byte(out.TradeEvents[0].GetTrade().GetSymbol())
	vals, err := marshalTradeBatch(out.TradeEvents)
	if err != nil {
		return 0, fmt.Errorf("publisher: marshal trade events: %w", err)
	}
	start := time.Now()
	if err := p.Producer.WriteBatch(ctx, p.TradeTopic, key, vals); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func (p *KafkaPublisher) publishMatchTradeParallel(ctx context.Context, out Outbound) (matchDur, tradeDur time.Duration, err error) {
	matchKey := []byte(out.MatchEvents[0].GetSymbol())
	matchVals, err := marshalMatchBatch(out.MatchEvents)
	if err != nil {
		return 0, 0, fmt.Errorf("publisher: marshal match events: %w", err)
	}
	tradeKey := []byte(out.TradeEvents[0].GetTrade().GetSymbol())
	tradeVals, err := marshalTradeBatch(out.TradeEvents)
	if err != nil {
		return 0, 0, fmt.Errorf("publisher: marshal trade events: %w", err)
	}

	var wg sync.WaitGroup
	var matchErr, tradeErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		start := time.Now()
		matchErr = p.Producer.WriteBatch(ctx, p.MatchTopic, matchKey, matchVals)
		matchDur = time.Since(start)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		tradeErr = p.Producer.WriteBatch(ctx, p.TradeTopic, tradeKey, tradeVals)
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
