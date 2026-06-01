package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

// RedisPublisher 将聚合结果写入 Redis Key，并发布 Pub/Sub。
type RedisPublisher struct {
	rdb *redis.Client
}

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

type tickerJSON struct {
	Symbol             string `json:"symbol"`
	LastPrice          string `json:"last_price"`
	OpenPrice          string `json:"open_price"`
	HighPrice          string `json:"high_price"`
	LowPrice           string `json:"low_price"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quote_volume"`
	PriceChangePercent string `json:"price_change_percent"`
	// 统一用 unix ms，便于 WS 帧直接复用。
	TimestampMs int64 `json:"ts"`
}

type depthJSON struct {
	Type         string     `json:"type"`
	Symbol       string     `json:"symbol"`
	LastUpdateID uint64     `json:"last_update_id"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
	TimestampMs  int64      `json:"ts"`
}

type tradeJSON struct {
	TradeID      string `json:"trade_id"`
	Symbol       string `json:"symbol"`
	Price        string `json:"price"`
	Quantity     string `json:"quantity"`
	MakerOrderID string `json:"maker_order_id"`
	TakerOrderID string `json:"taker_order_id"`
	TimestampMs  int64  `json:"ts"`
}

type tickerAllJSON struct {
	SnapshotID   string       `json:"snapshot_id"`
	SnapshotTime int64        `json:"snapshot_time"`
	Count        int          `json:"count"`
	Items        []tickerJSON `json:"items"`
}

// PublishTrade 发布公开市场成交到 `trade:{symbol}`（WS 扇出，无持久 Key）。
func (p *RedisPublisher) PublishTrade(ctx context.Context, tr tradeJSON) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	if tr.Symbol == "" {
		return fmt.Errorf("redis publisher: symbol is required")
	}
	payload, err := json.Marshal(tr)
	if err != nil {
		return fmt.Errorf("redis publisher: marshal trade: %w", err)
	}
	return p.rdb.Publish(ctx, "trade:"+tr.Symbol, string(payload))
}

// TradePayload 构造公开市场成交 JSON 载荷。
func TradePayload(tradeID uint64, symbol, price, qty string, makerOrderID, takerOrderID uint64, ts int64) tradeJSON {
	return tradeJSON{
		TradeID:      fmt.Sprintf("%d", tradeID),
		Symbol:       symbol,
		Price:        price,
		Quantity:     qty,
		MakerOrderID: fmt.Sprintf("%d", makerOrderID),
		TakerOrderID: fmt.Sprintf("%d", takerOrderID),
		TimestampMs:  ts,
	}
}

// PublishTicker 写 `ticker:{symbol}`，并发布 `ticker:{symbol}` channel。
func (p *RedisPublisher) PublishTicker(ctx context.Context, symbol string, t store.TickerState) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	if symbol == "" {
		return fmt.Errorf("redis publisher: symbol is required")
	}

	// TODO: 使用 protobuf 序列化
	payload, err := json.Marshal(tickerJSON{
		Symbol:             symbol,
		LastPrice:          store.FormatDecimal(t.LastPrice),
		OpenPrice:          store.FormatDecimal(t.OpenPrice),
		HighPrice:          store.FormatDecimal(t.HighPrice),
		LowPrice:           store.FormatDecimal(t.LowPrice),
		Volume:             store.FormatDecimal(t.Volume),
		QuoteVolume:        store.FormatDecimal(t.QuoteVolume),
		PriceChangePercent: store.FormatPercent(t.PriceChangePercent),
		TimestampMs:        t.UpdatedAtMs,
	})
	if err != nil {
		return fmt.Errorf("redis publisher: marshal ticker: %w", err)
	}

	key := "ticker:" + symbol
	ch := "ticker:" + symbol
	if err := p.rdb.Set(ctx, key, string(payload), 0); err != nil {
		return err
	}
	// Pub/Sub 允许丢：这里不做重试阻塞；调用方可记录 metrics。
	_ = p.rdb.Publish(ctx, ch, string(payload))
	return nil
}

// PublishDepth 写 `depth:{symbol}`，并发布 `depth:{symbol}` channel。
func (p *RedisPublisher) PublishDepth(ctx context.Context, snap store.OrderBookSnapshot) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	if snap.Symbol == "" {
		return fmt.Errorf("redis publisher: symbol is required")
	}

	payload, err := json.Marshal(depthJSON{
		Type:         "snapshot",
		Symbol:       snap.Symbol,
		LastUpdateID: snap.LastUpdateID,
		Bids:         levelsToJSON(snap.Bids),
		Asks:         levelsToJSON(snap.Asks),
		TimestampMs:  snap.UpdatedAtMs,
	})
	if err != nil {
		return fmt.Errorf("redis publisher: marshal depth: %w", err)
	}
	key := "depth:" + snap.Symbol
	ch := "depth:" + snap.Symbol
	if err := p.rdb.Set(ctx, key, string(payload), 0); err != nil {
		return err
	}
	_ = p.rdb.Publish(ctx, ch, string(payload))
	return nil
}

// PublishDepthDelta 仅发布变化档位（delta）。
func (p *RedisPublisher) PublishDepthDelta(ctx context.Context, symbol string, lastUpdateID uint64, bids, asks []store.PriceLevel, ts int64) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	payload, err := json.Marshal(depthJSON{
		Type:         "delta",
		Symbol:       symbol,
		LastUpdateID: lastUpdateID,
		Bids:         levelsToJSON(bids),
		Asks:         levelsToJSON(asks),
		TimestampMs:  ts,
	})
	if err != nil {
		return fmt.Errorf("redis publisher: marshal depth delta: %w", err)
	}
	return p.rdb.Publish(ctx, "depth:"+symbol, string(payload))
}

// SetTickerAllREST 写 `ticker:all:{quote}`（REST / gRPC 同源快照，长字段名）。
func (p *RedisPublisher) SetTickerAllREST(ctx context.Context, snap store.TickerAllSnapshot) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	payload, err := marshalTickerAllREST(snap)
	if err != nil {
		return err
	}
	key := tickerAllRedisKey(snap.QuoteAsset)
	return p.rdb.Set(ctx, key, string(payload), 0)
}

// PublishTickerAllWS 向 Pub/Sub 发布 §8.2 WS 帧（snapshot/delta/heartbeat）。
func (p *RedisPublisher) PublishTickerAllWS(ctx context.Context, channel string, payload []byte) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	if channel == "" {
		return fmt.Errorf("redis publisher: channel is required")
	}
	return p.rdb.Publish(ctx, channel, string(payload))
}

func marshalTickerAllREST(snap store.TickerAllSnapshot) ([]byte, error) {
	jsonItems := make([]tickerJSON, 0, len(snap.Items))
	for _, item := range snap.Items {
		jsonItems = append(jsonItems, tickerJSON{
			Symbol:             item.Symbol,
			LastPrice:          store.FormatDecimal(item.LastPrice),
			OpenPrice:          store.FormatDecimal(item.OpenPrice),
			HighPrice:          store.FormatDecimal(item.HighPrice),
			LowPrice:           store.FormatDecimal(item.LowPrice),
			Volume:             store.FormatDecimal(item.Volume),
			QuoteVolume:        store.FormatDecimal(item.QuoteVolume),
			PriceChangePercent: store.FormatPercent(item.PriceChangePercent),
			TimestampMs:        item.UpdatedAtMs,
		})
	}
	payload, err := json.Marshal(tickerAllJSON{
		SnapshotID:   snap.SnapshotID,
		SnapshotTime: snap.SnapshotTime,
		Count:        snap.Count,
		Items:        jsonItems,
	})
	if err != nil {
		return nil, fmt.Errorf("redis publisher: marshal ticker all: %w", err)
	}
	return payload, nil
}

func tickerAllRedisKey(quoteAsset string) string {
	q := strings.TrimSpace(quoteAsset)
	if q == "" {
		return "ticker:all:ALL"
	}
	return "ticker:all:" + q
}

// PublishHeartbeat 预留：后续需要可以发布服务心跳。
func (p *RedisPublisher) PublishHeartbeat(ctx context.Context, service string) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("redis publisher: not configured")
	}
	now := time.Now().UnixMilli()
	return p.rdb.Set(ctx, "svc:heartbeat:"+service, fmt.Sprintf("%d", now), 10*time.Second)
}

func levelsToJSON(levels []store.PriceLevel) [][]string {
	out := make([][]string, 0, len(levels))
	for _, lv := range levels {
		out = append(out, []string{lv.Price, lv.Quantity})
	}
	return out
}
