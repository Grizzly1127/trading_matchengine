package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

const (
	pendingCloseKey = "kline:pending:close"
	openKeyPrefix   = "kline:open:"
)

// RedisPublisher K 线 Redis 快照与 Pub/Sub。
type RedisPublisher struct {
	rdb *redis.Client
}

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

func openKey(symbol string, iv interval.Interval) string {
	return openKeyPrefix + symbol + ":" + string(iv)
}

func pubChannel(symbol string, iv interval.Interval) string {
	return "kline:" + symbol + ":" + string(iv)
}

// SaveOpenBar 写未闭合 bar 快照。
func (p *RedisPublisher) SaveOpenBar(ctx context.Context, symbol string, iv interval.Interval, b bar.OHLCV) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("kline publisher: not configured")
	}
	payload, err := json.Marshal(bar.ToJSON(symbol, iv, b, false))
	if err != nil {
		return fmt.Errorf("marshal open bar: %w", err)
	}
	ttl := iv.Duration() + time.Minute
	return p.rdb.Set(ctx, openKey(symbol, iv), string(payload), ttl)
}

// PublishOpenUpdate 推送未闭合 bar 更新（不写快照，调用方应先 SaveOpenBar）。
func (p *RedisPublisher) PublishOpenUpdate(ctx context.Context, symbol string, iv interval.Interval, b bar.OHLCV) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("kline publisher: not configured")
	}
	payload, err := json.Marshal(bar.ToJSON(symbol, iv, b, false))
	if err != nil {
		return fmt.Errorf("marshal kline: %w", err)
	}
	return p.rdb.Publish(ctx, pubChannel(symbol, iv), string(payload))
}

// PublishClosed 推送已闭合 bar，并删除 open 快照。
func (p *RedisPublisher) PublishClosed(ctx context.Context, symbol string, iv interval.Interval, b bar.OHLCV) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("kline publisher: not configured")
	}
	payload, err := json.Marshal(bar.ToJSON(symbol, iv, b, true))
	if err != nil {
		return fmt.Errorf("marshal kline: %w", err)
	}
	_ = p.rdb.Del(ctx, openKey(symbol, iv))
	return p.rdb.Publish(ctx, pubChannel(symbol, iv), string(payload))
}

// EnqueueClosed 将闭合 bar 写入待处理队列（崩溃后可重放）。
func (p *RedisPublisher) EnqueueClosed(ctx context.Context, symbol string, iv interval.Interval, b bar.OHLCV) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("kline publisher: not configured")
	}
	payload, err := json.Marshal(bar.ToJSON(symbol, iv, b, true))
	if err != nil {
		return fmt.Errorf("marshal closed bar: %w", err)
	}
	return p.rdb.LPush(ctx, pendingCloseKey, string(payload))
}

// PendingCloseCount 返回待处理闭合 bar 数量。
func (p *RedisPublisher) PendingCloseCount(ctx context.Context) (int64, error) {
	if p == nil || p.rdb == nil {
		return 0, fmt.Errorf("kline publisher: not configured")
	}
	return p.rdb.LLen(ctx, pendingCloseKey)
}

// PopPendingClosed 从队列尾部取一条待处理闭合 bar（FIFO：生产 LPUSH，消费 RPOP）。
func (p *RedisPublisher) PopPendingClosed(ctx context.Context) (bar.JSON, bool, error) {
	if p == nil || p.rdb == nil {
		return bar.JSON{}, false, fmt.Errorf("kline publisher: not configured")
	}
	raw, err := p.rdb.RPop(ctx, pendingCloseKey)
	if err != nil {
		return bar.JSON{}, false, err
	}
	if raw == "" {
		return bar.JSON{}, false, nil
	}
	var j bar.JSON
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return bar.JSON{}, false, fmt.Errorf("unmarshal pending closed: %w", err)
	}
	return j, true, nil
}

// LoadOpenBar 从 Redis 读取未闭合 bar。
func (p *RedisPublisher) LoadOpenBar(ctx context.Context, symbol string, iv interval.Interval) (bar.OHLCV, bool, error) {
	if p == nil || p.rdb == nil {
		return bar.OHLCV{}, false, fmt.Errorf("kline publisher: not configured")
	}
	raw, err := p.rdb.Get(ctx, openKey(symbol, iv))
	if err != nil {
		return bar.OHLCV{}, false, err
	}
	if raw == "" {
		return bar.OHLCV{}, false, nil
	}
	var j bar.JSON
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return bar.OHLCV{}, false, fmt.Errorf("unmarshal open bar: %w", err)
	}
	b, err := jsonToOHLCV(j)
	if err != nil {
		return bar.OHLCV{}, false, err
	}
	return b, true, nil
}

// ScanOpenBars 扫描所有 open bar 键并解析（用于重启恢复）。
func (p *RedisPublisher) ScanOpenBars(ctx context.Context, fn func(symbol string, iv interval.Interval, b bar.OHLCV) error) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("kline publisher: not configured")
	}
	return p.rdb.ScanKeys(ctx, openKeyPrefix+"*", func(key string) error {
		suffix := stringsTrimPrefix(key, openKeyPrefix)
		parts := splitSymbolInterval(suffix)
		if len(parts) != 2 {
			return nil
		}
		iv, err := interval.Parse(parts[1])
		if err != nil {
			return nil
		}
		raw, err := p.rdb.Get(ctx, key)
		if err != nil || raw == "" {
			return err
		}
		var j bar.JSON
		if err := json.Unmarshal([]byte(raw), &j); err != nil {
			return fmt.Errorf("unmarshal %s: %w", key, err)
		}
		if j.IsClosed {
			return nil
		}
		b, err := jsonToOHLCV(j)
		if err != nil {
			return err
		}
		return fn(parts[0], iv, b)
	})
}

func jsonToOHLCV(j bar.JSON) (bar.OHLCV, error) {
	open, err := bar.ParseDecimal(j.Open)
	if err != nil {
		return bar.OHLCV{}, err
	}
	high, err := bar.ParseDecimal(j.High)
	if err != nil {
		return bar.OHLCV{}, err
	}
	low, err := bar.ParseDecimal(j.Low)
	if err != nil {
		return bar.OHLCV{}, err
	}
	closeP, err := bar.ParseDecimal(j.Close)
	if err != nil {
		return bar.OHLCV{}, err
	}
	vol, err := bar.ParseDecimal(j.Volume)
	if err != nil {
		return bar.OHLCV{}, err
	}
	return bar.OHLCV{
		OpenTimeMs:  j.OpenTimeMs,
		Open:        open,
		High:        high,
		Low:         low,
		Close:       closeP,
		Volume:      vol,
		UpdatedAtMs: j.TimestampMs,
	}, nil
}

func stringsTrimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

// symbol 可能含 '-'，interval 在最后一个 ':' 之后；key 格式 kline:open:BTC-USDT:1m
func splitSymbolInterval(suffix string) []string {
	i := len(suffix) - 1
	for i >= 0 && suffix[i] != ':' {
		i--
	}
	if i <= 0 || i >= len(suffix)-1 {
		return nil
	}
	return []string{suffix[:i], suffix[i+1:]}
}
