package tickerall

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	TypeSnapshot  = "snapshot"
	TypeDelta     = "delta"
	TypeHeartbeat = "heartbeat"
)

// CompactItem 全市场 Ticker WS 短字段（rest-api §8.2）。
type CompactItem struct {
	S string `json:"s,omitempty"`
	P string `json:"p,omitempty"`
	V string `json:"v,omitempty"`
	Q string `json:"q,omitempty"`
	C string `json:"c,omitempty"`
}

type snapshotData struct {
	Count int           `json:"count"`
	Items []CompactItem `json:"items"`
}

type deltaData struct {
	Items []CompactItem `json:"items"`
}

// Frame 做市商 ticker@all WebSocket 推送帧。
type Frame struct {
	Stream     string `json:"stream"`
	Type       string `json:"type"`
	SnapshotID string `json:"snapshot_id"`
	Ts         int64  `json:"ts"`
	Data       any    `json:"data,omitempty"`
}

// WSStream 由计价币得到 WS stream 名（空或 ALL → ticker@all）。
func WSStream(quoteAsset string) string {
	q := NormalizeQuoteAsset(quoteAsset)
	if q == "" || q == "ALL" {
		return "ticker@all"
	}
	return "ticker@all:" + q
}

// PubSubChannel 与 WS stream 同名，供 Redis PUBLISH / Push PSubscribe。
func PubSubChannel(quoteAsset string) string {
	return WSStream(quoteAsset)
}

// RedisKey REST 快照 Key（ticker:all:{quote}，ALL 表示全市场）。
func RedisKey(quoteAsset string) string {
	q := NormalizeQuoteAsset(quoteAsset)
	if q == "" {
		return "ticker:all:ALL"
	}
	return "ticker:all:" + q
}

// NormalizeQuoteAsset 统一内部 quote 表示。
func NormalizeQuoteAsset(quoteAsset string) string {
	q := strings.TrimSpace(quoteAsset)
	if q == "" || strings.EqualFold(q, "ALL") {
		return "ALL"
	}
	return q
}

// CompactItemFromFields 由格式化字符串构造短字段条目。
func CompactItemFromFields(symbol, lastPrice, volume, quoteVolume, changePercent string) CompactItem {
	return CompactItem{
		S: symbol,
		P: lastPrice,
		V: volume,
		Q: quoteVolume,
		C: changePercent,
	}
}

// CompactMapFromItems 将条目列表转为 symbol → 条目 映射。
func CompactMapFromItems(items []CompactItem) map[string]CompactItem {
	out := make(map[string]CompactItem, len(items))
	for _, it := range items {
		if it.S == "" {
			continue
		}
		out[it.S] = it
	}
	return out
}

// ItemsFromCompactMap 将映射还原为稳定排序的切片。
func ItemsFromCompactMap(m map[string]CompactItem) []CompactItem {
	if len(m) == 0 {
		return nil
	}
	syms := make([]string, 0, len(m))
	for s := range m {
		syms = append(syms, s)
	}
	sortStrings(syms)
	out := make([]CompactItem, 0, len(syms))
	for _, s := range syms {
		out = append(out, m[s])
	}
	return out
}

// Diff 相对上一帧，返回发生变化的 symbol 条目（仅含变化字段）。
func Diff(prev, curr map[string]CompactItem) []CompactItem {
	if len(curr) == 0 {
		return nil
	}
	syms := make([]string, 0, len(curr))
	for s := range curr {
		syms = append(syms, s)
	}
	sortStrings(syms)

	var out []CompactItem
	for _, sym := range syms {
		c := curr[sym]
		p, ok := prev[sym]
		if !ok {
			out = append(out, c)
			continue
		}
		d := CompactItem{S: sym}
		changed := false
		if c.P != p.P {
			d.P = c.P
			changed = true
		}
		if c.V != p.V {
			d.V = c.V
			changed = true
		}
		if c.Q != p.Q {
			d.Q = c.Q
			changed = true
		}
		if c.C != p.C {
			d.C = c.C
			changed = true
		}
		if changed {
			out = append(out, d)
		}
	}
	return out
}

func MarshalSnapshot(stream, snapshotID string, ts int64, count int, items []CompactItem) ([]byte, error) {
	f := Frame{
		Stream:     stream,
		Type:       TypeSnapshot,
		SnapshotID: snapshotID,
		Ts:         ts,
		Data: snapshotData{
			Count: count,
			Items: items,
		},
	}
	return json.Marshal(f)
}

func MarshalDelta(stream, snapshotID string, ts int64, items []CompactItem) ([]byte, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("tickerall: empty delta")
	}
	f := Frame{
		Stream:     stream,
		Type:       TypeDelta,
		SnapshotID: snapshotID,
		Ts:         ts,
		Data:       deltaData{Items: items},
	}
	return json.Marshal(f)
}

func MarshalHeartbeat(stream, snapshotID string, ts int64) ([]byte, error) {
	f := Frame{
		Stream:     stream,
		Type:       TypeHeartbeat,
		SnapshotID: snapshotID,
		Ts:         ts,
	}
	return json.Marshal(f)
}

// WSSnapshotFromRedisREST 将 Redis REST 快照 JSON 转为 WS snapshot 帧。
func WSSnapshotFromRedisREST(wsStream string, restJSON []byte) ([]byte, error) {
	var rest struct {
		SnapshotID   string `json:"snapshot_id"`
		SnapshotTime int64  `json:"snapshot_time"`
		Count        int    `json:"count"`
		Items        []struct {
			Symbol             string `json:"symbol"`
			LastPrice          string `json:"last_price"`
			Volume             string `json:"volume"`
			QuoteVolume        string `json:"quote_volume"`
			PriceChangePercent string `json:"price_change_percent"`
		} `json:"items"`
	}
	if err := json.Unmarshal(restJSON, &rest); err != nil {
		return nil, err
	}
	items := make([]CompactItem, 0, len(rest.Items))
	for _, it := range rest.Items {
		items = append(items, CompactItemFromFields(
			it.Symbol, it.LastPrice, it.Volume, it.QuoteVolume, it.PriceChangePercent,
		))
	}
	ts := rest.SnapshotTime
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	count := rest.Count
	if count == 0 {
		count = len(items)
	}
	return MarshalSnapshot(wsStream, rest.SnapshotID, ts, count, items)
}

func sortStrings(ss []string) { sort.Strings(ss) }
