package interval

import (
	"fmt"
	"strings"
	"time"
)

// Interval K 线周期字符串（与 REST `interval` 参数一致）。
type Interval string

const (
	Sec1  Interval = "1s"
	Min1  Interval = "1m"
	Min15 Interval = "15m"
	Hour1 Interval = "1h"
	Hour4 Interval = "4h"
	Hour6 Interval = "6h"
	Day1  Interval = "1d"
)

// DefaultIntervals 服务默认聚合周期。
var DefaultIntervals = []Interval{Sec1, Min1, Min15, Hour1, Hour4, Hour6, Day1}

// Parse 解析周期字符串。
func Parse(s string) (Interval, error) {
	iv := Interval(strings.TrimSpace(s))
	switch iv {
	case Sec1, Min1, Min15, Hour1, Hour4, Hour6, Day1:
		return iv, nil
	default:
		return "", fmt.Errorf("unsupported interval %q", s)
	}
}

// Duration 返回周期时长。
func (i Interval) Duration() time.Duration {
	switch i {
	case Sec1:
		return time.Second
	case Min1:
		return time.Minute
	case Min15:
		return 15 * time.Minute
	case Hour1:
		return time.Hour
	case Hour4:
		return 4 * time.Hour
	case Hour6:
		return 6 * time.Hour
	case Day1:
		return 24 * time.Hour
	default:
		return 0
	}
}

// DurationMs 返回周期毫秒数。
func (i Interval) DurationMs() int64 {
	return i.Duration().Milliseconds()
}

// BucketStartMs 按成交时间对齐到桶起始毫秒。
func (i Interval) BucketStartMs(tradeTimeMs int64) int64 {
	d := i.DurationMs()
	if d <= 0 {
		return tradeTimeMs
	}
	return tradeTimeMs - tradeTimeMs%d
}

// CloseTimeMs 返回该桶的收盘时间（开区间右端，毫秒）。
func (i Interval) CloseTimeMs(openTimeMs int64) int64 {
	return openTimeMs + i.DurationMs() - 1
}
