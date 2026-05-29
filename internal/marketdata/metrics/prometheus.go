package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

func RegisterPrometheus(c *Counters) {
	registerGaugeFunc("marketdata_trade_events_total", "Consumed trade events", func() float64 {
		return float64(c.TradeEvents.Load())
	})
	registerGaugeFunc("marketdata_match_events_total", "Consumed match events", func() float64 {
		return float64(c.MatchEvents.Load())
	})
	registerGaugeFunc("marketdata_depth_published_total", "Published depth messages", func() float64 {
		return float64(c.DepthPublished.Load())
	})
	registerGaugeFunc("marketdata_ticker_all_published_total", "Published ticker@all messages", func() float64 {
		return float64(c.TickerAllPublished.Load())
	})
	registerGaugeFunc("marketdata_redis_publish_errors_total", "Redis publish errors", func() float64 {
		return float64(c.RedisPublishErrors.Load())
	})
}

func registerGaugeFunc(name, help string, fn func() float64) {
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: sanitize(name), Help: help}, fn)
	if err := prometheus.DefaultRegisterer.Register(g); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return
		}
	}
}

func sanitize(name string) string {
	// Prometheus metric names必须满足 [a-zA-Z_:][a-zA-Z0-9_:]*
	// 这里做最小兜底，避免非法字符导致注册失败。
	out := make([]rune, 0, len(name))
	for i, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || r == ':'
		if i > 0 {
			valid = valid || (r >= '0' && r <= '9')
		}
		if valid {
			out = append(out, r)
		} else {
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "marketdata_metric_" + strconv.Itoa(len(name))
	}
	return string(out)
}
