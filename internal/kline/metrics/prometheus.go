package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPrometheus 注册 Kline 指标到默认 Registerer。
func RegisterPrometheus(c *Counters) {
	registerGaugeFunc("kline_trade_events_total", "Consumed trade.events messages", func() float64 {
		return float64(c.TradeEvents.Load())
	})
	registerGaugeFunc("kline_open_bar_updates_total", "Redis open bar save/publish cycles", func() float64 {
		return float64(c.OpenBarUpdates.Load())
	})
	registerGaugeFunc("kline_closed_bars_persisted_total", "Closed bars written to PostgreSQL", func() float64 {
		return float64(c.ClosedBarsPersisted.Load())
	})
	registerGaugeFunc("kline_raw_published_total", "kline.raw Kafka messages published", func() float64 {
		return float64(c.KlineRawPublished.Load())
	})
	registerGaugeFunc("kline_redis_publish_errors_total", "Redis publish/save errors", func() float64 {
		return float64(c.RedisPublishErrors.Load())
	})
	registerGaugeFunc("kline_kafka_publish_errors_total", "kline.raw Kafka publish errors", func() float64 {
		return float64(c.KafkaPublishErrors.Load())
	})
	registerGaugeFunc("kline_close_worker_queue_full_total", "Closed bar worker queue full events", func() float64 {
		return float64(c.CloseWorkerQueueFull.Load())
	})
	registerGaugeFunc("kline_closed_persist_failures_total", "Closed bar persist failures after retries", func() float64 {
		return float64(c.ClosedPersistFailures.Load())
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
		return "kline_metric_" + strconv.Itoa(len(name))
	}
	return string(out)
}
