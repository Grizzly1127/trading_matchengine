package engine

import (
	"context"
	"sync"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/collector"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/config"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/store"
	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
)

// Engine 定时采集、聚合与发布。
type Engine struct {
	cfg        config.Config
	aggCfg     aggregator.Config
	collectors []collector.Collector
	store      *store.Store
	redis      *publisher.RedisPublisher
	kafka      *publisher.KafkaPublisher
	repo       *repository.Repository
	metrics    *metrics.Counters
	log        zerolog.Logger
}

// New 创建 Engine。
func New(
	cfg config.Config,
	aggCfg aggregator.Config,
	collectors []collector.Collector,
	st *store.Store,
	redisPub *publisher.RedisPublisher,
	kafkaPub *publisher.KafkaPublisher,
	repo *repository.Repository,
	m *metrics.Counters,
	log zerolog.Logger,
) *Engine {
	return &Engine{
		cfg:        cfg,
		aggCfg:     aggCfg,
		collectors: collectors,
		store:      st,
		redis:      redisPub,
		kafka:      kafkaPub,
		repo:       repo,
		metrics:    m,
		log:        log,
	}
}

// Run 阻塞直到 ctx 取消。
func (e *Engine) Run(ctx context.Context) {
	if e == nil {
		return
	}
	interval := time.Duration(e.cfg.PollIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	e.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tick(ctx)
		}
	}
}

func (e *Engine) tick(ctx context.Context) {
	if e.metrics != nil {
		e.metrics.TicksTotal.Add(1)
	}
	for _, symbol := range e.cfg.Symbols {
		e.processSymbol(ctx, symbol)
	}
}

func (e *Engine) processSymbol(ctx context.Context, symbol string) {
	quotes := e.fetchQuotes(ctx, symbol)
	res, ok := aggregator.Aggregate(quotes, e.aggCfg)
	now := time.Now().UTC()

	if !ok {
		if e.metrics != nil {
			e.metrics.AggregateFailed.Add(1)
		}
		e.applyStale(symbol, now)
		return
	}

	if e.metrics != nil {
		e.metrics.AggregateOK.Add(1)
	}
	e.publish(ctx, symbol, res.Price, res.Sources, now, false)
}

func (e *Engine) applyStale(symbol string, now time.Time) {
	prev, has := e.store.Get(symbol)
	if !has {
		e.log.Warn().Str("symbol", symbol).Msg("index: no price and aggregate failed")
		return
	}
	staleAfter := time.Duration(e.cfg.StaleAfterMs) * time.Millisecond
	stale := now.Sub(prev.Updated) > staleAfter
	if stale {
		e.log.Warn().
			Str("symbol", symbol).
			Str("last_price", prev.Price.String()).
			Msg("index: price stale")
	}
	e.store.Update(symbol, prev.Price, prev.Sources, prev.Updated, stale)
}

func (e *Engine) publish(ctx context.Context, symbol string, price decimal.Decimal, sources []string, at time.Time, stale bool) {
	e.store.Update(symbol, price, sources, at, stale)

	if e.redis != nil {
		if err := e.redis.Publish(ctx, symbol, price, at, sources); err != nil {
			if e.metrics != nil {
				e.metrics.RedisPublishErrors.Add(1)
			}
			e.log.Warn().Err(err).Str("symbol", symbol).Msg("index: redis publish failed")
		}
	}

	if e.kafka != nil && e.cfg.Kafka.ProducerEnabled {
		if err := e.kafka.Publish(ctx, symbol, price, at.UnixMilli(), sources); err != nil {
			if e.metrics != nil {
				e.metrics.KafkaPublishErrors.Add(1)
			}
			e.log.Warn().Err(err).Str("symbol", symbol).Msg("index: kafka publish failed")
		}
	}

	if e.repo != nil {
		go func(sym, p string, ts time.Time, src []string) {
			auditCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := e.repo.InsertAudit(auditCtx, sym, p, ts, src); err != nil {
				if e.metrics != nil {
					e.metrics.AuditWriteErrors.Add(1)
				}
				e.log.Warn().Err(err).Str("symbol", sym).Msg("index: audit insert failed")
			}
		}(symbol, price.String(), at, append([]string(nil), sources...))
	}
}

func (e *Engine) fetchQuotes(ctx context.Context, symbol string) []aggregator.Quote {
	timeout := time.Duration(e.cfg.FetchTimeoutMs) * time.Millisecond
	var (
		mu     sync.Mutex
		quotes []aggregator.Quote
		wg     sync.WaitGroup
	)
	for _, c := range e.collectors {
		wg.Go(func() {
			fetchCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			price, err := c.FetchPrice(fetchCtx, symbol)
			if err != nil {
				if e.metrics != nil {
					e.metrics.SourceFetchErrors.Add(1)
				}
				e.log.Debug().Err(err).Str("source", c.Name()).Str("symbol", symbol).Msg("index: fetch failed")
				return
			}
			mu.Lock()
			quotes = append(quotes, aggregator.Quote{
				Source: c.Name(),
				Price:  price,
				Weight: c.Weight(),
			})
			mu.Unlock()
		})
	}
	wg.Wait()
	e.log.Debug().Str("symbol", symbol).Int("count", len(quotes)).Msg("index: fetched quotes")
	for _, q := range quotes {
		e.log.Debug().Str("source", q.Source).Str("price", q.Price.String()).Msg("index: quote")
	}
	return quotes
}
