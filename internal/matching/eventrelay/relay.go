package eventrelay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/eventoutbox"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/rs/zerolog"
)

// Metrics Relay 可观测性。
type Metrics interface {
	SetEventOutboxPending(n uint64)
	ObserveRelayBatchSize(n int)
	ObserveRelayDispatchLatency(d time.Duration)
	AddRelayPublished(n int)
	ObservePublishError()
}

// Config 控制 relay 行为。
type Config struct {
	PollInterval time.Duration
	BatchSize    int
	Workers      int
	MatchTopic   string
	TradeTopic   string
}

// Relay 从本地 Event Outbox 投递至 Kafka。
type Relay struct {
	Dir       string
	Writer    kafka.Producer
	Log       zerolog.Logger
	Config    Config
	Metrics   Metrics
	published uint64
	mu        sync.Mutex
}

// Run 阻塞直至 ctx 取消。
func (r *Relay) Run(ctx context.Context) {
	if r == nil || r.Writer == nil || r.Dir == "" {
		return
	}
	cfg := r.normalizedConfig()
	if cfg.Workers <= 1 {
		r.runWorker(ctx, cfg)
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runWorker(ctx, cfg)
		}()
	}
	wg.Wait()
}

func (r *Relay) runWorker(ctx context.Context, cfg Config) {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		n := r.pollOnce(ctx, cfg)
		r.updatePendingMetric()
		if n >= cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) loadPublished() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published > 0 {
		return r.published
	}
	meta, err := eventoutbox.LoadMeta(r.Dir)
	if err != nil {
		return 0
	}
	r.published = meta.LastPublishedSeq
	return r.published
}

func (r *Relay) storePublished(seq uint64) error {
	r.mu.Lock()
	r.published = seq
	r.mu.Unlock()
	return eventoutbox.SaveMetaLocked(r.Dir, eventoutbox.Meta{LastPublishedSeq: seq})
}

func (r *Relay) pollOnce(ctx context.Context, cfg Config) int {
	start := time.Now()
	after := r.loadPublished()
	recs, err := eventoutbox.FetchUnpublished(r.Dir, after, cfg.BatchSize)
	if err != nil {
		r.Log.Error().Err(err).Msg("event relay: fetch unpublished")
		return 0
	}
	if len(recs) == 0 {
		return 0
	}

	published := 0
	lastOK := after
	i := 0
	for i < len(recs) {
		rec := recs[i]
		if rec.OutboxSeq <= after {
			i++
			continue
		}
		topic, err := r.topicFor(rec.TopicID)
		if err != nil {
			r.Log.Warn().Err(err).Uint64("outbox_seq", rec.OutboxSeq).Msg("event relay: skip bad topic")
			i++
			continue
		}
		j := i + 1
		for j < len(recs) && recs[j].OutboxSeq > after &&
			recs[j].TopicID == rec.TopicID && recs[j].PartitionKey == rec.PartitionKey {
			j++
		}
		vals := make([][]byte, 0, j-i)
		for k := i; k < j; k++ {
			vals = append(vals, recs[k].Payload)
		}
		if err := r.Writer.WriteBatch(ctx, topic, []byte(rec.PartitionKey), vals); err != nil {
			r.Log.Warn().Err(err).Str("topic", topic).Msg("event relay: write batch failed")
			if r.Metrics != nil {
				r.Metrics.ObservePublishError()
			}
			break
		}
		published += len(vals)
		lastOK = recs[j-1].OutboxSeq
		i = j
	}

	if lastOK > after {
		if err := r.storePublished(lastOK); err != nil {
			r.Log.Error().Err(err).Uint64("seq", lastOK).Msg("event relay: save meta")
			return len(recs)
		}
	}

	if r.Metrics != nil && len(recs) > 0 {
		r.Metrics.ObserveRelayBatchSize(len(recs))
		r.Metrics.ObserveRelayDispatchLatency(time.Since(start))
		if published > 0 {
			r.Metrics.AddRelayPublished(published)
		}
	}
	return len(recs)
}

func (r *Relay) updatePendingMetric() {
	if r.Metrics == nil {
		return
	}
	meta, _ := eventoutbox.LoadMeta(r.Dir)
	recs, err := eventoutbox.FetchUnpublished(r.Dir, meta.LastPublishedSeq, 1)
	if err != nil || len(recs) == 0 {
		r.Metrics.SetEventOutboxPending(0)
		return
	}
	last := recs[len(recs)-1].OutboxSeq
	if last > meta.LastPublishedSeq {
		r.Metrics.SetEventOutboxPending(last - meta.LastPublishedSeq)
	}
}

func (r *Relay) topicFor(id byte) (string, error) {
	switch id {
	case eventoutbox.TopicMatch:
		if r.Config.MatchTopic == "" {
			return "match.events", nil
		}
		return r.Config.MatchTopic, nil
	case eventoutbox.TopicTrade:
		if r.Config.TradeTopic == "" {
			return "trade.events", nil
		}
		return r.Config.TradeTopic, nil
	default:
		return "", fmt.Errorf("unknown topic_id %d", id)
	}
}

func (r *Relay) normalizedConfig() Config {
	cfg := r.Config
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	return cfg
}

// PublishedSeq 返回当前已发布 outbox_seq。
func (r *Relay) PublishedSeq() uint64 {
	return r.loadPublished()
}
