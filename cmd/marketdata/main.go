package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/config"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/handler"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "configs/marketdata.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service:    "marketdata",
		Level:      cfg.Log.Level,
		Dev:        cfg.Log.Dev,
		File:       cfg.Log.File,
		Async:      cfg.Log.Async,
		BufferSize: cfg.Log.BufferSize,
		Rotate: logger.RotateConfig{
			MaxSizeMB:   cfg.Log.MaxSizeMB,
			MaxAgeDays:  cfg.Log.MaxAgeDays,
			MaxBackups:  cfg.Log.MaxBackups,
			Compress:    cfg.Log.Compress,
			LocalTime:   cfg.Log.LocalTime,
			RotateDaily: cfg.Log.RotateDaily,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer logRes.Close()

	log := logRes.Logger

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb, err := redis.NewClient(redis.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("init redis client failed")
	}
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx); err != nil {
		log.Fatal().Err(err).Msg("redis ping failed")
	}

	st := store.New()
	pub := publisher.NewRedisPublisher(rdb)
	m := &metrics.Counters{}
	metrics.RegisterPrometheus(m)
	errCh := make(chan error, 1)

	grpcServer := grpc.NewServer()
	marketdatav1.RegisterMarketDataServiceServer(grpcServer, handler.NewServer(st))
	go startGRPC(ctx, log, cfg.GRPCListen, grpcServer, errCh)
	go startMetricsHTTP(ctx, log, cfg.MetricsListen, errCh)

	if cfg.Kafka.ConsumerEnabled {
		startEventConsumers(ctx, log, cfg, st, pub, m)
	}
	startDepthPublisher(ctx, log.With().Str("component", "depth_publisher").Logger(), cfg, st, pub, m)
	startTickerAllPublisher(ctx, log.With().Str("component", "ticker_all_publisher").Logger(), cfg, st, pub, m)
	startMetricsLogger(ctx, log.With().Str("component", "metrics").Logger(), m)

	select {
	case <-ctx.Done():
		log.Info().Msg("marketdata shutting down")
	case err := <-errCh:
		log.Error().Err(err).Msg("marketdata stopped")
		stop()
	}
}

func startGRPC(ctx context.Context, log zerolog.Logger, addr string, srv *grpc.Server, errCh chan<- error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		errCh <- fmt.Errorf("listen %s: %w", addr, err)
		return
	}
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	log.Info().Str("listen", addr).Msg("marketdata grpc listening")
	if err := srv.Serve(lis); err != nil {
		errCh <- fmt.Errorf("grpc serve: %w", err)
	}
}

func startEventConsumers(ctx context.Context, log zerolog.Logger, cfg config.Config, st *store.Store, pub *publisher.RedisPublisher, m *metrics.Counters) {
	base := consumer.TopicConsumerConfig{
		Brokers:     cfg.Kafka.Brokers,
		GroupID:     cfg.Kafka.GroupID,
		Partition:   cfg.Kafka.Partition,
		StartOffset: cfg.Kafka.ConsumerStartOffset,
	}

	matchLog := log.With().Str("component", "match_consumer").Logger()
	go runMarketdataConsumerLoop(ctx, matchLog, func() error {
		matchCfg := base
		matchCfg.Topic = cfg.Kafka.MatchTopic
		return consumer.RunTopic(ctx, matchLog, matchCfg, &consumer.MatchHandler{
			Store:   st,
			Metrics: m,
		})
	})

	tradeLog := log.With().Str("component", "trade_consumer").Logger()
	go runMarketdataConsumerLoop(ctx, tradeLog, func() error {
		tradeCfg := base
		tradeCfg.Topic = cfg.Kafka.TradeTopic
		return consumer.RunTopic(ctx, tradeLog, tradeCfg, &consumer.TradeHandler{
			Store:     st,
			Publisher: pub,
			Metrics:   m,
		})
	})
}

func runMarketdataConsumerLoop(ctx context.Context, log zerolog.Logger, run func() error) {
	for {
		if err := run(); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("kafka consumer stopped, retry in 1s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		return
	}
}

func startDepthPublisher(ctx context.Context, log zerolog.Logger, cfg config.Config, st *store.Store, pub *publisher.RedisPublisher, m *metrics.Counters) {
	interval := time.Duration(cfg.Publish.DepthIntervalMs) * time.Millisecond
	limit := cfg.Publish.DepthLimit
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		lastBySymbol := make(map[string]store.OrderBookSnapshot)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, symbol := range st.Symbols() {
					snap, ok := st.SnapshotOrderBook(symbol, limit)
					if !ok || snap.LastUpdateID == 0 {
						continue
					}
					prev, hasPrev := lastBySymbol[symbol]
					if !hasPrev || prev.LastUpdateID == 0 {
						if err := pub.PublishDepth(ctx, snap); err != nil {
							m.RedisPublishErrors.Add(1)
							log.Error().Err(err).Str("symbol", symbol).Msg("publish depth snapshot failed")
							continue
						}
						lastBySymbol[symbol] = snap
						m.DepthPublished.Add(1)
						continue
					}

					bidsDelta, asksDelta := computeDepthDelta(prev, snap)
					if len(bidsDelta) == 0 && len(asksDelta) == 0 {
						lastBySymbol[symbol] = snap
						continue
					}
					if err := pub.PublishDepthDelta(ctx, symbol, snap.LastUpdateID, bidsDelta, asksDelta, snap.UpdatedAtMs); err != nil {
						m.RedisPublishErrors.Add(1)
						log.Error().Err(err).Str("symbol", symbol).Msg("publish depth delta failed")
					} else {
						lastBySymbol[symbol] = snap
						m.DepthPublished.Add(1)
					}
				}
			}
		}
	}()
}

func startTickerAllPublisher(ctx context.Context, log zerolog.Logger, cfg config.Config, st *store.Store, pub *publisher.RedisPublisher, m *metrics.Counters) {
	interval := time.Duration(cfg.Publish.TickerAllIntervalMs) * time.Millisecond
	quoteAssets := append([]string(nil), cfg.Publish.TickerAllQuoteAssets...)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, quote := range quoteAssets {
					snap := st.BuildTickerAllSnapshot(quote)
					if err := pub.PublishTickerAll(ctx, snap); err != nil {
						m.RedisPublishErrors.Add(1)
						log.Error().Err(err).Str("quote_asset", quote).Msg("publish ticker all failed")
					} else {
						m.TickerAllPublished.Add(1)
					}
				}
			}
		}
	}()
}

func startMetricsLogger(ctx context.Context, log zerolog.Logger, m *metrics.Counters) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := m.Snapshot()
				log.Info().
					Uint64("trade_events", s.TradeEvents).
					Uint64("match_events", s.MatchEvents).
					Uint64("depth_published", s.DepthPublished).
					Uint64("ticker_all_published", s.TickerAllPublished).
					Uint64("redis_publish_errors", s.RedisPublishErrors).
					Msg("marketdata metrics")
			}
		}
	}()
}

func startMetricsHTTP(ctx context.Context, log zerolog.Logger, addr string, errCh chan<- error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Info().Str("listen", addr).Msg("marketdata metrics listening")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errCh <- fmt.Errorf("metrics serve: %w", err)
	}
}

func computeDepthDelta(prev, curr store.OrderBookSnapshot) (bids, asks []store.PriceLevel) {
	return diffOneSide(prev.Bids, curr.Bids), diffOneSide(prev.Asks, curr.Asks)
}

func diffOneSide(prev, curr []store.PriceLevel) []store.PriceLevel {
	prevMap := make(map[string]string, len(prev))
	currMap := make(map[string]string, len(curr))
	for _, lv := range prev {
		prevMap[lv.Price] = lv.Quantity
	}
	for _, lv := range curr {
		currMap[lv.Price] = lv.Quantity
	}
	changed := make([]store.PriceLevel, 0)
	for price, qty := range currMap {
		if prevQty, ok := prevMap[price]; !ok || prevQty != qty {
			changed = append(changed, store.PriceLevel{Price: price, Quantity: qty})
		}
	}
	for price := range prevMap {
		if _, ok := currMap[price]; !ok {
			changed = append(changed, store.PriceLevel{Price: price, Quantity: "0"})
		}
	}
	return changed
}
