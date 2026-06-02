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

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/config"
	klineconsumer "github.com/Grizzly1127/trading_matchengine/internal/kline/consumer"
	klmetrics "github.com/Grizzly1127/trading_matchengine/internal/kline/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/recovery"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/service"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/worker"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/consumer"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "configs/kline.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service:    "kline",
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

	pool, err := repository.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("connect postgres failed")
	}
	defer pool.Close()
	if err := repository.MigrateUp(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("migrate kline schema failed")
	}
	repo := repository.New(pool)

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

	pub := publisher.NewRedisPublisher(rdb)
	m := &klmetrics.Counters{}
	klmetrics.RegisterPrometheus(m)

	var kafkaPub *publisher.KafkaPublisher
	var kafkaWriter *kafka.EventWriter
	if cfg.Kafka.ProducerEnabled {
		kafkaWriter = kafka.NewEventWriter(kafka.WriterConfig{Brokers: cfg.Kafka.Brokers})
		defer func() { _ = kafkaWriter.Close() }()
		kafkaPub = publisher.NewKafkaPublisher(kafkaWriter, cfg.Kafka.KlineRawTopic)
	}

	agg := aggregator.NewAggregator()
	closeWorker := worker.New(repo, pub, log.With().Str("component", "close_worker").Logger(), cfg.ClosedQueueSize)
	closeWorker.Kafka = kafkaPub
	closeWorker.Metrics = m

	agg.SetOnClose(func(ev aggregator.ClosedEvent) {
		closeWorker.HandleClose(ctx, ev)
	})

	if err := recovery.Restore(ctx, log, agg, pub, closeWorker); err != nil {
		log.Fatal().Err(err).Msg("kline recovery failed")
	}

	go closeWorker.Run(ctx)

	errCh := make(chan error, 1)
	grpcServer := grpc.NewServer()
	klinev1.RegisterKlineServiceServer(grpcServer, service.NewServer(repo, agg))
	go startGRPC(ctx, log, cfg.GRPCListen, grpcServer, errCh)
	go startMetricsHTTP(ctx, log, cfg.MetricsListen, errCh)
	startMetricsLogger(ctx, log.With().Str("component", "metrics").Logger(), m)

	if cfg.Kafka.ConsumerEnabled {
		startTradeConsumer(ctx, log, cfg, agg, pub, m)
	}

	select {
	case <-ctx.Done():
		log.Info().Msg("kline shutting down")
		closeWorker.Drain(ctx)
		closeWorker.WaitStopped()
	case err := <-errCh:
		log.Error().Err(err).Msg("kline stopped")
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
	log.Info().Str("listen", addr).Msg("kline grpc listening")
	if err := srv.Serve(lis); err != nil {
		errCh <- fmt.Errorf("grpc serve: %w", err)
	}
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
	log.Info().Str("listen", addr).Msg("kline metrics listening")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errCh <- fmt.Errorf("metrics serve: %w", err)
	}
}

func startMetricsLogger(ctx context.Context, log zerolog.Logger, m *klmetrics.Counters) {
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
					Uint64("open_bar_updates", s.OpenBarUpdates).
					Uint64("closed_bars", s.ClosedBarsPersisted).
					Uint64("kline_raw_published", s.KlineRawPublished).
					Uint64("redis_publish_errors", s.RedisPublishErrors).
					Uint64("kafka_publish_errors", s.KafkaPublishErrors).
					Msg("kline metrics")
			}
		}
	}()
}

func startTradeConsumer(ctx context.Context, log zerolog.Logger, cfg config.Config, agg *aggregator.Aggregator, pub *publisher.RedisPublisher, m *klmetrics.Counters) {
	base := consumer.TopicConsumerConfig{
		Brokers:     cfg.Kafka.Brokers,
		GroupID:     cfg.Kafka.GroupID,
		Partition:   cfg.Kafka.Partition,
		StartOffset: cfg.Kafka.ConsumerStartOffset,
	}
	tradeLog := log.With().Str("component", "trade_consumer").Logger()
	go func() {
		tradeCfg := base
		tradeCfg.Topic = cfg.Kafka.TradeTopic
		if err := consumer.RunTopic(ctx, tradeLog, tradeCfg, &klineconsumer.TradeHandler{
			Aggregator: agg,
			Publisher:  pub,
			Metrics:    m,
		}); err != nil && ctx.Err() == nil {
			tradeLog.Error().Err(err).Msg("kline trade consumer stopped")
		}
	}()
}
