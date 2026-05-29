package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/config"
	klineconsumer "github.com/Grizzly1127/trading_matchengine/internal/kline/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/recovery"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/service"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/worker"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/consumer"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
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
	agg := aggregator.NewAggregator()
	closeWorker := worker.New(repo, pub, log.With().Str("component", "close_worker").Logger(), cfg.ClosedQueueSize)

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

	if cfg.Kafka.ConsumerEnabled {
		startTradeConsumer(ctx, log, cfg, agg, pub)
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

func startTradeConsumer(ctx context.Context, log zerolog.Logger, cfg config.Config, agg *aggregator.Aggregator, pub *publisher.RedisPublisher) {
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
		}); err != nil && ctx.Err() == nil {
			tradeLog.Error().Err(err).Msg("kline trade consumer stopped")
		}
	}()
}
