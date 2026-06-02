package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/collector"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/config"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/service"
	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "configs/indexprice.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service:    "indexprice",
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
		log.Fatal().Err(err).Msg("migrate indexprice schema failed")
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

	collectors, err := collector.Build(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("build collectors failed")
	}
	aggCfg, err := cfg.AggregatorParams()
	if err != nil {
		log.Fatal().Err(err).Msg("aggregator config invalid")
	}

	st := store.New()
	redisTTL := time.Duration(cfg.RedisKeyTTLMs) * time.Millisecond
	redisPub := publisher.NewRedisPublisher(rdb, redisTTL)

	var kafkaPub *publisher.KafkaPublisher
	var kafkaWriter *kafka.EventWriter
	if cfg.Kafka.ProducerEnabled {
		kafkaWriter = kafka.NewEventWriter(kafka.WriterConfig{Brokers: cfg.Kafka.Brokers})
		defer func() { _ = kafkaWriter.Close() }()
		kafkaPub = publisher.NewKafkaPublisher(kafkaWriter, cfg.Kafka.IndexTopic)
	}

	m := &metrics.Counters{}
	eng := engine.New(cfg, aggCfg, collectors, st, redisPub, kafkaPub, repo, m, log.With().Str("component", "engine").Logger())

	errCh := make(chan error, 1)
	grpcServer := grpc.NewServer()
	indexv1.RegisterIndexPriceServiceServer(grpcServer, service.NewServer(st))
	go startGRPC(ctx, log, cfg.GRPCListen, grpcServer, errCh)

	go eng.Run(ctx)

	select {
	case <-ctx.Done():
		log.Info().Msg("indexprice shutting down")
		grpcServer.GracefulStop()
	case err := <-errCh:
		log.Error().Err(err).Msg("indexprice stopped")
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
	log.Info().Str("listen", addr).Msg("indexprice grpc listening")
	if err := srv.Serve(lis); err != nil {
		errCh <- fmt.Errorf("grpc serve: %w", err)
	}
}
