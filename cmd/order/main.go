// Order Service 进程入口（第 4 步：PlaceOrder + Outbox + 事件消费骨架）。
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"

	"github.com/Grizzly1127/trading_matchengine/internal/order/config"
	"github.com/Grizzly1127/trading_matchengine/internal/order/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/order/handler"
	"github.com/Grizzly1127/trading_matchengine/internal/order/idempotency"
	"github.com/Grizzly1127/trading_matchengine/internal/order/marketdata"
	matchengine "github.com/Grizzly1127/trading_matchengine/internal/order/matching"
	"github.com/Grizzly1127/trading_matchengine/internal/order/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	orderpublisher "github.com/Grizzly1127/trading_matchengine/internal/order/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/order/reconciler"
	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/order/service"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/Grizzly1127/trading_matchengine/pkg/shardmgr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "configs/order.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service:    "order",
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
	ctx := context.Background()

	pool, err := repository.NewPool(ctx, cfg.DatabaseURL, cfg.Database.MaxConns)
	if err != nil {
		log.Fatal().Err(err).Msg("connect database")
	}
	defer pool.Close()

	relayPool, err := repository.NewPool(ctx, cfg.DatabaseURL, cfg.Database.RelayMaxConns)
	if err != nil {
		log.Fatal().Err(err).Msg("connect relay database pool")
	}
	defer relayPool.Close()

	if cfg.MigrateOnStart {
		if err := repository.MigrateUp(ctx, pool); err != nil {
			log.Fatal().Err(err).Msg("migrate database")
		}
		log.Info().Msg("database migration applied")
	}

	rulesCfg, err := cfg.LoadRules()
	if err != nil {
		log.Fatal().Err(err).Msg("symbol rules")
	}

	var shardMgr *shardmgr.Manager
	if strings.TrimSpace(cfg.ShardsFile) != "" {
		shardMgr, err = shardmgr.LoadFile(cfg.ShardsFile)
		if err != nil {
			log.Fatal().Err(err).Str("shards_file", cfg.ShardsFile).Msg("load shard manager")
		}
		log.Info().Str("shards_file", cfg.ShardsFile).Msg("shard manager loaded")
	}

	repo := repository.New(pool, rulesCfg.Assets)
	repo.SetRelayPool(relayPool)

	var matchingClient *matchengine.Client
	if cfg.Matching.Enabled && cfg.Matching.GRPCAddr != "" {
		mc, err := matchengine.Connect(context.Background(), cfg.Matching.GRPCAddr, time.Duration(cfg.Matching.DialTimeoutSeconds)*time.Second)
		if err != nil {
			log.Warn().Err(err).Str("grpc_addr", cfg.Matching.GRPCAddr).Msg("matching admin connect failed, reconciler without matching")
		} else {
			matchingClient = mc
			defer matchingClient.Close()
		}
	}

	writer := kafka.NewEventWriter(cfg.KafkaWriterConfig())
	defer writer.Close()
	mdClient, err := marketdata.Connect(ctx, cfg.MarketData.GRPCAddr, time.Duration(cfg.MarketData.DialTimeoutSeconds)*time.Second)
	if err != nil {
		log.Fatal().Err(err).Str("grpc_addr", cfg.MarketData.GRPCAddr).Msg("connect marketdata")
	}
	mdClient.Timeout = time.Duration(cfg.MarketData.RequestTimeoutSeconds) * time.Second
	defer mdClient.Close()

	grpcServer := grpc.NewServer()

	var orderRedis *redis.Client

	orderSvc := &service.OrderService{
		Repo:           repo,
		OutboxTopic:    cfg.Kafka.CommandTopic,
		MarketData:     mdClient,
		SlippageBuffer: decimal.NewFromFloat(cfg.MarketData.SlippageBuffer),
		Symbols:        rulesCfg.Registry,
		Shards:         shardMgr,
	}
	orderv1.RegisterOrderServiceServer(grpcServer, &handler.OrderServer{Svc: orderSvc})
	orderv1.RegisterOrderAdminServiceServer(grpcServer, &handler.AdminServer{Repo: repo})
	balanceSvc := &service.BalanceService{Repo: repo}
	orderv1.RegisterBalanceServiceServer(grpcServer, &handler.BalanceServer{Svc: balanceSvc})

	lis, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		log.Fatal().Err(err).Str("listen", cfg.GRPCListen).Msg("grpc listen")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	if cfg.MetricsListen != "" {
		go startOrderMetricsHTTP(ctx, log, cfg.MetricsListen)
		go startOrderMetricsCollector(ctx, log, repo, m)
	}

	relayCfg := cfg.OutboxRelayRuntime(cfg.Kafka.Partition, shardMgr)
	relay := &outbox.Relay{
		Store:   repo.RelayStore(),
		Writer:  writer,
		Log:     log.With().Str("component", "outbox_relay").Logger(),
		Config:  relayCfg,
		Metrics: m,
	}
	log.Info().
		Int("outbox_workers", relayCfg.Workers).
		Int("db_max_conns", cfg.Database.MaxConns).
		Int("db_relay_max_conns", cfg.Database.RelayMaxConns).
		Msg("outbox relay starting")
	go relay.Run(ctx)

	reconcileSched := &reconciler.Scheduler{
		Store:    repo,
		Matching: matchingClient,
		Log:      log.With().Str("component", "reconciler").Logger(),
		Config:   cfg.ReconcilerRuntime(cfg.Kafka.CommandTopic),
	}
	go reconcileSched.Run(ctx)

	var orderPush *orderpublisher.RedisPublisher
	if cfg.Redis.Addr != "" {
		rdb, err := redis.NewClient(redis.Config{
			Addr:     cfg.Redis.Addr,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("order redis client")
		}
		defer rdb.Close()
		if err := rdb.Ping(ctx); err != nil {
			log.Fatal().Err(err).Str("redis_addr", cfg.Redis.Addr).Msg("order redis ping")
		}
		orderRedis = rdb
		orderPush = orderpublisher.NewRedisPublisher(rdb)
		log.Info().Str("redis_addr", cfg.Redis.Addr).Msg("order ws publisher enabled")
	}
	if cfg.Idempotency.Enabled && orderRedis != nil {
		orderSvc.Idempotency = idempotency.NewRedisCache(orderRedis, time.Duration(cfg.Idempotency.TTLHours)*time.Hour)
		log.Info().Int("ttl_hours", cfg.Idempotency.TTLHours).Msg("order idempotency redis cache enabled")
	}

	if cfg.Kafka.ConsumerEnabled {
		startEventConsumers(ctx, log, cfg, repo, orderPush)
	}

	go func() {
		log.Info().
			Str("config", *configPath).
			Str("grpc_listen", cfg.GRPCListen).
			Str("command_topic", cfg.Kafka.CommandTopic).
			Str("match_topic", cfg.Kafka.MatchTopic).
			Str("trade_topic", cfg.Kafka.TradeTopic).
			Bool("consumer_enabled", cfg.Kafka.ConsumerEnabled).
			Int("partition", cfg.Kafka.Partition).
			Msg("order service ready")
		if err := grpcServer.Serve(lis); err != nil {
			log.Error().Err(err).Msg("grpc serve")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down")
	grpcServer.GracefulStop()
}

func runEventConsumerLoop(ctx context.Context, log zerolog.Logger, run func() error) {
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

func startEventConsumers(ctx context.Context, log zerolog.Logger, cfg config.Config, repo *repository.Repository, orderPush *orderpublisher.RedisPublisher) {
	base := consumer.TopicConsumerConfig{
		Brokers:     cfg.Kafka.Brokers,
		GroupID:     cfg.Kafka.GroupID,
		Partition:   cfg.Kafka.Partition,
		StartOffset: cfg.Kafka.ConsumerStartOffset,
	}

	matchLog := log.With().Str("component", "match_consumer").Logger()
	go runEventConsumerLoop(ctx, matchLog, func() error {
		matchCfg := base
		matchCfg.Topic = cfg.Kafka.MatchTopic
		return consumer.RunTopic(ctx, matchLog, matchCfg, &consumer.MatchHandler{Repo: repo, Publisher: orderPush})
	})

	tradeLog := log.With().Str("component", "trade_consumer").Logger()
	go runEventConsumerLoop(ctx, tradeLog, func() error {
		tradeCfg := base
		tradeCfg.Topic = cfg.Kafka.TradeTopic
		return consumer.RunTopic(ctx, tradeLog, tradeCfg, &consumer.TradeHandler{Repo: repo})
	})
}

func startOrderMetricsHTTP(ctx context.Context, log zerolog.Logger, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info().Str("listen", addr).Msg("order metrics listening")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error().Err(err).Msg("order metrics serve")
	}
}

func startOrderMetricsCollector(ctx context.Context, log zerolog.Logger, repo *repository.Repository, m *metrics.Metrics) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	refresh := func() {
		n, err := repo.CountUnpublishedOutbox(ctx)
		if err != nil {
			log.Debug().Err(err).Msg("order metrics: outbox count")
		} else {
			m.SetOutboxPendingCount(n)
		}
		sec, err := repo.MaxStuckPendingSeconds(ctx)
		if err != nil {
			log.Debug().Err(err).Msg("order metrics: stuck pending")
		} else {
			m.SetOrderStuckPendingSeconds(sec)
		}
	}
	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}
