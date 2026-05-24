// Order Service 进程入口（第 4 步 4.2：PlaceOrder + Transactional Outbox + Relay）。
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/Grizzly1127/trading_matchengine/internal/order/config"
	"github.com/Grizzly1127/trading_matchengine/internal/order/handler"
	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/order/service"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
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

	pool, err := repository.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("connect database")
	}
	defer pool.Close()

	if cfg.MigrateOnStart {
		if err := repository.MigrateUp(ctx, pool); err != nil {
			log.Fatal().Err(err).Msg("migrate database")
		}
		log.Info().Msg("database migration applied")
	}

	repo := repository.New(pool)
	writer := kafka.NewEventWriter(kafka.WriterConfig{Brokers: cfg.Kafka.Brokers})
	defer writer.Close()

	svc := &service.Service{
		Repo:        repo,
		OutboxTopic: cfg.Kafka.CommandTopic,
	}

	grpcServer := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(grpcServer, &handler.Server{Svc: svc})

	lis, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		log.Fatal().Err(err).Str("listen", cfg.GRPCListen).Msg("grpc listen")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	relay := &outbox.Relay{
		Store:  repo,
		Writer: writer,
		Log:    log.With().Str("component", "outbox_relay").Logger(),
		Config: outbox.RelayConfig{Partition: cfg.Kafka.Partition},
	}
	go relay.Run(ctx)

	go func() {
		log.Info().
			Str("config", *configPath).
			Str("grpc_listen", cfg.GRPCListen).
			Str("command_topic", cfg.Kafka.CommandTopic).
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
