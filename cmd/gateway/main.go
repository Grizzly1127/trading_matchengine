// API Gateway 进程入口（第 5 步：REST → Order gRPC）。
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/client"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/server"
	pushhub "github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	pushserver "github.com/Grizzly1127/trading_matchengine/internal/push/server"
	pushsubscriber "github.com/Grizzly1127/trading_matchengine/internal/push/subscriber"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

func main() {
	configPath := flag.String("config", "configs/gateway.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service:    "gateway",
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
	initCtx := context.Background()

	grpcClients, err := client.Connect(initCtx, cfg.OrderGRPCAddr, time.Duration(cfg.OrderGRPCDialSec)*time.Second)
	if err != nil {
		log.Fatal().Err(err).Str("order_grpc_addr", cfg.OrderGRPCAddr).Msg("connect order service")
	}
	defer grpcClients.Close()
	mdClients, err := client.ConnectMarketData(initCtx, cfg.MarketDataGRPCAddr, time.Duration(cfg.MarketDataGRPCDialSec)*time.Second)
	if err != nil {
		log.Fatal().Err(err).Str("marketdata_grpc_addr", cfg.MarketDataGRPCAddr).Msg("connect marketdata service")
	}
	defer mdClients.Close()
	wsHub := pushhub.New()
	rdb, err := redis.NewClient(redis.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("init redis client")
	}
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	wsServer := &pushserver.WSServer{
		Hub:   wsHub,
		Redis: rdb,
		Token: cfg.Auth.StaticToken,
		Log:   log.With().Str("component", "gateway_ws").Logger(),
	}
	wsSub := &pushsubscriber.RedisFanout{
		Redis: rdb,
		Hub:   wsHub,
		Log:   log.With().Str("component", "gateway_ws_subscriber").Logger(),
	}
	go func() {
		if err := wsSub.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("gateway ws subscriber stopped")
			stop()
		}
	}()

	router := server.NewRouter(server.Deps{
		Log:        log,
		Config:     cfg,
		Order:      grpcClients.OrderClient,
		Balance:    grpcClients.BalanceClient,
		MarketData: mdClients.Client,
		WSHandler:  wsServer.HandleWS,
	})
	httpServer := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info().
			Str("config", *configPath).
			Str("http_listen", cfg.HTTPListen).
			Str("order_grpc_addr", cfg.OrderGRPCAddr).
			Str("marketdata_grpc_addr", cfg.MarketDataGRPCAddr).
			Msg("gateway ready")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http serve")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("http shutdown")
	}
}
