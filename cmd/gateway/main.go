// API Gateway 进程入口：REST → Order / MarketData / Kline gRPC。
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
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
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

	grpcClients, err := client.ConnectOrder(initCtx, cfg.OrderService)
	if err != nil {
		log.Fatal().Err(err).Str("order_grpc_addr", cfg.OrderService.GRPCAddr).Msg("connect order service")
	}
	defer grpcClients.Close()
	mdClients, err := client.ConnectMarketData(initCtx, cfg.MarketDataService)
	if err != nil {
		log.Fatal().Err(err).Str("marketdata_grpc_addr", cfg.MarketDataService.GRPCAddr).Msg("connect marketdata service")
	}
	defer mdClients.Close()

	rulesCfg, err := cfg.LoadRules()
	if err != nil {
		log.Fatal().Err(err).Msg("symbol rules")
	}

	var klineClient klinev1.KlineServiceClient
	if cfg.KlineService.GRPCAddr != "" {
		klClients, err := client.ConnectKline(initCtx, cfg.KlineService)
		if err != nil {
			log.Fatal().Err(err).Str("kline_grpc_addr", cfg.KlineService.GRPCAddr).Msg("connect kline service")
		}
		defer klClients.Close()
		klineClient = klClients.Client
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	router := server.NewRouter(server.Deps{
		Log:        log,
		Config:     cfg,
		Order:      grpcClients.OrderClient,
		Balance:    grpcClients.BalanceClient,
		MarketData: mdClients.Client,
		Kline:      klineClient,
		Symbols:    rulesCfg.Registry,
		Assets:     rulesCfg.Assets,
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
			Str("order_grpc_addr", cfg.OrderService.GRPCAddr).
			Str("marketdata_grpc_addr", cfg.MarketDataService.GRPCAddr).
			Str("kline_grpc_addr", cfg.KlineService.GRPCAddr).
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
