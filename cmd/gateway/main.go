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
	gwmetrics "github.com/Grizzly1127/trading_matchengine/internal/gateway/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/server"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
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

	verifier, err := cfg.NewVerifier(initCtx)
	if err != nil {
		log.Fatal().Err(err).Msg("auth verifier")
	}
	defer verifier.Close()

	orderClients, err := client.ConnectOrder(initCtx, cfg.OrderService)
	if err != nil {
		log.Fatal().Err(err).Str("order_grpc_addr", cfg.OrderService.GRPCAddr).Msg("connect order service")
	}
	defer orderClients.Close()

	mdClients, err := client.ConnectMarketData(initCtx, cfg.MarketDataService)
	if err != nil {
		log.Fatal().Err(err).Str("marketdata_grpc_addr", cfg.MarketDataService.GRPCAddr).Msg("connect marketdata service")
	}
	defer mdClients.Close()

	klClients, err := client.ConnectKline(initCtx, cfg.KlineService)
	if err != nil {
		log.Fatal().Err(err).Str("kline_grpc_addr", cfg.KlineService.GRPCAddr).Msg("connect kline service")
	}
	defer klClients.Close()

	ipClients, err := client.ConnectIndexPrice(initCtx, cfg.IndexPriceService)
	if err != nil {
		log.Fatal().Err(err).Str("indexprice_grpc_addr", cfg.IndexPriceService.GRPCAddr).Msg("connect indexprice service")
	}
	defer ipClients.Close()

	rulesCfg, err := cfg.LoadRules()
	if err != nil {
		log.Fatal().Err(err).Msg("symbol rules")
	}

	tlsCfg, err := auth.ServerTLS(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("tls config")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	gwMet := gwmetrics.New()
	if cfg.MetricsListen != "" {
		go startGatewayMetricsHTTP(ctx, log, cfg.MetricsListen)
	}

	router := server.NewRouter(server.Deps{
		Log:        log,
		Config:     cfg,
		Verifier:   verifier,
		Metrics:    gwMet,
		Order:      orderClients.OrderClient,
		Balance:    orderClients.BalanceClient,
		MarketData: mdClients.Client,
		Kline:      klClients.Client,
		IndexPrice: ipClients.Client,
		Symbols:    rulesCfg.Registry,
		Assets:     rulesCfg.Assets,
	})
	httpServer := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig:         tlsCfg,
	}

	go func() {
		log.Info().
			Str("config", *configPath).
			Str("http_listen", cfg.HTTPListen).
			Str("auth_mode", cfg.Auth.Mode).
			Bool("tls_enabled", cfg.TLS.Enabled).
			Str("order_grpc_addr", cfg.OrderService.GRPCAddr).
			Str("marketdata_grpc_addr", cfg.MarketDataService.GRPCAddr).
			Str("kline_grpc_addr", cfg.KlineService.GRPCAddr).
			Str("indexprice_grpc_addr", cfg.IndexPriceService.GRPCAddr).
			Msg("gateway ready")
		var err error
		if tlsCfg != nil {
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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

func startGatewayMetricsHTTP(ctx context.Context, log zerolog.Logger, addr string) {
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
	log.Info().Str("listen", addr).Msg("gateway metrics listening")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error().Err(err).Msg("gateway metrics serve")
	}
}
