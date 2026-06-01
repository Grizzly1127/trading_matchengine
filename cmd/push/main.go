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

	"github.com/Grizzly1127/trading_matchengine/internal/push/config"
	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/server"
	"github.com/Grizzly1127/trading_matchengine/internal/push/subscriber"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/go-chi/chi/v5"
)

func main() {
	configPath := flag.String("config", "configs/push.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	logRes, err := logger.New(logger.Config{
		Service:    "push",
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb, err := redis.NewClient(redis.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
	if err != nil {
		log.Fatal().Err(err).Msg("init redis client failed")
	}
	defer rdb.Close()
	if err := rdb.Ping(ctx); err != nil {
		log.Fatal().Err(err).Msg("redis ping failed")
	}

	h := hub.NewWithLimits(cfg.Limits)
	ws := &server.WSServer{Hub: h, Redis: rdb, Verifier: verifier, Limits: cfg.Limits, Log: log}
	sub := &subscriber.RedisFanout{Redis: rdb, Hub: h, Log: log.With().Str("component", "redis_subscriber").Logger()}
	go func() {
		if err := sub.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("redis subscriber stopped")
			stop()
		}
	}()

	tlsCfg, err := auth.ServerTLS(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("tls config")
	}

	router := chi.NewRouter()
	router.Get("/v1/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	router.Get("/v1/ws", ws.HandleWS)

	httpServer := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig:         tlsCfg,
	}
	go func() {
		log.Info().
			Str("http_listen", cfg.HTTPListen).
			Str("auth_mode", cfg.Auth.Mode).
			Bool("tls_enabled", cfg.TLS.Enabled).
			Msg("push service ready")
		var err error
		if tlsCfg != nil {
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http serve")
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
