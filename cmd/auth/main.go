// 轻量服务 JWT 签发（dev/staging）；生产可改用外部 IdP + JWKS。
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

	"github.com/Grizzly1127/trading_matchengine/internal/authserver"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
)

func main() {
	configPath := flag.String("config", "configs/auth.json", "配置文件路径（JSON）")
	flag.Parse()

	cfg, err := authserver.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service: "auth",
		Level:   cfg.Log.Level,
		Dev:     cfg.Log.Dev,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer logRes.Close()
	log := logRes.Logger

	srv, err := authserver.New(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("init auth server")
	}

	httpServer := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info().Str("listen", cfg.HTTPListen).Str("issuer", cfg.Issuer).Msg("auth issuer ready")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http serve")
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
