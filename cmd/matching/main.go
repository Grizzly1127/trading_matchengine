// Matching 引擎进程入口：本地 JSONL（3.1）或 Kafka（3.2）。
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

	"github.com/Grizzly1127/trading_matchengine/internal/matching/admin"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/cli"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/config"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

func main() {
	configPath := flag.String("config", "configs/matching.json", "配置文件路径（JSON）")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -config <path>\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, cli.Usage())
	}
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logRes, err := logger.New(logger.Config{
		Service:    "matching",
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

	symbolRegistry, err := cfg.SymbolRegistry()
	if err != nil {
		log.Fatal().Err(err).Msg("symbol registry")
	}

	m := metrics.New()
	m.SetWalLastSeq(0)

	eng, err := recovery.Open(recovery.Config{
		ShardID:        cfg.ShardID,
		DataDir:        cfg.DataDir,
		SnapshotEvery:  cfg.SnapshotEvery,
		SymbolRegistry: symbolRegistry,
		Metrics:        m,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("open engine")
	}

	log.Info().
		Str("config", *configPath).
		Str("shard_id", cfg.ShardID).
		Str("data_dir", cfg.DataDir).
		Bool("kafka_enabled", cfg.Kafka.Enabled).
		Uint64("recovered_offset", eng.RecoveredOffset()).
		Uint64("last_seq", eng.LastSeq()).
		Msg("matching engine ready")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.MetricsListen != "" {
		go startMetricsHTTP(ctx, log, cfg.MetricsListen)
		go startMetricsLogger(ctx, log, m)
	}
	if cfg.AdminGRPCListen != "" {
		go startAdminGRPC(ctx, log, cfg.AdminGRPCListen, eng)
	}
	m.SetWalLastSeq(eng.LastSeq())

	var runErr error
	if cfg.Kafka.Enabled {
		runErr = runKafka(ctx, cfg, eng, log, m)
	} else {
		runErr = runCLI(ctx, cfg, eng)
	}

	lastSeq := eng.LastSeq()
	if err := cli.Shutdown(eng, cfg.SnapshotOnExit); err != nil {
		log.Error().Err(err).Msg("shutdown")
		if runErr == nil {
			runErr = err
		}
	} else if cfg.SnapshotOnExit {
		log.Info().Uint64("last_seq", lastSeq).Msg("snapshot saved")
	}

	if runErr != nil {
		log.Fatal().Err(runErr).Msg("exit")
	}
}

func runCLI(ctx context.Context, cfg config.Config, eng *recovery.Engine) error {
	input := os.Stdin
	if cfg.CommandsFile != "" {
		f, err := os.Open(cfg.CommandsFile)
		if err != nil {
			return fmt.Errorf("open commands file: %w", err)
		}
		defer f.Close()
		input = f
	}
	return cli.Run(ctx, eng, cli.Config{
		DefaultSymbol: cfg.DefaultSymbol,
		Input:         input,
		Output:        os.Stdout,
		UsageOutput:   os.Stderr,
		ShowUsageHint: cfg.CommandsFile == "" && isTerminal(os.Stdin),
	})
}

func runKafka(ctx context.Context, cfg config.Config, eng *recovery.Engine, log zerolog.Logger, m *metrics.Metrics) error {
	partition := cfg.Kafka.Partition
	resume, hasResume := eng.MaxKafkaOffset(uint32(partition))
	start := consumer.StartOffset(resume, hasResume)

	log.Info().
		Int("partition", partition).
		Uint64("resume_offset", resume).
		Bool("has_resume", hasResume).
		Int64("start_offset", start).
		Strs("brokers", cfg.Kafka.Brokers).
		Str("command_topic", cfg.Kafka.CommandTopic).
		Msg("kafka consumer starting")

	// Matching 以 WAL 维护 kafka 位点，不用 consumer group（避免 __consumer_offsets 残留导致空 WAL 仍重放历史）。
	reader, err := kafka.NewCommandReader(kafka.ReaderConfig{
		Brokers:     cfg.Kafka.Brokers,
		Topic:       cfg.Kafka.CommandTopic,
		GroupID:     "",
		Partition:   partition,
		StartOffset: start,
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	writer := kafka.NewEventWriter(kafka.WriterConfig{Brokers: cfg.Kafka.Brokers})
	defer writer.Close()

	pub := &publisher.KafkaPublisher{
		Producer:   writer,
		MatchTopic: cfg.Kafka.MatchTopic,
		TradeTopic: cfg.Kafka.TradeTopic,
	}
	h := &consumer.Handler{
		Engine:    eng,
		Publisher: pub,
		Partition: uint32(partition),
		Metrics:   m,
	}
	go pollKafkaLag(ctx, reader, m, log)
	return consumer.Run(ctx, reader, h)
}

func pollKafkaLag(ctx context.Context, reader *kafka.CommandReader, m *metrics.Metrics, log zerolog.Logger) {
	if m == nil || reader == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lag, err := reader.ReadLag(ctx)
			if err != nil {
				log.Debug().Err(err).Msg("kafka read lag")
				continue
			}
			m.SetKafkaLag(lag)
		}
	}
}

func startAdminGRPC(ctx context.Context, log zerolog.Logger, addr string, eng *recovery.Engine) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Err(err).Str("listen", addr).Msg("matching admin grpc listen failed")
		return
	}
	srv := grpc.NewServer()
	matchingv1.RegisterMatchingAdminServiceServer(srv, &admin.Server{Engine: eng})
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	log.Info().Str("listen", addr).Msg("matching admin grpc listening")
	if err := srv.Serve(lis); err != nil {
		log.Error().Err(err).Msg("matching admin grpc serve")
	}
}

func startMetricsHTTP(ctx context.Context, log zerolog.Logger, addr string) {
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
	log.Info().Str("listen", addr).Msg("matching metrics listening")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error().Err(err).Msg("matching metrics serve")
	}
}

func startMetricsLogger(ctx context.Context, log zerolog.Logger, m *metrics.Metrics) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := m.Snap()
				log.Info().
					Uint64("commands_processed", s.CommandsProcessed).
					Uint64("commands_failed", s.CommandsFailed).
					Int64("kafka_lag", s.KafkaLag).
					Uint64("last_processed_offset", s.LastProcessedOffset).
					Uint64("wal_last_seq", s.WalLastSeq).
					Msg("matching metrics")
			}
		}
	}()
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
