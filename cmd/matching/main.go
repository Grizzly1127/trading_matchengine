// Matching 引擎进程入口：本地 JSONL（3.1）或 Kafka（3.2）。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/cli"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/config"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
	"github.com/rs/zerolog"
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

	eng, err := recovery.Open(recovery.Config{
		ShardID:       cfg.ShardID,
		DataDir:       cfg.DataDir,
		SnapshotEvery: cfg.SnapshotEvery,
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

	var runErr error
	if cfg.Kafka.Enabled {
		runErr = runKafka(ctx, cfg, eng, log)
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

func runKafka(ctx context.Context, cfg config.Config, eng *recovery.Engine, log zerolog.Logger) error {
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
	}
	return consumer.Run(ctx, reader, h)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
