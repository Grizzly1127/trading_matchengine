// Matching 引擎本地进程入口。
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
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/logger"
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
		Str("log_file", cfg.Log.File).
		Bool("log_async", cfg.Log.Async).
		Uint64("recovered_offset", eng.RecoveredOffset()).
		Uint64("last_seq", eng.LastSeq()).
		Msg("matching engine ready")

	input := os.Stdin
	if cfg.CommandsFile != "" {
		f, err := os.Open(cfg.CommandsFile)
		if err != nil {
			log.Fatal().Err(err).Str("file", cfg.CommandsFile).Msg("open commands file")
		}
		defer f.Close()
		input = f
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runErr := cli.Run(ctx, eng, cli.Config{
		DefaultSymbol: cfg.DefaultSymbol,
		Input:         input,
		Output:        os.Stdout,
		UsageOutput:   os.Stderr,
		ShowUsageHint: cfg.CommandsFile == "" && isTerminal(os.Stdin),
	})

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

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
