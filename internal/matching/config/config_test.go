package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/config"
)

func TestLoad_defaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matching.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "data" {
		t.Fatalf("data_dir = %q, want data", cfg.DataDir)
	}
	if cfg.ShardID != "shard-0" {
		t.Fatalf("shard_id = %q, want shard-0", cfg.ShardID)
	}
	if cfg.SnapshotEvery != 10000 {
		t.Fatalf("snapshot_every = %d, want 10000", cfg.SnapshotEvery)
	}
	if cfg.SnapshotIntervalSeconds != 300 {
		t.Fatalf("snapshot_interval_seconds = %d, want 300", cfg.SnapshotIntervalSeconds)
	}
	if !cfg.SnapshotOnExit {
		t.Fatal("snapshot_on_exit should default true")
	}
	if cfg.DefaultSymbol != "BTC-USDT" {
		t.Fatalf("default_symbol = %q", cfg.DefaultSymbol)
	}
	if cfg.Log.Level != "info" || !cfg.Log.Dev {
		t.Fatalf("log = %+v", cfg.Log)
	}
}

func TestLoad_fullFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matching.json")
	content := `{
  "data_dir": "/var/data",
  "shard_id": "shard-1",
  "snapshot_every": 500,
  "snapshot_interval_seconds": 60,
  "snapshot_on_exit": false,
  "commands_file": "orders.jsonl",
  "default_symbol": "ETH-USDT",
  "log": { "level": "debug", "dev": false }
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "/var/data" || cfg.ShardID != "shard-1" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.SnapshotEvery != 500 || cfg.SnapshotIntervalSeconds != 60 || cfg.SnapshotOnExit {
		t.Fatalf("snapshot cfg = %+v", cfg)
	}
	if cfg.CommandsFile != "orders.jsonl" || cfg.DefaultSymbol != "ETH-USDT" {
		t.Fatalf("cli cfg = %+v", cfg)
	}
	if cfg.Log.Level != "debug" || cfg.Log.Dev {
		t.Fatalf("log = %+v", cfg.Log)
	}
}

func TestLoad_missingFile(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "missing.json"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_invalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLoad_logFileDefaultsAsync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matching.json")
	content := `{"log":{"file":"logs/test.log"}}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Log.File != "logs/test.log" {
		t.Fatalf("file = %q", cfg.Log.File)
	}
	if !cfg.Log.Async {
		t.Fatal("async should default true when file is set")
	}
	if cfg.Log.MaxSizeMB != 100 || cfg.Log.MaxAgeDays != 7 {
		t.Fatalf("rotate defaults: size=%d age=%d", cfg.Log.MaxSizeMB, cfg.Log.MaxAgeDays)
	}
	if !cfg.Log.RotateDaily || !cfg.Log.LocalTime {
		t.Fatalf("rotate_daily/local_time = %v/%v", cfg.Log.RotateDaily, cfg.Log.LocalTime)
	}
	if cfg.Log.BufferSize != 512 {
		t.Fatalf("buffer_size = %d, want 512", cfg.Log.BufferSize)
	}
}

func TestLoad_emptyShardID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matching.json")
	if err := os.WriteFile(path, []byte(`{"shard_id":""}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected validation error for empty shard_id")
	}
}
