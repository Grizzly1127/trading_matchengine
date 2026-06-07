package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_KafkaDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "order.json")
	const body = `{
  "database_url": "postgres://localhost/db",
  "kafka": {
    "brokers": ["localhost:9092"]
  }
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Kafka.MatchTopic != "match.events" {
		t.Fatalf("match_topic=%q", cfg.Kafka.MatchTopic)
	}
	if cfg.Kafka.TradeTopic != "trade.events" {
		t.Fatalf("trade_topic=%q", cfg.Kafka.TradeTopic)
	}
	if cfg.Kafka.GroupID != "order-service" {
		t.Fatalf("group_id=%q", cfg.Kafka.GroupID)
	}
	if !cfg.Kafka.ConsumerEnabled {
		t.Fatal("expected consumer_enabled=true by default")
	}
	if cfg.Kafka.ConsumerStartOffset != -1 {
		t.Fatalf("consumer_start_offset=%d want -1", cfg.Kafka.ConsumerStartOffset)
	}
}

func TestLoad_OutboxRelayAndKafkaBatchDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "order.json")
	const body = `{
  "database_url": "postgres://localhost/db",
  "kafka": { "brokers": ["localhost:9092"] }
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OutboxRelay.PollIntervalMs != 20 {
		t.Fatalf("poll_interval_ms=%d want 20", cfg.OutboxRelay.PollIntervalMs)
	}
	if cfg.OutboxRelay.BatchSize != 500 {
		t.Fatalf("outbox batch_size=%d want 500", cfg.OutboxRelay.BatchSize)
	}
	if cfg.OutboxRelay.MaxRetry != 100 {
		t.Fatalf("max_retry=%d want 100", cfg.OutboxRelay.MaxRetry)
	}
	if cfg.OutboxRelay.Workers != 1 {
		t.Fatalf("default workers=%d want 1", cfg.OutboxRelay.Workers)
	}
	if cfg.Kafka.BatchSize != 500 {
		t.Fatalf("kafka batch_size=%d want 500", cfg.Kafka.BatchSize)
	}
	if cfg.Kafka.BatchTimeoutMs != 5 {
		t.Fatalf("kafka batch_timeout_ms=%d want 5", cfg.Kafka.BatchTimeoutMs)
	}
	rc := cfg.OutboxRelayRuntime(0, nil)
	if rc.BatchSize != 500 || rc.MaxRetry != 100 {
		t.Fatalf("OutboxRelayRuntime: %+v", rc)
	}
	if rc.PollInterval.Milliseconds() != 20 {
		t.Fatalf("poll interval=%s want 20ms", rc.PollInterval)
	}
}

func TestLoad_DatabaseDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "order.json")
	const body = `{
  "database_url": "postgres://localhost/db",
  "kafka": { "brokers": ["localhost:9092"] }
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Database.MaxConns != 50 {
		t.Fatalf("max_conns=%d want 50", cfg.Database.MaxConns)
	}
	if cfg.Database.RelayMaxConns != 20 {
		t.Fatalf("relay_max_conns=%d want 20", cfg.Database.RelayMaxConns)
	}
}

func TestLoad_SymbolDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "order.json")
	const body = `{
  "database_url": "postgres://localhost/db",
  "kafka": { "brokers": ["localhost:9092"] },
  "symbols": {
    "BTC-USDT": {
      "price_precision": 2,
      "quantity_precision": 6,
      "min_quantity": "0.000001",
      "min_notional": "5"
    }
  }
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, err := cfg.SymbolRegistry()
	if err != nil {
		t.Fatalf("SymbolRegistry: %v", err)
	}
	sp, err := reg.Lookup("BTC-USDT")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if sp.PricePrecision != 2 || sp.QuantityPrecision != 6 {
		t.Fatalf("precision: %+v", sp)
	}
}
