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
