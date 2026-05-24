package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/config"
)

func TestLoad_kafkaEnabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matching.json")
	content := `{
  "kafka": {
    "enabled": true,
    "brokers": ["localhost:9092"],
    "group_id": "matching-shard-0",
    "command_topic": "order.commands",
    "match_topic": "match.events",
    "trade_topic": "trade.events",
    "partition": 0
  }
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Kafka.Enabled {
		t.Fatal("kafka should be enabled")
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.GroupID == "" {
		t.Fatalf("kafka = %+v", cfg.Kafka)
	}
}

func TestLoad_kafkaEnabledMissingBrokers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matching.json")
	if err := os.WriteFile(path, []byte(`{"kafka":{"enabled":true}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
}
