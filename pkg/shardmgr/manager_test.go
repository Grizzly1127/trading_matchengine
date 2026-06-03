package shardmgr_test

import (
	"errors"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/pkg/shardmgr"
)

func testConfig() shardmgr.FileConfig {
	return shardmgr.FileConfig{
		Shards: []shardmgr.ShardDef{
			{
				ShardID: "shard-btc", KafkaPartition: 0, Node: "matching-btc-0",
				Tier: shardmgr.TierExclusive, Symbols: []string{"BTC-USDT"},
			},
			{
				ShardID: "shard-0", KafkaPartition: 1, Node: "matching-0-0",
				Tier: shardmgr.TierShared, Symbols: []string{"ETH-USDT", "SOL-USDT"},
			},
		},
	}
}

func TestResolve_exclusiveAndShared(t *testing.T) {
	m, err := shardmgr.NewFromConfig(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	btc, err := m.Resolve("BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if btc.Tier != shardmgr.TierExclusive || btc.KafkaPartition != 0 || btc.ShardID != "shard-btc" {
		t.Fatalf("btc route: %+v", btc)
	}
	eth, err := m.CommandRoute("ETH-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if eth.Tier != shardmgr.TierShared || eth.KafkaPartition != 1 {
		t.Fatalf("eth route: %+v", eth)
	}
}

func TestMigration_preventsDoubleWrite(t *testing.T) {
	m, err := shardmgr.NewFromConfig(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	to := shardmgr.Route{
		ShardID: "shard-eth", KafkaPartition: 2, Node: "matching-eth-0", Tier: shardmgr.TierExclusive,
	}
	if err := m.StartMigration("ETH-USDT", to); err != nil {
		t.Fatal(err)
	}
	if err := m.AssertPlaceOrder("ETH-USDT"); !errors.Is(err, shardmgr.ErrMigrationNotAllowed) {
		t.Fatalf("place during halt: %v", err)
	}
	r, err := m.CommandRoute("ETH-USDT")
	if err != nil || r.ShardID != "shard-0" {
		t.Fatalf("command route before cutover: %+v %v", r, err)
	}
	for _, ph := range []shardmgr.MigrationPhase{
		shardmgr.MigrationHaltNoWrite,
		shardmgr.MigrationDrain,
		shardmgr.MigrationCutover,
	} {
		if err := m.SetMigrationPhase("ETH-USDT", ph); err != nil {
			t.Fatalf("phase %s: %v", ph, err)
		}
	}
	r, err = m.CommandRoute("ETH-USDT")
	if err != nil || r.ShardID != "shard-eth" || r.KafkaPartition != 2 {
		t.Fatalf("command route after cutover: %+v %v", r, err)
	}
	if !m.ShouldSymbolReadOnly("ETH-USDT", "shard-0") {
		t.Fatal("source shard should be read-only during cutover")
	}
	if err := m.CompleteMigration("ETH-USDT"); err != nil {
		t.Fatal(err)
	}
	final, _ := m.Resolve("ETH-USDT")
	if final.ShardID != "shard-eth" {
		t.Fatalf("after complete: %+v", final)
	}
}

func TestValidateLocalShard(t *testing.T) {
	m, err := shardmgr.NewFromConfig(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateLocalShard("shard-btc", []string{"BTC-USDT"}); err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateLocalShard("shard-0", []string{"BTC-USDT"}); !errors.Is(err, shardmgr.ErrSymbolNotOnShard) {
		t.Fatalf("want not on shard: %v", err)
	}
}

func TestDuplicateSymbol(t *testing.T) {
	cfg := testConfig()
	cfg.Shards[1].Symbols = append(cfg.Shards[1].Symbols, "BTC-USDT")
	if _, err := shardmgr.NewFromConfig(cfg); !errors.Is(err, shardmgr.ErrDuplicateSymbol) {
		t.Fatalf("want duplicate: %v", err)
	}
}
