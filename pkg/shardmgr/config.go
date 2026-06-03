package shardmgr

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// ShardDef 静态分片定义（第一版配置驱动）。
type ShardDef struct {
	ShardID        string   `json:"shard_id"`
	KafkaPartition int      `json:"kafka_partition"`
	Node           string   `json:"node"`
	Tier           Tier     `json:"tier"`
	Symbols        []string `json:"symbols"`
}

// MigrationDef 可选：配置文件中的进行中迁移（通常由运维写入）。
type MigrationDef struct {
	Symbol      string         `json:"symbol"`
	FromShardID string         `json:"from_shard_id"`
	ToShardID   string         `json:"to_shard_id"`
	ToPartition int            `json:"to_kafka_partition"`
	ToNode      string         `json:"to_node"`
	Phase       MigrationPhase `json:"phase"`
}

// FileConfig shards.json 顶层结构。
type FileConfig struct {
	Shards     []ShardDef     `json:"shards"`
	Migrations []MigrationDef `json:"migrations,omitempty"`
}

// LoadFile 从 JSON 文件加载并构建 Manager。
func LoadFile(path string) (*Manager, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: path is required", ErrInvalidConfig)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("shardmgr: read %q: %w", path, err)
	}
	var fc FileConfig
	if err := json.Unmarshal(b, &fc); err != nil {
		return nil, fmt.Errorf("shardmgr: parse %q: %w", path, err)
	}
	return NewFromConfig(fc)
}

// NewFromConfig 由内存配置构建 Manager（测试用）。
func NewFromConfig(fc FileConfig) (*Manager, error) {
	m := &Manager{
		routes:     make(map[string]Route),
		byShard:    make(map[string][]string),
		migrations: make(map[string]*migrationState),
	}
	for _, sh := range fc.Shards {
		if err := m.addShardDef(sh); err != nil {
			return nil, err
		}
	}
	for _, md := range fc.Migrations {
		if err := m.applyMigrationDef(md); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) addShardDef(sh ShardDef) error {
	shardID := strings.TrimSpace(sh.ShardID)
	if shardID == "" {
		return fmt.Errorf("%w: empty shard_id", ErrInvalidConfig)
	}
	if sh.KafkaPartition < 0 {
		return fmt.Errorf("%w: negative kafka_partition on shard %q", ErrInvalidConfig, shardID)
	}
	node := strings.TrimSpace(sh.Node)
	if node == "" {
		return fmt.Errorf("%w: empty node on shard %q", ErrInvalidConfig, shardID)
	}
	tier := sh.Tier
	if tier == "" {
		tier = TierShared
	}
	if tier != TierExclusive && tier != TierShared {
		return fmt.Errorf("%w: invalid tier %q on shard %q", ErrInvalidConfig, sh.Tier, shardID)
	}
	for _, sym := range sh.Symbols {
		symbol := strings.TrimSpace(sym)
		if symbol == "" {
			continue
		}
		if _, exists := m.routes[symbol]; exists {
			return fmt.Errorf("%w: %s", ErrDuplicateSymbol, symbol)
		}
		m.routes[symbol] = Route{
			Symbol:         symbol,
			ShardID:        shardID,
			KafkaPartition: sh.KafkaPartition,
			Node:           node,
			Tier:           tier,
		}
		m.byShard[shardID] = append(m.byShard[shardID], symbol)
	}
	return nil
}

func (m *Manager) applyMigrationDef(md MigrationDef) error {
	symbol := strings.TrimSpace(md.Symbol)
	if symbol == "" {
		return fmt.Errorf("%w: migration missing symbol", ErrInvalidConfig)
	}
	from, ok := m.routes[symbol]
	if !ok {
		return fmt.Errorf("%w: migration symbol %q", ErrSymbolNotMapped, symbol)
	}
	if strings.TrimSpace(md.FromShardID) != "" && md.FromShardID != from.ShardID {
		return fmt.Errorf("%w: migration from_shard_id mismatch for %q", ErrInvalidConfig, symbol)
	}
	phase := md.Phase
	if phase == "" {
		phase = MigrationHaltOnlyCancel
	}
	if phase == MigrationComplete {
		return m.completeMigrationLocked(symbol)
	}
	toShard := strings.TrimSpace(md.ToShardID)
	if toShard == "" {
		return fmt.Errorf("%w: migration to_shard_id required for %q", ErrInvalidConfig, symbol)
	}
	toNode := strings.TrimSpace(md.ToNode)
	if toNode == "" {
		return fmt.Errorf("%w: migration to_node required for %q", ErrInvalidConfig, symbol)
	}
	if md.ToPartition < 0 {
		return fmt.Errorf("%w: migration to_kafka_partition for %q", ErrInvalidConfig, symbol)
	}
	m.migrations[symbol] = &migrationState{
		From: from,
		To: Route{
			Symbol:         symbol,
			ShardID:        toShard,
			KafkaPartition: md.ToPartition,
			Node:           toNode,
			Tier:           TierExclusive,
		},
		Phase: phase,
	}
	return nil
}
