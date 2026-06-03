package shardmgr

import "errors"

// Tier 描述分片部署形态（架构 §2.3.1）。
type Tier string

const (
	// TierExclusive 热门交易对：1 symbol 独占 shard / partition / Pod。
	TierExclusive Tier = "exclusive"
	// TierShared 冷门交易对：多 symbol 共享 shard。
	TierShared Tier = "shared"
)

// Route 为 symbol 的完整路由：symbol -> shard_id -> kafka_partition -> node。
type Route struct {
	Symbol         string
	ShardID        string
	KafkaPartition int
	Node           string
	Tier           Tier
}

// MigrationPhase symbol 迁移阶段（停牌窗口、防双写）。
type MigrationPhase string

const (
	MigrationHaltOnlyCancel MigrationPhase = "halt_only_cancel" // 拒新单，允许撤单
	MigrationHaltNoWrite    MigrationPhase = "halt_no_write"    // 完全停写（含撤单发往撮合）
	MigrationDrain          MigrationPhase = "drain"              // 源 shard 只读，消化存量命令
	MigrationCutover        MigrationPhase = "cutover"            // 命令路由至目标 shard
	MigrationComplete       MigrationPhase = "complete"
)

var (
	ErrSymbolNotMapped     = errors.New("shardmgr: symbol not mapped")
	ErrSymbolNotOnShard    = errors.New("shardmgr: symbol not owned by shard")
	ErrDuplicateSymbol     = errors.New("shardmgr: duplicate symbol mapping")
	ErrInvalidConfig       = errors.New("shardmgr: invalid config")
	ErrMigrationActive     = errors.New("shardmgr: symbol migration in progress")
	ErrMigrationNotActive  = errors.New("shardmgr: no active migration for symbol")
	ErrMigrationNotAllowed = errors.New("shardmgr: operation not allowed during migration")
	ErrInvalidPhase        = errors.New("shardmgr: invalid migration phase transition")
)
