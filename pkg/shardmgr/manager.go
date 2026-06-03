package shardmgr

import (
	"strings"
	"sync"
)

// Manager 维护 symbol -> shard_id -> kafka_partition -> node 映射（Shard Manager）。
type Manager struct {
	mu         sync.RWMutex
	routes     map[string]Route
	byShard    map[string][]string
	migrations map[string]*migrationState
}

// Resolve 返回 symbol 当前静态路由（不含迁移中的命令路由覆盖）。
func (m *Manager) Resolve(symbol string) (Route, error) {
	if m == nil {
		return Route{}, ErrInvalidConfig
	}
	symbol = strings.TrimSpace(symbol)
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.routes[symbol]
	if !ok {
		return Route{}, ErrSymbolNotMapped
	}
	return r, nil
}

// CommandRoute 返回发命令时应使用的路由（Order Outbox / 撮合消费前须一致）。
// 迁移 cutover 之后路由目标 shard；此前路由源 shard，避免双写。
func (m *Manager) CommandRoute(symbol string) (Route, error) {
	if m == nil {
		return Route{}, ErrInvalidConfig
	}
	symbol = strings.TrimSpace(symbol)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if st, ok := m.migrations[symbol]; ok {
		if st.Phase == MigrationCutover || st.Phase == MigrationComplete {
			return st.To, nil
		}
		return st.From, nil
	}
	r, ok := m.routes[symbol]
	if !ok {
		return Route{}, ErrSymbolNotMapped
	}
	return r, nil
}

// PartitionForSymbol 实现 Order Outbox Relay 的分区解析。
func (m *Manager) PartitionForSymbol(symbol string) (int, error) {
	r, err := m.CommandRoute(symbol)
	if err != nil {
		return 0, err
	}
	return r.KafkaPartition, nil
}

// AssertPlaceOrder 发新单前检查：未映射或迁移停牌阶段则拒绝。
func (m *Manager) AssertPlaceOrder(symbol string) error {
	if m == nil {
		return nil
	}
	symbol = strings.TrimSpace(symbol)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.routes[symbol]; !ok {
		return ErrSymbolNotMapped
	}
	if st, ok := m.migrations[symbol]; ok {
		switch st.Phase {
		case MigrationHaltOnlyCancel, MigrationHaltNoWrite, MigrationDrain:
			return ErrMigrationNotAllowed
		}
	}
	return nil
}

// AssertCancelOrder 撤单前检查：完全停写阶段拒绝（撮合侧亦不应再收命令）。
func (m *Manager) AssertCancelOrder(symbol string) error {
	if m == nil {
		return nil
	}
	symbol = strings.TrimSpace(symbol)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.routes[symbol]; !ok {
		return ErrSymbolNotMapped
	}
	if st, ok := m.migrations[symbol]; ok && st.Phase == MigrationHaltNoWrite {
		return ErrMigrationNotAllowed
	}
	return nil
}

// SymbolsForShard 返回某 shard 负责的交易对列表。
func (m *Manager) SymbolsForShard(shardID string) []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.byShard[shardID]))
	copy(out, m.byShard[shardID])
	return out
}

// ValidateLocalShard 校验本进程 shard_id 与 symbols 配置一致（Matching 启动用）。
func (m *Manager) ValidateLocalShard(shardID string, symbols []string) error {
	if m == nil {
		return nil
	}
	shardID = strings.TrimSpace(shardID)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sym := range symbols {
		symbol := strings.TrimSpace(sym)
		if symbol == "" {
			continue
		}
		r, ok := m.routes[symbol]
		if !ok {
			return ErrSymbolNotMapped
		}
		if st, mig := m.migrations[symbol]; mig && st.Phase == MigrationCutover {
			if r.ShardID != shardID && st.To.ShardID != shardID {
				return ErrSymbolNotOnShard
			}
			continue
		}
		if r.ShardID != shardID {
			return ErrSymbolNotOnShard
		}
	}
	return nil
}

// MigrationPhase 返回 symbol 当前迁移阶段；无迁移时 ok=false。
func (m *Manager) MigrationPhase(symbol string) (MigrationPhase, bool) {
	if m == nil {
		return "", false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.migrations[strings.TrimSpace(symbol)]
	if !ok {
		return "", false
	}
	return st.Phase, true
}

// ShouldSymbolReadOnly 迁移 drain/cutover 时源 shard 应将 symbol 设为只读（防双写）。
func (m *Manager) ShouldSymbolReadOnly(symbol, localShardID string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.migrations[strings.TrimSpace(symbol)]
	if !ok {
		return false
	}
	switch st.Phase {
	case MigrationDrain, MigrationCutover:
		return st.From.ShardID == strings.TrimSpace(localShardID)
	default:
		return false
	}
}
