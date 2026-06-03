package shardmgr

import "strings"

type migrationState struct {
	From  Route
	To    Route
	Phase MigrationPhase
}

var phaseOrder = []MigrationPhase{
	MigrationHaltOnlyCancel,
	MigrationHaltNoWrite,
	MigrationDrain,
	MigrationCutover,
	MigrationComplete,
}

func phaseIndex(p MigrationPhase) int {
	for i, ph := range phaseOrder {
		if ph == p {
			return i
		}
	}
	return -1
}

// StartMigration 启动 symbol 从源 shard 到目标 shard 的受控迁移（内存态，防双写）。
func (m *Manager) StartMigration(symbol string, to Route) error {
	if m == nil {
		return ErrInvalidConfig
	}
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	from, ok := m.routes[symbol]
	if !ok {
		return ErrSymbolNotMapped
	}
	if _, active := m.migrations[symbol]; active {
		return ErrMigrationActive
	}
	if strings.TrimSpace(to.ShardID) == "" || strings.TrimSpace(to.Node) == "" || to.KafkaPartition < 0 {
		return ErrInvalidConfig
	}
	if to.ShardID == from.ShardID && to.KafkaPartition == from.KafkaPartition {
		return ErrInvalidConfig
	}
	to.Symbol = symbol
	if to.Tier == "" {
		to.Tier = TierExclusive
	}
	m.migrations[symbol] = &migrationState{
		From:  from,
		To:    to,
		Phase: MigrationHaltOnlyCancel,
	}
	return nil
}

// SetMigrationPhase 推进迁移阶段（必须按序）。
func (m *Manager) SetMigrationPhase(symbol string, phase MigrationPhase) error {
	if m == nil {
		return ErrInvalidConfig
	}
	symbol = strings.TrimSpace(symbol)
	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.migrations[symbol]
	if !ok {
		return ErrMigrationNotActive
	}
	if phase == MigrationComplete {
		return m.completeMigrationLocked(symbol)
	}
	cur := phaseIndex(st.Phase)
	next := phaseIndex(phase)
	if cur < 0 || next < 0 || next != cur+1 {
		return ErrInvalidPhase
	}
	st.Phase = phase
	return nil
}

// CompleteMigration 结束迁移：symbol 永久路由至目标 shard。
func (m *Manager) CompleteMigration(symbol string) error {
	if m == nil {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.completeMigrationLocked(strings.TrimSpace(symbol))
}

func (m *Manager) completeMigrationLocked(symbol string) error {
	st, ok := m.migrations[symbol]
	if !ok {
		return ErrMigrationNotActive
	}
	oldShard := st.From.ShardID
	m.routes[symbol] = st.To
	delete(m.migrations, symbol)
	m.rebuildShardIndex(symbol, oldShard, st.To.ShardID)
	return nil
}

func (m *Manager) rebuildShardIndex(symbol, oldShard, newShard string) {
	if oldShard != "" {
		m.byShard[oldShard] = removeSymbol(m.byShard[oldShard], symbol)
	}
	m.byShard[newShard] = append(m.byShard[newShard], symbol)
}

func removeSymbol(list []string, symbol string) []string {
	out := list[:0]
	for _, s := range list {
		if s != symbol {
			out = append(out, s)
		}
	}
	return out
}

func (m *Manager) migration(symbol string) (*migrationState, bool) {
	st, ok := m.migrations[symbol]
	return st, ok
}
