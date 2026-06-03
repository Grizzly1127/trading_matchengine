package shardbind

import (
	"fmt"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/shardmgr"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

// Apply 加载分片映射、校验本 shard 归属，并在迁移 drain/cutover 时将 symbol 设为只读。
func Apply(eng *recovery.Engine, shardID, shardsFile string, reg *symbolrules.Registry) (*shardmgr.Manager, error) {
	if strings.TrimSpace(shardsFile) == "" {
		return nil, nil
	}
	mgr, err := shardmgr.LoadFile(shardsFile)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		return mgr, nil
	}
	symbols := make([]string, 0, len(reg.All()))
	for _, sp := range reg.All() {
		symbols = append(symbols, sp.Symbol)
	}
	if err := mgr.ValidateLocalShard(shardID, symbols); err != nil {
		return nil, fmt.Errorf("shard bind: %w", err)
	}
	for _, sp := range reg.All() {
		if mgr.ShouldSymbolReadOnly(sp.Symbol, shardID) {
			eng.SetSymbolReadOnly(sp.Symbol, "shard migration: source shard read-only")
		}
	}
	return mgr, nil
}
