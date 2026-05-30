package symbol

import "github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"

// RegisterRegistry 将规则表中的交易对注册到分片。
func RegisterRegistry(s *Shard, reg *symbolrules.Registry) {
	if s == nil || reg == nil {
		return
	}
	for _, sp := range reg.All() {
		s.Register(NewSymbolEngineFromSpec(sp))
	}
}
