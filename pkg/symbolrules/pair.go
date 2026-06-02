package symbolrules

import (
	"fmt"
	"strings"
)

// Pair 交易对 BASE-QUOTE。
type Pair struct {
	Base  string
	Quote string
}

// ParsePair 解析 BASE-QUOTE。
func ParsePair(symbol string) (Pair, error) {
	symbol = strings.TrimSpace(symbol)
	parts := strings.Split(symbol, "-")
	if len(parts) != 2 {
		return Pair{}, fmt.Errorf("invalid pair %q", symbol)
	}
	base := strings.TrimSpace(parts[0])
	quote := strings.TrimSpace(parts[1])
	if base == "" || quote == "" {
		return Pair{}, fmt.Errorf("invalid pair %q", symbol)
	}
	return Pair{Base: base, Quote: quote}, nil
}
