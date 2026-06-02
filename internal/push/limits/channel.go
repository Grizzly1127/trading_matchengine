package limits

import "strings"

// IsTickerAllChannel 是否为做市商全市场 Ticker 频道。
func IsTickerAllChannel(ch string) bool {
	return ch == "ticker@all" || strings.HasPrefix(ch, "ticker@all:")
}

// SymbolFromChannel 从 per-symbol 频道解析交易对；非 symbol 类频道返回 ("", false)。
func SymbolFromChannel(ch string) (string, bool) {
	if ch == "" || IsTickerAllChannel(ch) || ch == "order" {
		return "", false
	}
	if uid, ok := parseOrderChannel(ch); ok {
		_ = uid
		return "", false
	}
	if sym, ok := symbolAfterPrefix(ch, "depth:"); ok {
		return sym, true
	}
	if sym, ok := symbolAfterPrefix(ch, "ticker:"); ok {
		return sym, true
	}
	if sym, ok := symbolAfterPrefix(ch, "trade:"); ok {
		return sym, true
	}
	if sym, ok := symbolAfterPrefix(ch, "index:"); ok {
		return sym, true
	}
	if rest, ok := strings.CutPrefix(ch, "kline:"); ok {
		sym, _, ok := strings.Cut(rest, ":")
		if ok && sym != "" {
			return sym, true
		}
	}
	return "", false
}

func symbolAfterPrefix(ch, prefix string) (string, bool) {
	sym, ok := strings.CutPrefix(ch, prefix)
	if !ok || sym == "" {
		return "", false
	}
	return sym, true
}

func parseOrderChannel(ch string) (uint64, bool) {
	if _, ok := strings.CutPrefix(ch, "order:"); !ok {
		return 0, false
	}
	// order:{user_id} 不计入 symbol 配额
	return 1, true
}

// CountUniqueSymbols 统计频道列表中的唯一交易对数量。
func CountUniqueSymbols(channels []string) int {
	set := make(map[string]struct{})
	for _, ch := range channels {
		if sym, ok := SymbolFromChannel(strings.TrimSpace(ch)); ok {
			set[sym] = struct{}{}
		}
	}
	return len(set)
}

// MergeSymbolCount 已有订阅集合再合并候选频道后的 symbol 数。
func MergeSymbolCount(existing []string, add []string) int {
	set := make(map[string]struct{})
	for _, ch := range existing {
		if sym, ok := SymbolFromChannel(ch); ok {
			set[sym] = struct{}{}
		}
	}
	for _, ch := range add {
		if sym, ok := SymbolFromChannel(strings.TrimSpace(ch)); ok {
			set[sym] = struct{}{}
		}
	}
	return len(set)
}
