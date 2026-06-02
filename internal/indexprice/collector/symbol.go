package collector

import "strings"

// ToBinanceSymbol BTC-USDT -> BTCUSDT。
func ToBinanceSymbol(symbol string) string {
	return strings.ReplaceAll(symbol, "-", "")
}

// ToOKXInstID BTC-USDT -> BTC-USDT（已是 instId 格式）。
func ToOKXInstID(symbol string) string {
	return symbol
}

// ToBybitSymbol BTC-USDT -> BTCUSDT。
func ToBybitSymbol(symbol string) string {
	return strings.ReplaceAll(symbol, "-", "")
}
