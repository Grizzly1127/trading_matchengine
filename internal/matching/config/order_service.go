package config

import "time"

// RecoveryVerifyTimeout 启动对账超时。
func (c Config) RecoveryVerifyTimeout() time.Duration {
	sec := c.OrderService.RecoveryVerifyTimeoutSeconds
	if sec <= 0 {
		sec = 30
	}
	return time.Duration(sec) * time.Second
}
