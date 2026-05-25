package config

import (
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/reconciler"
)

// ReconcilerRuntime 将配置转为 scheduler 运行时参数。
func (c Config) ReconcilerRuntime(commandTopic string) reconciler.Config {
	rc := c.Reconciler
	return reconciler.Config{
		Enabled:              rc.Enabled,
		Interval:             time.Duration(rc.IntervalSeconds) * time.Second,
		BatchSize:            rc.BatchSize,
		PendingAcceptTimeout: time.Duration(rc.PendingAcceptTimeoutSeconds) * time.Second,
		CancelConfirmTimeout: time.Duration(rc.CancelConfirmTimeoutSeconds) * time.Second,
		OutboxStaleWarn:      time.Duration(rc.OutboxStaleWarnSeconds) * time.Second,
		CommandTopic:         commandTopic,
	}
}
