package config

import (
	"path/filepath"

	"github.com/Grizzly1127/trading_matchengine/pkg/eventoutbox"
)

// EventOutboxConfig 本地事件 Outbox（异步发布）。
type EventOutboxConfig struct {
	Enabled          bool `json:"enabled"`
	SyncEveryRecords int  `json:"sync_every_records"`
	SyncIntervalMs   int  `json:"sync_interval_ms"`
}

// EventRelayConfig Event Outbox Relay。
type EventRelayConfig struct {
	PollIntervalMs int `json:"poll_interval_ms"`
	BatchSize      int `json:"batch_size"`
	Workers        int `json:"workers"`
}

// EventOutboxDir 返回 Event Outbox 存储路径。
func (c Config) EventOutboxDir() string {
	return filepath.Join(c.DataDir, "event_outbox", c.ShardID)
}

// EventOutboxWriterConfig 转为 pkg/eventoutbox 写配置。
func (c Config) EventOutboxWriterConfig() eventoutbox.FileWriterConfig {
	syncEvery := c.EventOutbox.SyncEveryRecords
	if syncEvery <= 0 {
		syncEvery = c.WALGroupCommit.SyncEveryRecords
	}
	syncInterval := c.EventOutbox.SyncIntervalMs
	if syncInterval <= 0 {
		syncInterval = c.WALGroupCommit.SyncIntervalMs
	}
	return eventoutbox.FileWriterConfig{
		SyncEveryRecords: syncEvery,
		SyncIntervalMs:   syncInterval,
	}
}
