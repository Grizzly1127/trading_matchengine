package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// RotateConfig 控制 lumberjack 日志切割与保留。
type RotateConfig struct {
	MaxSizeMB   int  // 单文件最大 MB，超过则切割
	MaxAgeDays  int  // 保留最近 N 天
	MaxBackups  int  // 最多保留备份个数，0 表示不按个数限制（仍受 MaxAgeDays 约束）
	Compress    bool // 是否 gzip 压缩历史文件
	LocalTime   bool // 备份文件名是否用本地时间
	RotateDaily bool // 是否在自然日切换时切割
}

type fileSink struct {
	w     io.Writer
	close func() error
}

func openFileSink(path string, rot RotateConfig) (*fileSink, error) {
	if path == "" {
		return nil, fmt.Errorf("logger: log file path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("logger: mkdir for log file: %w", err)
	}

	if rot.MaxSizeMB <= 0 {
		rot.MaxSizeMB = 100
	}
	if rot.MaxAgeDays <= 0 {
		rot.MaxAgeDays = 7
	}

	lj := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    rot.MaxSizeMB,
		MaxAge:     rot.MaxAgeDays,
		MaxBackups: rot.MaxBackups,
		Compress:   rot.Compress,
		LocalTime:  rot.LocalTime,
	}

	var w io.Writer = lj
	if rot.RotateDaily {
		w = &dailyRotateWriter{inner: lj}
	}

	return &fileSink{
		w:     w,
		close: func() error { return lj.Close() },
	}, nil
}

// dailyRotateWriter 在日期变化时调用 lumberjack.Rotate，实现按天切割。
type dailyRotateWriter struct {
	inner  *lumberjack.Logger
	mu     sync.Mutex
	curDay string
}

func (d *dailyRotateWriter) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	today := time.Now().Format("2006-01-02")
	if d.curDay != "" && today != d.curDay {
		if err := d.inner.Rotate(); err != nil {
			return 0, err
		}
	}
	d.curDay = today
	return d.inner.Write(p)
}
