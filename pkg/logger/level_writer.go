package logger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// LevelSplitWriter 按级别分流：info 及以下 → stdout，warn 及以上 → stderr。
type LevelSplitWriter struct {
	Stdout io.Writer
	Stderr io.Writer
	Dev    bool
}

// WriteLevel 实现 zerolog.LevelWriter。
func (w LevelSplitWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	out := w.Stdout
	if w.Stdout == nil {
		out = os.Stdout
	}
	errOut := w.Stderr
	if w.Stderr == nil {
		errOut = os.Stderr
	}

	if level >= zerolog.WarnLevel {
		out = errOut
	}

	if w.Dev {
		cw := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
		return cw.Write(p)
	}
	return out.Write(p)
}

// Write 满足 io.Writer；无级别信息时写入 stdout。
func (w LevelSplitWriter) Write(p []byte) (int, error) {
	return w.WriteLevel(zerolog.InfoLevel, p)
}
