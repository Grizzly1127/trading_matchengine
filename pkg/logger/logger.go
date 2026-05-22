// Package logger provides a thin zerolog wrapper shared by all services.
package logger

import (
	"io"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Config holds logger options (usually from config file).
type Config struct {
	Service    string
	Level      string
	Dev        bool
	File       string
	Async      bool
	BufferSize int
	Rotate     RotateConfig
}

// Result 包含 Logger 与关闭函数（刷新文件写入器）。
type Result struct {
	Logger zerolog.Logger
	close  func() error
}

// Close 刷新并关闭文件写入器。
func (r Result) Close() error {
	if r.close != nil {
		return r.close()
	}
	return nil
}

// New 返回同时写控制台（stdout/stderr 分流）与可选文件的 logger。
func New(cfg Config) (Result, error) {
	level := parseLevel(cfg.Level)
	zerolog.SetGlobalLevel(level)

	writers := []io.Writer{
		LevelSplitWriter{Dev: cfg.Dev},
	}

	var closeFn func() error

	if cfg.File != "" {
		sink, err := openFileSink(cfg.File, cfg.Rotate)
		if err != nil {
			return Result{}, err
		}

		if cfg.Async {
			afw, err := NewAsyncWriter(sink.w, &sinkCloser{sink.close}, cfg.BufferSize)
			if err != nil {
				return Result{}, err
			}
			writers = append(writers, afw)
			closeFn = func() error { return afw.Close() }
		} else {
			writers = append(writers, sink.w)
			closeFn = sink.close
		}
	}

	multi := zerolog.MultiLevelWriter(writers...)

	l := zerolog.New(multi).With().
		Timestamp().
		Str("service", cfg.Service).
		Logger()

	log.Logger = l

	return Result{Logger: l, close: closeFn}, nil
}

type sinkCloser struct {
	fn func() error
}

func (c *sinkCloser) Close() error { return c.fn() }

func parseLevel(s string) zerolog.Level {
	switch s {
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}

// WithRequestID returns a child logger with request_id field (architecture §9).
func WithRequestID(l zerolog.Logger, requestID string) zerolog.Logger {
	return l.With().Str("request_id", requestID).Logger()
}
