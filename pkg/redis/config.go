package redis

import "time"

// Config 连接与超时；零值字段在 NewClient 中补默认。
type Config struct {
	Addr         string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c Config) withDefaults() Config {
	out := c
	if out.Addr == "" {
		out.Addr = "localhost:6379"
	}
	if out.DialTimeout <= 0 {
		out.DialTimeout = 3 * time.Second
	}
	if out.ReadTimeout <= 0 {
		out.ReadTimeout = 3 * time.Second
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = 3 * time.Second
	}
	return out
}
