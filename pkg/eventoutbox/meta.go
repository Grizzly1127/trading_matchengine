package eventoutbox

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const metaFileName = "meta.json"

// Meta 持久化 relay 游标。
type Meta struct {
	LastPublishedSeq uint64 `json:"last_published_seq"`
}

// LoadMeta 读取 meta；不存在时返回零值。
func LoadMeta(dir string) (Meta, error) {
	path := filepath.Join(dir, metaFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Meta{}, nil
		}
		return Meta{}, fmt.Errorf("eventoutbox: read meta: %w", err)
	}
	var m Meta
	if err := json.Unmarshal(b, &m); err != nil {
		return Meta{}, fmt.Errorf("eventoutbox: parse meta: %w", err)
	}
	return m, nil
}

// SaveMeta 原子写入 meta 并 fsync。
func SaveMeta(dir string, m Meta) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("eventoutbox: mkdir meta: %w", err)
	}
	path := filepath.Join(dir, metaFileName)
	tmp := path + ".tmp"
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("eventoutbox: write meta tmp: %w", err)
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("eventoutbox: open meta tmp: %w", err)
	}
	if err := durableSync(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("eventoutbox: rename meta: %w", err)
	}
	return nil
}

// PendingCount 返回未发布条数（lastSeq - published）；只读估计。
func PendingCount(lastSeq, publishedSeq uint64) uint64 {
	if lastSeq <= publishedSeq {
		return 0
	}
	return lastSeq - publishedSeq
}

var metaMu sync.Mutex

// SaveMetaLocked 进程内串行化 meta 写入。
func SaveMetaLocked(dir string, m Meta) error {
	metaMu.Lock()
	defer metaMu.Unlock()
	return SaveMeta(dir, m)
}
