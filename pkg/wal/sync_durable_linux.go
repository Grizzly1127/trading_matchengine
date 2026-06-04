//go:build linux

package wal

import (
	"fmt"
	"os"
	"syscall"
)

// durableSync 仅刷数据页（append-only segment 足够）；比 fsync 少刷 inode 元数据。
func durableSync(f *os.File) error {
	if f == nil {
		return fmt.Errorf("wal: sync nil file")
	}
	if err := syscall.Fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("wal: fdatasync: %w", err)
	}
	return nil
}
