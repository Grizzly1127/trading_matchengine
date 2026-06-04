//go:build !linux

package wal

import (
	"fmt"
	"os"
)

func durableSync(f *os.File) error {
	if f == nil {
		return fmt.Errorf("wal: sync nil file")
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}
