//go:build !linux

package eventoutbox

import (
	"fmt"
	"os"
)

func durableSync(f *os.File) error {
	if f == nil {
		return fmt.Errorf("eventoutbox: sync nil file")
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("eventoutbox: fsync: %w", err)
	}
	return nil
}
