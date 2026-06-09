//go:build linux

package eventoutbox

import (
	"fmt"
	"os"
	"syscall"
)

func durableSync(f *os.File) error {
	if f == nil {
		return fmt.Errorf("eventoutbox: sync nil file")
	}
	if err := syscall.Fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("eventoutbox: fdatasync: %w", err)
	}
	return nil
}
