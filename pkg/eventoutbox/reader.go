package eventoutbox

import (
	"fmt"
	"io"
	"os"
)

// Reader 顺序扫描 Event Outbox segment。
type Reader struct {
	paths   []string
	pathIdx int
	file    *os.File
}

// OpenReader 打开目录下全部 segment。
func OpenReader(dir string) (*Reader, error) {
	paths, err := SegmentPaths(dir)
	if err != nil {
		return nil, err
	}
	return &Reader{paths: paths}, nil
}

// ReadNext 读取下一条；EOF 表示当前无更多记录。
func (r *Reader) ReadNext() (Record, error) {
	for {
		if r.file == nil {
			if r.pathIdx >= len(r.paths) {
				return Record{}, io.EOF
			}
			f, err := os.Open(r.paths[r.pathIdx])
			if err != nil {
				return Record{}, fmt.Errorf("eventoutbox: open segment: %w", err)
			}
			r.file = f
			r.pathIdx++
		}
		rec, err := ReadRecord(r.file)
		if err == nil {
			return rec, nil
		}
		if !isEOF(err) {
			return Record{}, err
		}
		_ = r.file.Close()
		r.file = nil
	}
}

func isEOF(err error) bool {
	return err == io.EOF
}

// FetchUnpublished 返回 seq > afterSeq 的最多 limit 条记录（全目录扫描）。
func FetchUnpublished(dir string, afterSeq uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = 256
	}
	r, err := OpenReader(dir)
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, limit)
	for len(out) < limit {
		rec, err := r.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if rec.OutboxSeq <= afterSeq {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}
