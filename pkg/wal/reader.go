package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// Reader 从 WAL segment 顺序读取记录。
type Reader interface {
	// ReadFrom 返回 seq 严格大于 fromSeq 的记录迭代器。
	ReadFrom(fromSeq uint64) (*Iterator, error)
}

// FileReader 读取 data/wal/{shard_id}/ 下的 segment 文件。
type FileReader struct {
	dir string
}

// OpenFileReader 打开 shard WAL 目录供读取；目录不存在时视为空 WAL。
func OpenFileReader(dir string) (*FileReader, error) {
	if dir == "" {
		return nil, ErrEmptyShardDir
	}
	st, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &FileReader{dir: dir}, nil
		}
		return nil, fmt.Errorf("wal: stat %q: %w", dir, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("wal: %q is not a directory", dir)
	}
	return &FileReader{dir: dir}, nil
}

// ReadFrom 按 segment 顺序读取 seq > fromSeq 的记录。
func (r *FileReader) ReadFrom(fromSeq uint64) (*Iterator, error) {
	paths, err := SegmentPaths(r.dir)
	if err != nil {
		return nil, err
	}
	return newIterator(paths, fromSeq), nil
}

// ReadAll 读取 seq > fromSeq 的全部记录（便于 recovery 与测试）。
func (r *FileReader) ReadAll(fromSeq uint64) ([]Record, error) {
	it, err := r.ReadFrom(fromSeq)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var records []Record
	for it.Next() {
		records = append(records, it.Record())
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// Iterator 遍历 WAL 记录；用完应 Close。
type Iterator struct {
	paths   []string
	pathIdx int
	file    *os.File
	fromSeq uint64

	record Record
	err    error
	closed bool
}

// newIterator 构造跨 segment 顺序读取的迭代器。
func newIterator(paths []string, fromSeq uint64) *Iterator {
	return &Iterator{
		paths:   paths,
		fromSeq: fromSeq,
	}
}

// Next 前进到下一条 seq > fromSeq 的记录。
func (it *Iterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	for {
		if it.file == nil {
			if !it.openNextSegment() {
				return false
			}
		}

		rec, err := ReadRecord(it.file)
		if errors.Is(err, io.EOF) {
			it.closeSegment()
			continue
		}
		if err != nil {
			it.err = fmt.Errorf("wal: read segment %q: %w", it.paths[it.pathIdx-1], err)
			it.closeSegment()
			return false
		}
		if rec.SeqID <= it.fromSeq {
			continue
		}

		it.record = rec
		return true
	}
}

// Record 返回当前记录；仅在 Next 返回 true 后有效。
func (it *Iterator) Record() Record {
	return it.record
}

// Err 返回迭代过程中的首个错误。
func (it *Iterator) Err() error {
	return it.err
}

// Close 关闭迭代器持有的 segment 文件。
func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.closeSegment()
}

// openNextSegment 打开 paths 中的下一个 segment 文件。
func (it *Iterator) openNextSegment() bool {
	if it.pathIdx >= len(it.paths) {
		return false
	}
	path := it.paths[it.pathIdx]
	it.pathIdx++

	f, err := os.Open(path)
	if err != nil {
		it.err = fmt.Errorf("wal: open %q: %w", path, err)
		return false
	}
	it.file = f
	return true
}

// closeSegment 关闭当前打开的 segment 文件。
func (it *Iterator) closeSegment() error {
	if it.file == nil {
		return nil
	}
	err := it.file.Close()
	it.file = nil
	return err
}
