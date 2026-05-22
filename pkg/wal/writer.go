package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaxSegmentBytes = 100 * 1024 * 1024
	segmentFilePrefix      = "wal_"
	segmentFileSuffix      = ".log"
)

var (
	ErrWriterClosed  = errors.New("wal: writer closed")
	ErrNonMonotonic  = errors.New("wal: non-monotonic sequence")
	ErrEmptyShardDir = errors.New("wal: shard directory is required")
)

// Writer 将 WAL 记录顺序追加到本地磁盘；每次 Append 后 fsync。
type Writer interface {
	// Append 追加一条 WAL 记录；seq 必须严格递增。
	Append(seq uint64, eventType byte, payload []byte) error
	// Close 关闭底层文件并释放资源。
	Close() error
}

// FileWriterConfig 配置 FileWriter。
type FileWriterConfig struct {
	// MaxSegmentBytes 单段 WAL 上限，超出后滚动新文件；0 表示默认 100MB。
	MaxSegmentBytes int64
}

// FileWriter 将 WAL 写入 data/wal/{shard_id}/wal_NNNNNN.log。
type FileWriter struct {
	dir     string
	cfg     FileWriterConfig
	mu      sync.Mutex
	file    *os.File
	path    string
	segment int
	lastSeq uint64
	written int64
	closed  bool
}

// OpenFileWriter 打开或创建 shard WAL 目录，并追加写入最新 segment。
func OpenFileWriter(dir string, cfg FileWriterConfig) (*FileWriter, error) {
	if dir == "" {
		return nil, ErrEmptyShardDir
	}
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}

	w := &FileWriter{dir: dir, cfg: cfg}
	segment, err := w.latestSegment()
	if err != nil {
		return nil, err
	}
	if err := w.openSegment(segment); err != nil {
		return nil, err
	}
	lastSeq, err := scanLastSeq(w.file)
	if err != nil {
		_ = w.file.Close()
		return nil, err
	}
	w.lastSeq = lastSeq
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		_ = w.file.Close()
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}
	if st, err := w.file.Stat(); err == nil {
		w.written = st.Size()
	}
	return w, nil
}

// Append 编码并写入一条记录，成功后 fsync。
func (w *FileWriter) Append(seq uint64, eventType byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWriterClosed
	}
	if w.lastSeq != 0 && seq <= w.lastSeq {
		return fmt.Errorf("%w: got %d last %d", ErrNonMonotonic, seq, w.lastSeq)
	}

	rec := NewRecord(seq, eventType, payload, time.Time{})
	frame, err := rec.Encode()
	if err != nil {
		return err
	}

	n, err := w.file.Write(frame)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if n != len(frame) {
		return fmt.Errorf("wal: short write: %d/%d", n, len(frame))
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}

	w.lastSeq = seq
	w.written += int64(len(frame))

	if w.written >= w.cfg.MaxSegmentBytes {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭当前 segment 文件。
func (w *FileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// LastSeq 返回已持久化的最大 seq（供上层恢复位点）。
func (w *FileWriter) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

// latestSegment 返回 shard 目录中编号最大的 segment；无文件时返回 1。
func (w *FileWriter) latestSegment() (int, error) {
	matches, err := filepath.Glob(filepath.Join(w.dir, segmentFilePrefix+"*"+segmentFileSuffix))
	if err != nil {
		return 0, fmt.Errorf("wal: glob segments: %w", err)
	}
	if len(matches) == 0 {
		return 1, nil
	}

	maxSeg := 0
	for _, path := range matches {
		seg, ok := parseSegmentName(filepath.Base(path))
		if !ok {
			continue
		}
		if seg > maxSeg {
			maxSeg = seg
		}
	}
	if maxSeg == 0 {
		return 1, nil
	}
	return maxSeg, nil
}

// parseSegmentName 从 wal_NNNNNN.log 文件名解析 segment 编号。
func parseSegmentName(name string) (int, bool) {
	if !strings.HasPrefix(name, segmentFilePrefix) || !strings.HasSuffix(name, segmentFileSuffix) {
		return 0, false
	}
	numStr := strings.TrimSuffix(strings.TrimPrefix(name, segmentFilePrefix), segmentFileSuffix)
	seg, err := strconv.Atoi(numStr)
	if err != nil || seg <= 0 {
		return 0, false
	}
	return seg, true
}

// openSegment 打开或创建指定编号的 segment 文件并切换写入目标。
func (w *FileWriter) openSegment(segment int) error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("wal: close segment: %w", err)
		}
	}

	path := filepath.Join(w.dir, fmt.Sprintf("%s%06d%s", segmentFilePrefix, segment, segmentFileSuffix))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open %q: %w", path, err)
	}

	w.file = f
	w.path = path
	w.segment = segment
	w.written = 0
	return nil
}

// rotateLocked 在当前 segment 写满时滚动到下一个文件；调用方需持有 w.mu。
func (w *FileWriter) rotateLocked() error {
	next := w.segment + 1
	if err := w.openSegment(next); err != nil {
		return err
	}
	if st, err := w.file.Stat(); err == nil {
		w.written = st.Size()
	}
	return nil
}

// scanLastSeq 扫描 segment 内全部记录并返回最大 seq，用于重启恢复写入位点。
func scanLastSeq(r io.ReadSeeker) (uint64, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("wal: seek start: %w", err)
	}

	var last uint64
	for {
		rec, err := ReadRecord(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("wal: scan segment: %w", err)
		}
		last = rec.SeqID
	}
	return last, nil
}

// SegmentPaths 返回 shard 目录下按 segment 编号排序的 WAL 文件路径。
func SegmentPaths(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, segmentFilePrefix+"*"+segmentFileSuffix))
	if err != nil {
		return nil, err
	}
	sort.Slice(matches, func(i, j int) bool {
		si, _ := parseSegmentName(filepath.Base(matches[i]))
		sj, _ := parseSegmentName(filepath.Base(matches[j]))
		return si < sj
	})
	return matches, nil
}
