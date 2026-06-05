package wal

import (
	"bufio"
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
	defaultWriteBufferSize = 64 * 1024
	segmentFilePrefix      = "wal_"
	segmentFileSuffix      = ".log"
)

var (
	ErrWriterClosed  = errors.New("wal: writer closed")
	ErrNonMonotonic  = errors.New("wal: non-monotonic sequence")
	ErrEmptyShardDir = errors.New("wal: shard directory is required")
)

// Writer 将 WAL 记录顺序追加到本地磁盘。
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
	// WriteBufferSize 用户态写缓冲；0 表示默认 64KB。
	WriteBufferSize int
	// SyncEveryRecords 每追加 N 条后自动 fdatasync；<=1 表示每条都 sync（默认行为）。
	SyncEveryRecords int
	// SyncIntervalMs 距上次 sync 超过该毫秒也触发 sync（仅当 SyncEveryRecords>1 时在 Sync() 外生效）；0 禁用。
	SyncIntervalMs int
}

// GroupCommitEnabled 是否启用组提交（多条 append 后才 fsync）。
func (c FileWriterConfig) GroupCommitEnabled() bool {
	return c.SyncEveryRecords > 1 || c.SyncIntervalMs > 0
}

// FileWriter 将 WAL 写入 data/wal/{shard_id}/wal_NNNNNN.log。
type FileWriter struct {
	dir              string
	cfg              FileWriterConfig
	mu               sync.Mutex
	file             *os.File
	buf              *bufio.Writer
	path             string
	segment          int
	lastSeq          uint64
	lastDurableSeq   uint64
	written          int64
	closed           bool
	writeBufferSize  int
	recordsSinceSync int
	lastSyncAt       time.Time
}

// OpenFileWriter 打开或创建 shard WAL 目录，并追加写入最新 segment。
func OpenFileWriter(dir string, cfg FileWriterConfig) (*FileWriter, error) {
	if dir == "" {
		return nil, ErrEmptyShardDir
	}
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	bufSize := cfg.WriteBufferSize
	if bufSize <= 0 {
		bufSize = defaultWriteBufferSize
	}
	if cfg.SyncEveryRecords <= 0 {
		cfg.SyncEveryRecords = 1
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}

	w := &FileWriter{
		dir:             dir,
		cfg:             cfg,
		writeBufferSize: bufSize,
		lastSyncAt:      time.Now(),
	}
	segment, err := w.latestSegment()
	if err != nil {
		return nil, err
	}
	if err := w.openSegment(segment); err != nil {
		return nil, err
	}
	lastSeq, err := scanLastSeq(w.file)
	if err != nil {
		_ = w.closeFileLocked()
		return nil, err
	}
	w.lastSeq = lastSeq
	w.lastDurableSeq = lastSeq
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		_ = w.closeFileLocked()
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}
	if st, err := w.file.Stat(); err == nil {
		w.written = st.Size()
	}
	return w, nil
}

// GroupCommitEnabled 同 FileWriterConfig.GroupCommitEnabled。
func (w *FileWriter) GroupCommitEnabled() bool {
	return w.cfg.GroupCommitEnabled()
}

// Append 编码并写入一条记录；SyncEveryRecords<=1 时立即 durable sync。
func (w *FileWriter) Append(seq uint64, eventType byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(seq, eventType, payload)
}

// AppendNext 分配递增 seq 并追加。
func (w *FileWriter) AppendNext(eventType byte, payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWriterClosed
	}
	seq := w.lastSeq + 1
	if err := w.appendLocked(seq, eventType, payload); err != nil {
		return 0, err
	}
	return seq, nil
}

func (w *FileWriter) appendLocked(seq uint64, eventType byte, payload []byte) error {
	if w.closed {
		return ErrWriterClosed
	}
	if w.lastSeq != 0 && seq <= w.lastSeq {
		return fmt.Errorf("%w: got %d last %d", ErrNonMonotonic, seq, w.lastSeq)
	}

	rec := NewRecord(seq, eventType, payload, time.Time{})
	frameLen := recordLenSize + rec.BodyLen()
	frame := acquireFrame(frameLen)
	if err := rec.encodeInto(frame); err != nil {
		releaseFrame(frame)
		return err
	}

	if _, err := w.buf.Write(frame); err != nil {
		releaseFrame(frame)
		return fmt.Errorf("wal: write buffer: %w", err)
	}
	releaseFrame(frame)

	w.lastSeq = seq
	w.written += int64(frameLen)
	w.recordsSinceSync++

	if err := w.maybeSyncLocked(false); err != nil {
		return err
	}

	if w.written >= w.cfg.MaxSegmentBytes {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 强制刷缓冲并 fdatasync，使 lastSeq 成为 durable。
func (w *FileWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maybeSyncLocked(true)
}

func (w *FileWriter) maybeSyncLocked(force bool) error {
	if w.recordsSinceSync == 0 && !force {
		return nil
	}
	if !force && w.cfg.GroupCommitEnabled() {
		// 组提交：仅由显式 Sync() 落盘，不在 append 时自动 sync。
		return nil
	}
	if !force && w.cfg.SyncEveryRecords > 1 {
		if w.recordsSinceSync < w.cfg.SyncEveryRecords {
			if w.cfg.SyncIntervalMs <= 0 || time.Since(w.lastSyncAt) < time.Duration(w.cfg.SyncIntervalMs)*time.Millisecond {
				return nil
			}
		}
	}
	if err := w.flushAndSyncLocked(); err != nil {
		return err
	}
	w.lastDurableSeq = w.lastSeq
	w.recordsSinceSync = 0
	w.lastSyncAt = time.Now()
	return nil
}

func (w *FileWriter) flushAndSyncLocked() error {
	if w.buf != nil {
		if err := w.buf.Flush(); err != nil {
			return fmt.Errorf("wal: flush buffer: %w", err)
		}
	}
	if err := durableSync(w.file); err != nil {
		return err
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
	return w.closeFileLocked()
}

func (w *FileWriter) closeFileLocked() error {
	if w.file == nil {
		return nil
	}
	if err := w.maybeSyncLocked(true); err != nil {
		_ = w.file.Close()
		w.file = nil
		w.buf = nil
		return err
	}
	err := w.file.Close()
	w.file = nil
	w.buf = nil
	return err
}

// LastSeq 返回已分配的最大 seq（含尚未 sync 的缓冲记录）。
func (w *FileWriter) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

// LastDurableSeq 返回已 fdatasync 的最大 seq。
func (w *FileWriter) LastDurableSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastDurableSeq
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
		if err := w.maybeSyncLocked(true); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("wal: close segment: %w", err)
		}
		w.buf = nil
	}

	path := filepath.Join(w.dir, fmt.Sprintf("%s%06d%s", segmentFilePrefix, segment, segmentFileSuffix))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open %q: %w", path, err)
	}

	w.file = f
	w.buf = bufio.NewWriterSize(f, w.writeBufferSize)
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
