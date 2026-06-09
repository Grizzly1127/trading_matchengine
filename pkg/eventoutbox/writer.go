package eventoutbox

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
	segmentFilePrefix      = "outbox_"
	segmentFileSuffix      = ".log"
)

var (
	ErrWriterClosed = errors.New("eventoutbox: writer closed")
	ErrNonMonotonic = errors.New("eventoutbox: non-monotonic sequence")
	ErrEmptyDir     = errors.New("eventoutbox: directory is required")
)

// FileWriterConfig 配置 Event Outbox 写入。
type FileWriterConfig struct {
	MaxSegmentBytes  int64
	WriteBufferSize  int
	SyncEveryRecords int
	SyncIntervalMs   int
}

func (c FileWriterConfig) GroupCommitEnabled() bool {
	return c.SyncEveryRecords > 1 || c.SyncIntervalMs > 0
}

// FileWriter 顺序追加 Event Outbox。
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

// OpenFileWriter 打开或创建 outbox 目录。
func OpenFileWriter(dir string, cfg FileWriterConfig) (*FileWriter, error) {
	if dir == "" {
		return nil, ErrEmptyDir
	}
	bufSize := cfg.WriteBufferSize
	if bufSize <= 0 {
		bufSize = defaultWriteBufferSize
	}
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if cfg.SyncEveryRecords <= 0 {
		cfg.SyncEveryRecords = 1
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("eventoutbox: mkdir %q: %w", dir, err)
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
		return nil, fmt.Errorf("eventoutbox: seek end: %w", err)
	}
	if st, err := w.file.Stat(); err == nil {
		w.written = st.Size()
	}
	return w, nil
}

func (w *FileWriter) GroupCommitEnabled() bool {
	return w.cfg.GroupCommitEnabled()
}

// Append 写入一条记录；非组提交模式下 append 后自动 sync。
func (w *FileWriter) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(rec)
}

// AppendNext 分配递增 outbox_seq 并写入。
func (w *FileWriter) AppendNext(rec Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWriterClosed
	}
	seq := w.lastSeq + 1
	rec.OutboxSeq = seq
	if err := w.appendLocked(rec); err != nil {
		return 0, err
	}
	return seq, nil
}

func (w *FileWriter) appendLocked(rec Record) error {
	if w.closed {
		return ErrWriterClosed
	}
	if rec.OutboxSeq == 0 {
		rec.OutboxSeq = w.lastSeq + 1
	}
	if w.lastSeq != 0 && rec.OutboxSeq <= w.lastSeq {
		return fmt.Errorf("%w: got %d last %d", ErrNonMonotonic, rec.OutboxSeq, w.lastSeq)
	}

	frameLen := recordLenSize + rec.bodyLen()
	frame := make([]byte, frameLen)
	if err := rec.encodeInto(frame); err != nil {
		return err
	}
	if _, err := w.buf.Write(frame); err != nil {
		return fmt.Errorf("eventoutbox: write buffer: %w", err)
	}

	w.lastSeq = rec.OutboxSeq
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

// Sync 强制 durable。
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
			return fmt.Errorf("eventoutbox: flush buffer: %w", err)
		}
	}
	return durableSync(w.file)
}

// Close 关闭 writer。
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

// LastSeq 已分配最大 seq。
func (w *FileWriter) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

// LastDurableSeq 已 fsync 的最大 seq。
func (w *FileWriter) LastDurableSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastDurableSeq
}

// Dir 返回存储目录。
func (w *FileWriter) Dir() string {
	return w.dir
}

func (w *FileWriter) latestSegment() (int, error) {
	matches, err := filepath.Glob(filepath.Join(w.dir, segmentFilePrefix+"*"+segmentFileSuffix))
	if err != nil {
		return 0, fmt.Errorf("eventoutbox: glob segments: %w", err)
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

func (w *FileWriter) openSegment(segment int) error {
	if w.file != nil {
		if err := w.maybeSyncLocked(true); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("eventoutbox: close segment: %w", err)
		}
		w.buf = nil
	}
	path := filepath.Join(w.dir, fmt.Sprintf("%s%06d%s", segmentFilePrefix, segment, segmentFileSuffix))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("eventoutbox: open %q: %w", path, err)
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, w.writeBufferSize)
	w.path = path
	w.segment = segment
	w.written = 0
	return nil
}

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

func scanLastSeq(r io.ReadSeeker) (uint64, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("eventoutbox: seek start: %w", err)
	}
	var last uint64
	for {
		rec, err := ReadRecord(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("eventoutbox: scan segment: %w", err)
		}
		last = rec.OutboxSeq
	}
	return last, nil
}

// SegmentPaths 返回排序后的 segment 路径。
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
