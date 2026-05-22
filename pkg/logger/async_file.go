package logger

import (
	"fmt"
	"io"
	"sync"
)

const defaultAsyncBuffer = 512

// AsyncFileWriter 在后台 goroutine 中写入目标 Writer（如 lumberjack）。
type AsyncFileWriter struct {
	ch     chan []byte
	dest   io.Writer
	closer io.Closer
	wg     sync.WaitGroup
	closed chan struct{}
}

// NewAsyncWriter 创建异步写入器。
func NewAsyncWriter(dest io.Writer, closer io.Closer, bufferSize int) (*AsyncFileWriter, error) {
	if dest == nil {
		return nil, fmt.Errorf("logger: async writer destination is nil")
	}
	if bufferSize <= 0 {
		bufferSize = defaultAsyncBuffer
	}

	w := &AsyncFileWriter{
		ch:     make(chan []byte, bufferSize),
		dest:   dest,
		closer: closer,
		closed: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.loop()
	return w, nil
}

func (w *AsyncFileWriter) loop() {
	defer w.wg.Done()
	for payload := range w.ch {
		_, _ = w.dest.Write(payload)
	}
}

// Write 将日志行放入队列，由后台线程写入。
func (w *AsyncFileWriter) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	copy(buf, p)

	select {
	case <-w.closed:
		return 0, fmt.Errorf("logger: async writer closed")
	case w.ch <- buf:
		return len(p), nil
	}
}

// Close 刷完队列后关闭底层写入器。
func (w *AsyncFileWriter) Close() error {
	select {
	case <-w.closed:
		return nil
	default:
		close(w.closed)
		close(w.ch)
	}
	w.wg.Wait()
	if w.closer != nil {
		return w.closer.Close()
	}
	return nil
}
