package wal

import "sync"

// 复用 WAL 帧缓冲，减少 Append 热路径上的 make。
const maxPooledFrameCap = 64 * 1024

var framePool sync.Pool

func acquireFrame(size int) []byte {
	if size > maxPooledFrameCap {
		return make([]byte, size)
	}
	b, ok := framePool.Get().([]byte)
	if !ok || cap(b) < size {
		if ok {
			framePool.Put(b)
		}
		return make([]byte, size)
	}
	return b[:size]
}

func releaseFrame(b []byte) {
	if cap(b) == 0 || cap(b) > maxPooledFrameCap {
		return
	}
	framePool.Put(b[:0])
}
