package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// WAL 命令类型（payload 由上层 proto 序列化）。
const (
	EventTypeNewOrder    byte = 1
	EventTypeCancelOrder byte = 2
)

// 帧布局：[4B len][8B seq][8B ts][1B type][payload][4B crc32]
// len 为紧随其后的 body 字节数（含 seq、ts、type、payload、crc32）。
const (
	recordLenSize    = 4
	recordSeqSize    = 8
	recordTimeSize   = 8
	recordTypeSize   = 1
	recordCRCSize    = 4
	recordMinBodyLen = recordSeqSize + recordTimeSize + recordTypeSize + recordCRCSize
	recordMaxBodyLen = 16 * 1024 * 1024 // 单条 WAL 上限，防止损坏长度字段
)

var (
	ErrRecordTooShort = errors.New("wal: record buffer too short")
	ErrInvalidLength  = errors.New("wal: invalid record length")
	ErrCRCMismatch    = errors.New("wal: crc mismatch")
)

// Record 表示一条 WAL 日志记录。
type Record struct {
	SeqID     uint64
	Timestamp int64 // Unix 纳秒
	EventType byte
	Payload   []byte
}

// NewRecord 构造记录；ts 为零时使用当前时间。
func NewRecord(seqID uint64, eventType byte, payload []byte, ts time.Time) Record {
	if ts.IsZero() {
		ts = time.Now()
	}
	return Record{
		SeqID:     seqID,
		Timestamp: ts.UnixNano(),
		EventType: eventType,
		Payload:   payload,
	}
}

// BodyLen 返回不含 4 字节长度前缀的 body 长度。
func (r Record) BodyLen() int {
	return recordMinBodyLen + len(r.Payload)
}

// Encode 将记录序列化为完整帧（含长度前缀）。
func (r Record) Encode() ([]byte, error) {
	bodyLen := r.BodyLen()
	if bodyLen > recordMaxBodyLen {
		return nil, fmt.Errorf("wal: record body too large: %d", bodyLen)
	}

	frame := make([]byte, recordLenSize+bodyLen)
	binary.LittleEndian.PutUint32(frame[0:recordLenSize], uint32(bodyLen))

	off := recordLenSize
	binary.LittleEndian.PutUint64(frame[off:off+recordSeqSize], r.SeqID)
	off += recordSeqSize
	binary.LittleEndian.PutUint64(frame[off:off+recordTimeSize], uint64(r.Timestamp))
	off += recordTimeSize
	frame[off] = r.EventType
	off += recordTypeSize
	copy(frame[off:off+len(r.Payload)], r.Payload)
	off += len(r.Payload)

	crc := crc32.ChecksumIEEE(frame[recordLenSize:off])
	binary.LittleEndian.PutUint32(frame[off:off+recordCRCSize], crc)

	return frame, nil
}

// DecodeRecord 从完整帧（含长度前缀）解析一条记录。
func DecodeRecord(frame []byte) (Record, error) {
	if len(frame) < recordLenSize+recordMinBodyLen {
		return Record{}, ErrRecordTooShort
	}

	bodyLen := int(binary.LittleEndian.Uint32(frame[0:recordLenSize]))
	if bodyLen < recordMinBodyLen || bodyLen > recordMaxBodyLen {
		return Record{}, ErrInvalidLength
	}
	if len(frame) < recordLenSize+bodyLen {
		return Record{}, ErrRecordTooShort
	}

	body := frame[recordLenSize : recordLenSize+bodyLen]
	return decodeBody(body)
}

// ReadRecord 从 r 读取一条带长度前缀的记录。
func ReadRecord(r io.Reader) (Record, error) {
	var lenBuf [recordLenSize]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Record{}, err
	}

	bodyLen := int(binary.LittleEndian.Uint32(lenBuf[:]))
	if bodyLen < recordMinBodyLen || bodyLen > recordMaxBodyLen {
		return Record{}, ErrInvalidLength
	}

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return Record{}, err
	}
	return decodeBody(body)
}

// decodeBody 解析单条 WAL body 并校验 CRC32。
func decodeBody(body []byte) (Record, error) {
	if len(body) < recordMinBodyLen {
		return Record{}, ErrRecordTooShort
	}

	off := 0
	seq := binary.LittleEndian.Uint64(body[off : off+recordSeqSize])
	off += recordSeqSize
	ts := int64(binary.LittleEndian.Uint64(body[off : off+recordTimeSize]))
	off += recordTimeSize
	eventType := body[off]
	off += recordTypeSize

	payloadLen := len(body) - recordMinBodyLen
	payload := make([]byte, payloadLen)
	copy(payload, body[off:off+payloadLen])
	off += payloadLen

	storedCRC := binary.LittleEndian.Uint32(body[off : off+recordCRCSize])
	computedCRC := crc32.ChecksumIEEE(body[:off])
	if storedCRC != computedCRC {
		return Record{}, ErrCRCMismatch
	}

	return Record{
		SeqID:     seq,
		Timestamp: ts,
		EventType: eventType,
		Payload:   payload,
	}, nil
}
