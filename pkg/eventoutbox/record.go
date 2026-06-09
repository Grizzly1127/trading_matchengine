package eventoutbox

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// Topic 标识 Kafka 目标 topic。
const (
	TopicMatch byte = 1
	TopicTrade byte = 2
)

const (
	recordLenSize     = 4
	recordSeqSize     = 8
	recordWalSeqSize  = 8
	recordTimeSize    = 8
	recordTopicSize   = 1
	recordPartSize    = 4
	recordOffsetSize  = 8
	recordKeyLenSize  = 2
	recordCRCSize     = 4
	recordHeaderFixed = recordSeqSize + recordWalSeqSize + recordTimeSize + recordTopicSize +
		recordPartSize + recordOffsetSize + recordKeyLenSize
	recordMinBodyLen = recordHeaderFixed + recordCRCSize
	recordMaxBodyLen = 16 * 1024 * 1024
)

var (
	ErrRecordTooShort = errors.New("eventoutbox: record buffer too short")
	ErrInvalidLength  = errors.New("eventoutbox: invalid record length")
	ErrCRCMismatch    = errors.New("eventoutbox: crc mismatch")
)

// Record 本地 Event Outbox 一条待投递事件。
type Record struct {
	OutboxSeq      uint64
	WalSeq         uint64
	Timestamp      int64
	TopicID        byte
	KafkaPartition uint32
	KafkaOffset    uint64
	PartitionKey   string
	Payload        []byte
}

// NewRecord 构造记录。
func NewRecord(outboxSeq, walSeq uint64, topicID byte, kafkaPartition uint32, kafkaOffset uint64, key string, payload []byte, ts time.Time) Record {
	if ts.IsZero() {
		ts = time.Now()
	}
	return Record{
		OutboxSeq:      outboxSeq,
		WalSeq:         walSeq,
		Timestamp:      ts.UnixNano(),
		TopicID:        topicID,
		KafkaPartition: kafkaPartition,
		KafkaOffset:    kafkaOffset,
		PartitionKey:   key,
		Payload:        payload,
	}
}

func (r Record) bodyLen() int {
	return recordHeaderFixed + len(r.PartitionKey) + len(r.Payload) + recordCRCSize
}

func (r Record) encodeInto(frame []byte) error {
	bodyLen := r.bodyLen()
	if bodyLen > recordMaxBodyLen {
		return fmt.Errorf("eventoutbox: record body too large: %d", bodyLen)
	}
	want := recordLenSize + bodyLen
	if len(frame) < want {
		return fmt.Errorf("eventoutbox: frame buffer too short: %d < %d", len(frame), want)
	}

	binary.LittleEndian.PutUint32(frame[0:recordLenSize], uint32(bodyLen))
	off := recordLenSize
	binary.LittleEndian.PutUint64(frame[off:off+recordSeqSize], r.OutboxSeq)
	off += recordSeqSize
	binary.LittleEndian.PutUint64(frame[off:off+recordWalSeqSize], r.WalSeq)
	off += recordWalSeqSize
	binary.LittleEndian.PutUint64(frame[off:off+recordTimeSize], uint64(r.Timestamp))
	off += recordTimeSize
	frame[off] = r.TopicID
	off += recordTopicSize
	binary.LittleEndian.PutUint32(frame[off:off+recordPartSize], r.KafkaPartition)
	off += recordPartSize
	binary.LittleEndian.PutUint64(frame[off:off+recordOffsetSize], r.KafkaOffset)
	off += recordOffsetSize
	binary.LittleEndian.PutUint16(frame[off:off+recordKeyLenSize], uint16(len(r.PartitionKey)))
	off += recordKeyLenSize
	copy(frame[off:off+len(r.PartitionKey)], r.PartitionKey)
	off += len(r.PartitionKey)
	copy(frame[off:off+len(r.Payload)], r.Payload)
	off += len(r.Payload)

	crc := crc32.ChecksumIEEE(frame[recordLenSize:off])
	binary.LittleEndian.PutUint32(frame[off:off+recordCRCSize], crc)
	return nil
}

// Encode 序列化为完整帧。
func (r Record) Encode() ([]byte, error) {
	frame := make([]byte, recordLenSize+r.bodyLen())
	if err := r.encodeInto(frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// DecodeRecord 从完整帧解析。
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
	return decodeBody(frame[recordLenSize : recordLenSize+bodyLen])
}

// ReadRecord 从 r 读取一条记录。
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

func decodeBody(body []byte) (Record, error) {
	if len(body) < recordMinBodyLen {
		return Record{}, ErrRecordTooShort
	}
	off := 0
	outboxSeq := binary.LittleEndian.Uint64(body[off : off+recordSeqSize])
	off += recordSeqSize
	walSeq := binary.LittleEndian.Uint64(body[off : off+recordWalSeqSize])
	off += recordWalSeqSize
	ts := int64(binary.LittleEndian.Uint64(body[off : off+recordTimeSize]))
	off += recordTimeSize
	topicID := body[off]
	off += recordTopicSize
	part := binary.LittleEndian.Uint32(body[off : off+recordPartSize])
	off += recordPartSize
	kafkaOff := binary.LittleEndian.Uint64(body[off : off+recordOffsetSize])
	off += recordOffsetSize
	keyLen := int(binary.LittleEndian.Uint16(body[off : off+recordKeyLenSize]))
	off += recordKeyLenSize
	if off+keyLen+recordCRCSize > len(body) {
		return Record{}, ErrRecordTooShort
	}
	key := string(body[off : off+keyLen])
	off += keyLen
	payloadEnd := len(body) - recordCRCSize
	if off > payloadEnd {
		return Record{}, ErrRecordTooShort
	}
	payload := make([]byte, payloadEnd-off)
	copy(payload, body[off:payloadEnd])
	off = payloadEnd

	storedCRC := binary.LittleEndian.Uint32(body[off : off+recordCRCSize])
	computedCRC := crc32.ChecksumIEEE(body[:off])
	if storedCRC != computedCRC {
		return Record{}, ErrCRCMismatch
	}

	return Record{
		OutboxSeq:      outboxSeq,
		WalSeq:         walSeq,
		Timestamp:      ts,
		TopicID:        topicID,
		KafkaPartition: part,
		KafkaOffset:    kafkaOff,
		PartitionKey:   key,
		Payload:        payload,
	}, nil
}
