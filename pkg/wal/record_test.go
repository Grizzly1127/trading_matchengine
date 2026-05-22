package wal

import (
	"bytes"
	"testing"
	"time"
)

func TestRecord_EncodeDecode_roundTrip(t *testing.T) {
	ts := time.Unix(1_700_000_000, 123).UTC()
	want := NewRecord(42, EventTypeNewOrder, []byte("hello-wal"), ts)

	frame, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}

	got, err := DecodeRecord(frame)
	if err != nil {
		t.Fatal(err)
	}
	if got.SeqID != want.SeqID || got.Timestamp != want.Timestamp ||
		got.EventType != want.EventType || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("got = %+v, want = %+v", got, want)
	}
}

func TestRecord_EncodeDecode_emptyPayload(t *testing.T) {
	want := NewRecord(1, EventTypeCancelOrder, nil, time.Unix(0, 0))

	frame, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRecord(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("payload = %q, want empty", got.Payload)
	}
}

func TestDecodeRecord_crcMismatch(t *testing.T) {
	frame, err := NewRecord(1, EventTypeNewOrder, []byte("x"), time.Unix(0, 0)).Encode()
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff

	if _, err := DecodeRecord(frame); err != ErrCRCMismatch {
		t.Fatalf("err = %v, want %v", err, ErrCRCMismatch)
	}
}

func TestDecodeRecord_invalidLength(t *testing.T) {
	frame := make([]byte, recordLenSize+recordMinBodyLen)
	binaryPutU32(frame[0:recordLenSize], uint32(recordMinBodyLen-1))

	if _, err := DecodeRecord(frame); err != ErrInvalidLength {
		t.Fatalf("err = %v, want %v", err, ErrInvalidLength)
	}
}

func TestReadRecord_fromReader(t *testing.T) {
	want := NewRecord(7, EventTypeNewOrder, []byte("payload"), time.Unix(100, 0))
	frame, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}

	got, err := ReadRecord(bytes.NewReader(frame))
	if err != nil {
		t.Fatal(err)
	}
	if got.SeqID != want.SeqID || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("got = %+v, want = %+v", got, want)
	}
}

func binaryPutU32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
