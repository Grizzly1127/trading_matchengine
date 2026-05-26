package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/wal"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type outputRecord struct {
	SeqID     uint64 `json:"seq_id"`
	Time      string `json:"time"`
	EventType string `json:"event_type"`

	// 解析 payload 后的关键信息（便于 grep / 人眼查看）
	Symbol         string `json:"symbol,omitempty"`
	OrderID        uint64 `json:"order_id,omitempty"`
	CommandID      uint64 `json:"command_id,omitempty"`
	KafkaPartition uint32 `json:"kafka_partition,omitempty"`
	KafkaOffset    uint64 `json:"kafka_offset,omitempty"`

	// 需要时可输出完整 proto（JSON）
	ProtoJSON json.RawMessage `json:"proto_json,omitempty"`
}

func main() {
	var (
		walDir  = flag.String("dir", "", "WAL shard 目录，例如 data/wal/shard-0")
		fromSeq = flag.Uint64("from", 0, "只输出 seq > from 的记录")
		full    = flag.Bool("full", false, "输出完整 proto_json（体积会变大）")
	)
	flag.Parse()

	if *walDir == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing -dir")
		flag.Usage()
		os.Exit(2)
	}

	r, err := wal.OpenFileReader(*walDir)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "open wal reader: %v\n", err)
		os.Exit(1)
	}

	it, err := r.ReadFrom(*fromSeq)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "read wal: %v\n", err)
		os.Exit(1)
	}
	defer it.Close()

	protoM := protojson.MarshalOptions{
		UseProtoNames:   true,  // 便于和 proto 字段对齐
		EmitUnpopulated: false, // 避免输出大量空字段
	}

	enc := json.NewEncoder(os.Stdout)
	for it.Next() {
		rec := it.Record()
		out := outputRecord{
			SeqID:     rec.SeqID,
			Time:      time.Unix(0, rec.Timestamp).UTC().Format(time.RFC3339Nano),
			EventType: eventTypeString(rec.EventType),
		}

		switch rec.EventType {
		case wal.EventTypeNewOrder:
			cmd := &matchingv1.NewOrderCommand{}
			if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "seq=%d unmarshal NewOrderCommand: %v\n", rec.SeqID, err)
				os.Exit(1)
			}
			if o := cmd.GetOrder(); o != nil {
				out.Symbol = o.GetSymbol()
				out.OrderID = o.GetOrderId()
			}
			out.CommandID = cmd.GetCommandId()
			out.KafkaPartition = cmd.GetKafkaPartition()
			out.KafkaOffset = cmd.GetKafkaOffset()
			if *full {
				b, err := protoM.Marshal(cmd)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "seq=%d marshal proto json: %v\n", rec.SeqID, err)
					os.Exit(1)
				}
				out.ProtoJSON = b
			}

		case wal.EventTypeCancelOrder:
			cmd := &matchingv1.CancelOrderCommand{}
			if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "seq=%d unmarshal CancelOrderCommand: %v\n", rec.SeqID, err)
				os.Exit(1)
			}
			out.Symbol = cmd.GetSymbol()
			out.OrderID = cmd.GetOrderId()
			out.CommandID = cmd.GetCommandId()
			out.KafkaPartition = cmd.GetKafkaPartition()
			out.KafkaOffset = cmd.GetKafkaOffset()
			if *full {
				b, err := protoM.Marshal(cmd)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "seq=%d marshal proto json: %v\n", rec.SeqID, err)
					os.Exit(1)
				}
				out.ProtoJSON = b
			}

		default:
			// 未知类型：只输出基础字段，避免误解析
		}

		if err := enc.Encode(out); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "encode json: %v\n", err)
			os.Exit(1)
		}
	}

	if err := it.Err(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "iterate wal: %v\n", err)
		os.Exit(1)
	}
}

func eventTypeString(t byte) string {
	switch t {
	case wal.EventTypeNewOrder:
		return "NEW_ORDER"
	case wal.EventTypeCancelOrder:
		return "CANCEL_ORDER"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

