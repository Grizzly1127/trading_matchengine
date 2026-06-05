package publisher

import (
	"sync"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

// proto 序列化 scratch 复用，减少 MarshalAppend 时的扩容分配。
var marshalScratchPool sync.Pool

func marshalProto(m proto.Message) ([]byte, error) {
	scratch, _ := marshalScratchPool.Get().([]byte)
	scratch = scratch[:0]
	b, err := proto.MarshalOptions{}.MarshalAppend(scratch, m)
	if err != nil {
		marshalScratchPool.Put(scratch[:0])
		return nil, err
	}
	// Kafka 持有各条 value 直至发送完成，须独立拷贝（与 proto.Marshal 相同）。
	out := make([]byte, len(b))
	copy(out, b)
	marshalScratchPool.Put(scratch[:0])
	return out, nil
}

func marshalMatchBatch(events []*matchingv1.MatchEvent) ([][]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}
	vals := make([][]byte, len(events))
	for i, ev := range events {
		b, err := marshalProto(ev)
		if err != nil {
			return nil, err
		}
		vals[i] = b
	}
	return vals, nil
}

func marshalTradeBatch(events []*matchingv1.TradeEvent) ([][]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}
	vals := make([][]byte, len(events))
	for i, ev := range events {
		b, err := marshalProto(ev)
		if err != nil {
			return nil, err
		}
		vals[i] = b
	}
	return vals, nil
}
