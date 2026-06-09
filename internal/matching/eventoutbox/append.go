package eventoutbox

import (
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/eventoutbox"
)

// CommandMeta 单条 order.commands 的 Kafka 位点（与 outbound 对应）。
type CommandMeta struct {
	KafkaPartition uint32
	KafkaOffset    uint64
}

// AppendOutbound 将一批 outbound 写入 Event Outbox（未 Sync）。
func AppendOutbound(w *eventoutbox.FileWriter, meta CommandMeta, out publisher.Outbound) error {
	if w == nil {
		return fmt.Errorf("eventoutbox: writer is nil")
	}
	for _, ev := range out.MatchEvents {
		payload, err := marshalMatch(ev)
		if err != nil {
			return err
		}
		rec := eventoutbox.NewRecord(
			0,
			ev.GetWalSeq(),
			eventoutbox.TopicMatch,
			meta.KafkaPartition,
			meta.KafkaOffset,
			ev.GetSymbol(),
			payload,
			time.Time{},
		)
		if _, err := w.AppendNext(rec); err != nil {
			return err
		}
	}
	for _, ev := range out.TradeEvents {
		payload, err := marshalTrade(ev)
		if err != nil {
			return err
		}
		sym := ""
		if tr := ev.GetTrade(); tr != nil {
			sym = tr.GetSymbol()
		}
		rec := eventoutbox.NewRecord(
			0,
			ev.GetWalSeq(),
			eventoutbox.TopicTrade,
			meta.KafkaPartition,
			meta.KafkaOffset,
			sym,
			payload,
			time.Time{},
		)
		if _, err := w.AppendNext(rec); err != nil {
			return err
		}
	}
	return nil
}

func marshalMatch(ev *matchingv1.MatchEvent) ([]byte, error) {
	return publisher.MarshalMatchEvent(ev)
}

func marshalTrade(ev *matchingv1.TradeEvent) ([]byte, error) {
	return publisher.MarshalTradeEvent(ev)
}
