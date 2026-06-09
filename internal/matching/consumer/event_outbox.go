package consumer

import (
	"fmt"
	"time"

	matcheventoutbox "github.com/Grizzly1127/trading_matchengine/internal/matching/eventoutbox"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/eventoutbox"
)

// persistOutboundBatch 将本批事件写入 Event Outbox 并 fsync（I2：commit offset 前必须 durable）。
func (h *Handler) persistOutboundBatch(staged []*stagedMessage, outcomes []recovery.CommandOutcome) (time.Duration, error) {
	if h.EventOutbox == nil {
		return 0, fmt.Errorf("consumer: event outbox writer is nil")
	}
	for i, sc := range staged {
		o := outcomes[i]
		if o.ReadOnly || o.Duplicate {
			continue
		}
		meta := matcheventoutbox.CommandMeta{
			KafkaPartition: uint32(sc.msg.Partition),
			KafkaOffset:    uint64(sc.msg.Offset),
		}
		var out publisher.Outbound
		if no := sc.env.GetNewOrder(); no != nil {
			if h.Metrics != nil {
				h.Metrics.ObserveTrades(len(o.Trades))
			}
			out = publisher.BuildNewOrderEvents(h.Engine.Shard(), no, o.Trades, o.WalSeq, o.Duplicate)
		} else if co := sc.env.GetCancelOrder(); co != nil {
			out = publisher.BuildCancelEvents(co, o.WalSeq)
		} else {
			return 0, fmt.Errorf("consumer: empty envelope at offset %d", sc.msg.Offset)
		}
		if len(out.MatchEvents) == 0 && len(out.TradeEvents) == 0 {
			continue
		}
		if err := matcheventoutbox.AppendOutbound(h.EventOutbox, meta, out); err != nil {
			return 0, err
		}
	}
	syncStart := time.Now()
	if err := h.EventOutbox.Sync(); err != nil {
		return 0, err
	}
	syncDur := time.Since(syncStart)
	if h.Metrics != nil {
		h.Metrics.ObserveEventOutboxSync(syncDur)
		h.updateEventOutboxPendingMetric()
	}
	return syncDur, nil
}

func (h *Handler) updateEventOutboxPendingMetric() {
	if h.Metrics == nil || h.EventOutbox == nil {
		return
	}
	meta, err := eventoutbox.LoadMeta(h.EventOutbox.Dir())
	if err != nil {
		return
	}
	last := h.EventOutbox.LastDurableSeq()
	h.Metrics.SetEventOutboxPending(eventoutbox.PendingCount(last, meta.LastPublishedSeq))
}
