package publisher

import (
	"context"
	"fmt"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

func matchSymbol(ev *matchingv1.MatchEvent) string { return ev.GetSymbol() }

func tradeSymbol(ev *matchingv1.TradeEvent) string {
	if tr := ev.GetTrade(); tr != nil {
		return tr.GetSymbol()
	}
	return ""
}

// writeEventsBySymbol 按 symbol 保序分组，每组一次 WriteBatch（同 key）。
func writeEventsBySymbol[T any](
	ctx context.Context,
	prod kafka.Producer,
	topic string,
	events []T,
	symbolFn func(T) string,
	marshalFn func([]T) ([][]byte, error),
) error {
	if len(events) == 0 {
		return nil
	}
	order := make([]string, 0, 1)
	bySym := make(map[string][]T)
	for _, ev := range events {
		sym := symbolFn(ev)
		if sym == "" {
			return fmt.Errorf("publisher: event missing symbol for topic %s", topic)
		}
		if _, ok := bySym[sym]; !ok {
			order = append(order, sym)
		}
		bySym[sym] = append(bySym[sym], ev)
	}
	for _, sym := range order {
		group := bySym[sym]
		vals, err := marshalFn(group)
		if err != nil {
			return err
		}
		if err := prod.WriteBatch(ctx, topic, []byte(sym), vals); err != nil {
			return err
		}
	}
	return nil
}
