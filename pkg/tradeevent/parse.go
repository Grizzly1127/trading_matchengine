package tradeevent

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// Trade 从 trade.events 解码出的成交字段。
type Trade struct {
	TradeID       uint64
	Symbol        string
	Price         string
	Quantity      string
	MakerOrderID  uint64
	TakerOrderID  uint64
	TradeTimeMs   int64
}

// ParseKafkaMessage 解码 Kafka 消息中的 TradeEvent。
func ParseKafkaMessage(msg kafka.Message) (Trade, error) {
	var ev matchingv1.TradeEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return Trade{}, fmt.Errorf("decode trade event: %w", err)
	}
	tr := ev.GetTrade()
	if tr == nil || tr.GetTradeId() == 0 {
		return Trade{}, fmt.Errorf("trade_id is required")
	}
	if tr.GetPrice() == nil || tr.GetQuantity() == nil {
		return Trade{}, fmt.Errorf("price and quantity are required")
	}
	symbol := tr.GetSymbol()
	if symbol == "" {
		return Trade{}, fmt.Errorf("symbol is required")
	}

	tradeTimeMs := time.Now().UnixMilli()
	if ct := tr.GetCreateTime(); ct != nil {
		tradeTimeMs = ct.AsTime().UnixMilli()
	}
	return Trade{
		TradeID:      tr.GetTradeId(),
		Symbol:       symbol,
		Price:        tr.GetPrice().GetValue(),
		Quantity:     tr.GetQuantity().GetValue(),
		MakerOrderID: tr.GetMakerOrderId(),
		TakerOrderID: tr.GetTakerOrderId(),
		TradeTimeMs:  tradeTimeMs,
	}, nil
}
