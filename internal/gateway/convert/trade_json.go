package convert

import (
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// TradeJSON GET /v1/trades 单条成交（rest-api §3.7）。
type TradeJSON struct {
	TradeID  string `json:"trade_id"`
	OrderID  string `json:"order_id"`
	Symbol   string `json:"symbol"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
	Fee      string `json:"fee"`
	FeeAsset string `json:"fee_asset"`
	IsMaker  bool   `json:"is_maker"`
	TradedAt string `json:"traded_at"`
}

// TradeListData GET /v1/trades 列表 data。
type TradeListData struct {
	Items   []TradeJSON `json:"items"`
	Cursor  string      `json:"cursor,omitempty"`
	HasMore bool        `json:"has_more"`
}

// TradeFromPB 映射 TradeInfo。
func TradeFromPB(t *orderv1.TradeInfo) TradeJSON {
	if t == nil {
		return TradeJSON{}
	}
	orderID := t.GetOrderId()
	if orderID == 0 {
		orderID = t.GetTakerOrderId()
	}
	return TradeJSON{
		TradeID:  OrderIDToString(t.GetTradeId()),
		OrderID:  OrderIDToString(orderID),
		Symbol:   t.GetSymbol(),
		Side:     t.GetSide(),
		Price:    DecimalToString(t.GetPrice()),
		Quantity: DecimalToString(t.GetQuantity()),
		Fee:      "0",
		FeeAsset: "",
		IsMaker:  t.GetIsMaker(),
		TradedAt: FormatTimestamp(t.GetCreatedAt()),
	}
}

// TradesFromPB 批量映射。
func TradesFromPB(trades []*orderv1.TradeInfo) []TradeJSON {
	out := make([]TradeJSON, 0, len(trades))
	for _, t := range trades {
		out = append(out, TradeFromPB(t))
	}
	return out
}
