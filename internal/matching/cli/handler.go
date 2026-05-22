package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/shopspring/decimal"
)

// HandleLine 解析并执行一行 JSONL 命令。
func HandleLine(eng *recovery.Engine, line, defaultSymbol string) Response {
	var cmd Command
	if err := json.Unmarshal([]byte(line), &cmd); err != nil {
		return Response{OK: false, Error: fmt.Sprintf("invalid json: %v", err)}
	}
	return HandleCommand(eng, cmd, defaultSymbol)
}

// HandleCommand 执行已解析命令。
func HandleCommand(eng *recovery.Engine, cmd Command, defaultSymbol string) Response {
	op := strings.ToLower(strings.TrimSpace(cmd.Op))
	switch op {
	case "new_order":
		return handleNewOrder(eng, cmd, defaultSymbol)
	case "cancel_order", "cancel":
		return handleCancel(eng, cmd, defaultSymbol)
	case "snapshot":
		return handleSnapshot(eng)
	case "status", "book":
		return handleStatus(eng, cmd, defaultSymbol)
	case "quit", "exit":
		return Response{OK: true, Op: "quit", LastSeq: eng.LastSeq()}
	default:
		return Response{OK: false, Op: op, Error: fmt.Sprintf("unknown op %q", cmd.Op)}
	}
}

func handleNewOrder(eng *recovery.Engine, cmd Command, defaultSymbol string) Response {
	symbolName := cmd.Symbol
	if symbolName == "" {
		symbolName = defaultSymbol
	}
	if cmd.OrderID == 0 {
		return Response{OK: false, Op: "new_order", Error: "order_id is required"}
	}
	if cmd.Price == "" || cmd.Quantity == "" {
		return Response{OK: false, Op: "new_order", Error: "price and quantity are required"}
	}

	side, err := parseSide(cmd.Side)
	if err != nil {
		return Response{OK: false, Op: "new_order", Error: err.Error()}
	}
	typ, err := parseOrderType(cmd.Type)
	if err != nil {
		return Response{OK: false, Op: "new_order", Error: err.Error()}
	}
	price, err := decimal.NewFromString(cmd.Price)
	if err != nil {
		return Response{OK: false, Op: "new_order", Error: fmt.Sprintf("invalid price: %v", err)}
	}
	qty, err := decimal.NewFromString(cmd.Quantity)
	if err != nil {
		return Response{OK: false, Op: "new_order", Error: fmt.Sprintf("invalid quantity: %v", err)}
	}

	now := time.Now().UTC()
	order := engine.Order{
		OrderID:       cmd.OrderID,
		ClientOrderID: cmd.ClientOrderID,
		Symbol:        symbolName,
		CreateTime:    now,
		UpdateTime:    now,
		Side:          side,
		Type:          typ,
		Price:         price,
		Quantity:      qty,
	}

	commandID := cmd.CommandID
	if commandID == 0 {
		commandID = cmd.OrderID
	}
	pbCmd := recovery.NewOrderFromEngine(order, commandID)

	beforeSeq := eng.LastSeq()
	trades, err := eng.ApplyNewOrder(pbCmd)
	if err != nil {
		return Response{OK: false, Op: "new_order", Error: err.Error()}
	}
	if len(trades) == 0 && eng.LastSeq() == beforeSeq {
		return Response{OK: true, Op: "new_order", Duplicate: true, LastSeq: eng.LastSeq()}
	}

	return Response{
		OK:      true,
		Op:      "new_order",
		Trades:  tradesToJSON(trades),
		LastSeq: eng.LastSeq(),
	}
}

func handleCancel(eng *recovery.Engine, cmd Command, defaultSymbol string) Response {
	symbolName := cmd.Symbol
	if symbolName == "" {
		symbolName = defaultSymbol
	}
	if cmd.OrderID == 0 {
		return Response{OK: false, Op: "cancel_order", Error: "order_id is required"}
	}

	pbCmd := &matchingv1.CancelOrderCommand{
		CommandId: cmd.CommandID,
		Symbol:    symbolName,
		OrderId:   cmd.OrderID,
	}
	if pbCmd.CommandId == 0 {
		pbCmd.CommandId = cmd.OrderID
	}

	if err := eng.ApplyCancel(pbCmd); err != nil {
		return Response{OK: false, Op: "cancel_order", Error: err.Error()}
	}
	return Response{OK: true, Op: "cancel_order", LastSeq: eng.LastSeq()}
}

func handleSnapshot(eng *recovery.Engine) Response {
	if err := eng.SnapshotNow(); err != nil {
		return Response{OK: false, Op: "snapshot", Error: err.Error()}
	}
	return Response{OK: true, Op: "snapshot", LastSeq: eng.LastSeq()}
}

func handleStatus(eng *recovery.Engine, cmd Command, defaultSymbol string) Response {
	symbolName := cmd.Symbol
	if symbolName == "" {
		symbolName = defaultSymbol
	}

	book := eng.Shard().Symbol(symbolName).OrderBook
	resp := Response{
		OK:          true,
		Op:          "status",
		LastSeq:     eng.LastSeq(),
		ActiveCount: book.ActiveOrderCount(),
	}

	if bid, ok := book.BestBid(); ok {
		resp.BestBid = bid.String()
	}
	if ask, ok := book.BestAsk(); ok {
		resp.BestAsk = ask.String()
	}

	active := book.ActiveOrders()
	orders := make([]Order, 0, len(active))
	for _, o := range active {
		remaining := o.Remaining
		if !remaining.IsPositive() {
			remaining = o.Quantity
		}
		orders = append(orders, Order{
			OrderID:   o.OrderID,
			Side:      sideString(o.Side),
			Price:     o.Price.String(),
			Quantity:  o.Quantity.String(),
			Remaining: remaining.String(),
		})
	}
	resp.ActiveOrders = orders
	return resp
}

func tradesToJSON(trades []engine.Trade) []Trade {
	if len(trades) == 0 {
		return nil
	}
	out := make([]Trade, len(trades))
	for i, t := range trades {
		out[i] = Trade{
			TradeID:      t.TradeID,
			Symbol:       t.Symbol,
			Price:        t.Price.String(),
			Quantity:     t.Quantity.String(),
			MakerOrderID: t.MakerOrderID,
			TakerOrderID: t.TakerOrderID,
		}
	}
	return out
}

func parseSide(s string) (engine.Side, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "buy", "b":
		return engine.SideBuy, nil
	case "sell", "s":
		return engine.SideSell, nil
	default:
		return 0, fmt.Errorf("invalid side %q (use buy or sell)", s)
	}
}

func parseOrderType(s string) (engine.OrderType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "limit", "l":
		return engine.OrderTypeLimit, nil
	case "market", "m":
		return engine.OrderTypeMarket, nil
	default:
		return 0, fmt.Errorf("invalid type %q (use limit or market)", s)
	}
}

func sideString(s engine.Side) string {
	switch s {
	case engine.SideBuy:
		return "buy"
	case engine.SideSell:
		return "sell"
	default:
		return "unknown"
	}
}
