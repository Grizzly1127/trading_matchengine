package engine_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/symbol"
	"github.com/shopspring/decimal"
)

func newTestShard() *symbol.Shard {
	return symbol.NewShard()
}

// assertSpreadInvariant 非空盘口时最优买价必须严格低于最优卖价。
func assertSpreadInvariant(t *testing.T, book *engine.OrderBook) {
	t.Helper()
	bid, hasBid := book.BestBid()
	ask, hasAsk := book.BestAsk()
	if hasBid && hasAsk && !bid.LessThan(ask) {
		t.Fatalf("spread violated: bid=%s ask=%s", bid, ask)
	}
}

func TestMatch_buyTakesRestingSell(t *testing.T) {
	sh := newTestShard()

	_, err := sh.Match(engine.Order{
		OrderID: 1, ClientOrderID: "sell-1", Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(2),
	}, 1)
	if err != nil {
		t.Fatal(err)
	}

	trades, err := sh.Match(engine.Order{
		OrderID: 2, ClientOrderID: "buy-1", Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 {
		t.Fatalf("trades len = %d, want 1", len(trades))
	}
	if !trades[0].Price.Equal(decimal.NewFromInt(100)) || !trades[0].Quantity.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("trade = %+v", trades[0])
	}
	if trades[0].MakerOrderID != 1 || trades[0].TakerOrderID != 2 {
		t.Fatalf("maker/taker = %+v", trades[0])
	}
	if _, ok := sh.Symbol("BTC-USDT").OrderBook.BestBid(); ok {
		t.Fatal("fully filled taker must not rest on book")
	}
}

func TestMatch_noMatch_restOnBook(t *testing.T) {
	sh := newTestShard()

	trades, err := sh.Match(engine.Order{
		OrderID: 3, ClientOrderID: "buy-1", Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(99), Quantity: decimal.NewFromInt(1),
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 0 {
		t.Fatalf("expected no trades, got %d", len(trades))
	}

	book := sh.Symbol("BTC-USDT").OrderBook
	if _, ok := book.BestBid(); !ok {
		t.Fatal("expected bid on book")
	}
}

func TestMatch_marketBuyTakesBestAsk(t *testing.T) {
	sh := newTestShard()

	_, err := sh.Match(engine.Order{
		OrderID: 10, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(3),
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sh.Match(engine.Order{
		OrderID: 11, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(101), Quantity: decimal.NewFromInt(1),
	}, 2)
	if err != nil {
		t.Fatal(err)
	}

	trades, err := sh.Match(engine.Order{
		OrderID: 12, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeMarket,
		Quantity: decimal.NewFromInt(2),
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 {
		t.Fatalf("trades len = %d, want 1", len(trades))
	}
	if !trades[0].Price.Equal(decimal.NewFromInt(100)) || !trades[0].Quantity.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("trade = %+v", trades[0])
	}

	price, ok := sh.Symbol("BTC-USDT").OrderBook.BestAsk()
	if !ok || !price.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("bestAsk = %s ok=%v, want 100 (1 unit left)", price, ok)
	}
}

func TestMatch_marketNoLiquidity_noResting(t *testing.T) {
	sh := newTestShard()

	trades, err := sh.Match(engine.Order{
		OrderID: 20, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeMarket,
		Quantity: decimal.NewFromInt(1),
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 0 {
		t.Fatalf("expected no trades, got %d", len(trades))
	}
	if _, ok := sh.Symbol("BTC-USDT").OrderBook.BestBid(); ok {
		t.Fatal("market buy with no liquidity must not rest on book")
	}
}

func TestMatch_marketPartialFill_remainderNotResting(t *testing.T) {
	sh := newTestShard()

	_, _ = sh.Match(engine.Order{
		OrderID: 30, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(50), Quantity: decimal.NewFromInt(1),
	}, 1)

	trades, err := sh.Match(engine.Order{
		OrderID: 31, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeMarket,
		Quantity: decimal.NewFromInt(5),
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || !trades[0].Quantity.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("partial fill trades = %+v", trades)
	}
}

func TestCancel_removesRestingOrder(t *testing.T) {
	sh := newTestShard()
	_, _ = sh.Match(engine.Order{
		OrderID: 4, ClientOrderID: "buy-1", Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(99), Quantity: decimal.NewFromInt(1),
	}, 1)
	if err := sh.Cancel("BTC-USDT", 4); err != nil {
		t.Fatal(err)
	}
	if _, ok := sh.Symbol("BTC-USDT").OrderBook.BestBid(); ok {
		t.Fatal("bid should be gone after cancel")
	}
}

func TestOrderBook_spreadInvariant_afterRestingOrders(t *testing.T) {
	sh := newTestShard()
	book := sh.Symbol("BTC-USDT").OrderBook

	_, _ = sh.Match(engine.Order{
		OrderID: 40, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(99), Quantity: decimal.NewFromInt(1),
	}, 1)
	assertSpreadInvariant(t, book)

	_, _ = sh.Match(engine.Order{
		OrderID: 41, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	}, 2)
	assertSpreadInvariant(t, book)
}

func TestMatch_limitPartialFill_remainderOnBook(t *testing.T) {
	sh := newTestShard()

	_, _ = sh.Match(engine.Order{
		OrderID: 50, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	}, 1)

	trades, err := sh.Match(engine.Order{
		OrderID: 51, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(2),
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || !trades[0].Quantity.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("trades = %+v", trades)
	}

	book := sh.Symbol("BTC-USDT").OrderBook
	bid, ok := book.BestBid()
	if !ok || !bid.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("bestBid = %s ok=%v, want 100", bid, ok)
	}
	assertSpreadInvariant(t, book)
}

func TestMatch_twoRestingSells_oneLimitBuyFills(t *testing.T) {
	sh := newTestShard()

	_, _ = sh.Match(engine.Order{
		OrderID: 60, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	}, 1)
	_, _ = sh.Match(engine.Order{
		OrderID: 61, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(101), Quantity: decimal.NewFromInt(1),
	}, 2)

	trades, err := sh.Match(engine.Order{
		OrderID: 62, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(101), Quantity: decimal.NewFromInt(2),
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 2 {
		t.Fatalf("trades len = %d, want 2", len(trades))
	}
	if !trades[0].Price.Equal(decimal.NewFromInt(100)) || !trades[1].Price.Equal(decimal.NewFromInt(101)) {
		t.Fatalf("trades = %+v", trades)
	}

	book := sh.Symbol("BTC-USDT").OrderBook
	if _, ok := book.BestAsk(); ok {
		t.Fatal("asks should be fully consumed")
	}
	assertSpreadInvariant(t, book)
}
