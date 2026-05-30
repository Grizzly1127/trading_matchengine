package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/cli"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

func testEngine(t *testing.T) *recovery.Engine {
	t.Helper()
	reg, err := symbolrules.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	eng, err := recovery.Open(recovery.Config{
		ShardID:        "shard-0",
		DataDir:        t.TempDir(),
		SnapshotEvery:  1000,
		SymbolRegistry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func TestHandleLine_newOrderAndMatch(t *testing.T) {
	eng := testEngine(t)

	sell := cli.HandleLine(eng, `{"op":"new_order","order_id":1,"side":"sell","price":"100","quantity":"1"}`, "BTC-USDT")
	if !sell.OK || sell.Op != "new_order" {
		t.Fatalf("sell: %+v", sell)
	}

	buy := cli.HandleLine(eng, `{"op":"new_order","order_id":2,"side":"buy","price":"100","quantity":"1"}`, "BTC-USDT")
	if !buy.OK || len(buy.Trades) != 1 {
		t.Fatalf("buy: %+v", buy)
	}
	if buy.Trades[0].MakerOrderID != 1 || buy.Trades[0].TakerOrderID != 2 {
		t.Fatalf("trade parties: %+v", buy.Trades[0])
	}
}

func TestHandleLine_duplicateOrder(t *testing.T) {
	eng := testEngine(t)

	line := `{"op":"new_order","order_id":1,"side":"sell","price":"100","quantity":"1"}`
	first := cli.HandleLine(eng, line, "BTC-USDT")
	if !first.OK || first.Duplicate {
		t.Fatalf("first: %+v", first)
	}

	dup := cli.HandleLine(eng, line, "BTC-USDT")
	if !dup.OK || !dup.Duplicate {
		t.Fatalf("dup: %+v", dup)
	}
}

func TestHandleLine_cancelAndStatus(t *testing.T) {
	eng := testEngine(t)

	cli.HandleLine(eng, `{"op":"new_order","order_id":1,"side":"sell","price":"100","quantity":"1"}`, "BTC-USDT")

	status := cli.HandleLine(eng, `{"op":"status"}`, "BTC-USDT")
	if !status.OK || status.ActiveCount != 1 || status.BestAsk != "100" {
		t.Fatalf("status before cancel: %+v", status)
	}

	cancel := cli.HandleLine(eng, `{"op":"cancel_order","order_id":1}`, "BTC-USDT")
	if !cancel.OK {
		t.Fatalf("cancel: %+v", cancel)
	}

	after := cli.HandleLine(eng, `{"op":"status"}`, "BTC-USDT")
	if after.ActiveCount != 0 {
		t.Fatalf("status after cancel: %+v", after)
	}
}

func TestHandleLine_invalidJSON(t *testing.T) {
	eng := testEngine(t)
	resp := cli.HandleLine(eng, `{bad`, "BTC-USDT")
	if resp.OK || resp.Error == "" {
		t.Fatalf("resp: %+v", resp)
	}
}

func TestRun_quitStopsLoop(t *testing.T) {
	eng := testEngine(t)

	in := strings.NewReader(`{"op":"status"}` + "\n" + `{"op":"quit"}` + "\n")
	out := &bytes.Buffer{}

	if err := cli.Run(context.Background(), eng, cli.Config{
		DefaultSymbol: "BTC-USDT",
		Input:         in,
		Output:        out,
	}); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%d body=%q", len(lines), out.String())
	}
}

func TestWriteResponse(t *testing.T) {
	var buf bytes.Buffer
	if err := cli.WriteResponse(&buf, cli.Response{OK: true, Op: "snapshot", LastSeq: 3}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"last_seq":3`) {
		t.Fatalf("got %q", buf.String())
	}
}
