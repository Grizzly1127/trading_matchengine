package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseVegetaLatenciesMs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.txt")
	const report = `Latencies     [mean, 50, 95, 99, max]    9.47879ms, 7.000473ms, 21.880155ms, 50.807472ms, 279.962536ms`
	if err := os.WriteFile(path, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, p99, err := parseVegetaLatencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if p99 < 50.7 || p99 > 50.9 {
		t.Fatalf("p99=%v want ~50.8", p99)
	}
}

func TestParseVegetaLatenciesSeconds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.txt")
	const report = `Latencies     [mean, 50, 95, 99, max]    16.216542968s, 17.434845873s, 34.005988346s, 36.330373849s, 1m31.636747229s`
	if err := os.WriteFile(path, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, p99, err := parseVegetaLatencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if p99 < 36300 || p99 > 36350 {
		t.Fatalf("p99=%v want ~36330", p99)
	}
}

func TestPrintL3Breakdown(t *testing.T) {
	dir := t.TempDir()
	veg := filepath.Join(dir, "report.txt")
	if err := os.WriteFile(veg, []byte(`Latencies     [mean, 50, 95, 99, max]    9ms, 7ms, 20ms, 55ms, 200ms`), 0o644); err != nil {
		t.Fatal(err)
	}
	pre := `# TYPE order_place_order_db_tx_ms histogram
order_place_order_db_tx_ms_bucket{le="10"} 0
order_place_order_db_tx_ms_bucket{le="50"} 10
order_place_order_db_tx_ms_bucket{le="100"} 10
order_place_order_db_tx_ms_bucket{le="+Inf"} 10
order_place_order_db_tx_ms_sum 40
order_place_order_db_tx_ms_count 10
`
	post := `# TYPE order_place_order_db_tx_ms histogram
order_place_order_db_tx_ms_bucket{le="10"} 0
order_place_order_db_tx_ms_bucket{le="50"} 100
order_place_order_db_tx_ms_bucket{le="100"} 100
order_place_order_db_tx_ms_bucket{le="+Inf"} 100
order_place_order_db_tx_ms_sum 400
order_place_order_db_tx_ms_count 100
`
	orderPre := filepath.Join(dir, "order-pre.prom")
	orderPost := filepath.Join(dir, "order-post.prom")
	if err := os.WriteFile(orderPre, []byte(pre), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orderPost, []byte(post), 0o644); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	if err := printL3Breakdown(&out, veg, orderPre, orderPost, "", ""); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "vegeta_p99_ms=55.00") {
		t.Fatalf("missing vegeta p99: %s", s)
	}
	if !strings.Contains(s, "order_place_order_db_tx_ms_samples=90") {
		t.Fatalf("missing db tx samples: %s", s)
	}
}
