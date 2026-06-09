package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/pkg/benchutil"
)

type latencyLine struct {
	name string
	p50  float64
	p95  float64
	p99  float64
	n    float64
}

func printL3Breakdown(
	w io.Writer,
	vegetaReport string,
	orderPre, orderPost string,
	gatewayPre, gatewayPost string,
) error {
	fmt.Fprintln(w, "# L3 延迟分解（压测窗口直方图差分：post − pre）")
	fmt.Fprintln(w, "# vegeta = 端到端 HTTP；gateway = POST /v1/orders；order_* = Order gRPC 分段")
	fmt.Fprintln(w)

	vegP50, vegP95, vegP99, err := parseVegetaLatencies(vegetaReport)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "vegeta_p50_ms=%.2f\n", vegP50)
	fmt.Fprintf(w, "vegeta_p95_ms=%.2f\n", vegP95)
	fmt.Fprintf(w, "vegeta_p99_ms=%.2f\n", vegP99)
	fmt.Fprintln(w)

	lines, err := deltaHistogramLines(orderPre, orderPost, gatewayPre, gatewayPost)
	if err != nil {
		return err
	}
	for _, ln := range lines {
		fmt.Fprintf(w, "%s_p50_ms=%.2f\n", ln.name, ln.p50)
		fmt.Fprintf(w, "%s_p95_ms=%.2f\n", ln.name, ln.p95)
		fmt.Fprintf(w, "%s_p99_ms=%.2f\n", ln.name, ln.p99)
		fmt.Fprintf(w, "%s_samples=%.0f\n", ln.name, ln.n)
		fmt.Fprintln(w)
	}

	if gw := findLine(lines, "gateway_place_order_duration_ms"); gw != nil {
		if grpc := findLine(lines, "order_grpc_place_order_duration_ms"); grpc != nil {
			fmt.Fprintf(w, "gateway_minus_grpc_p99_ms=%.2f\n", gw.p99-grpc.p99)
		}
	}
	if grpc := findLine(lines, "order_grpc_place_order_duration_ms"); grpc != nil {
		if db := findLine(lines, "order_place_order_db_tx_ms"); db != nil {
			fmt.Fprintf(w, "grpc_minus_db_tx_p99_ms=%.2f\n", grpc.p99-db.p99)
			fmt.Fprintf(w, "bottleneck_hint=")
			fmt.Fprintln(w, bottleneckHint(vegP99, lines, db.p99))
		}
	}
	return nil
}

func deltaHistogramLines(orderPre, orderPost, gatewayPre, gatewayPost string) ([]latencyLine, error) {
	type pair struct {
		name string
		pre  string
		post string
	}
	pairs := []pair{
		{"gateway_place_order_duration_ms", gatewayPre, gatewayPost},
		{"order_grpc_place_order_duration_ms", orderPre, orderPost},
		{"order_place_order_validate_ms", orderPre, orderPost},
		{"order_place_order_idempotency_ms", orderPre, orderPost},
		{"order_place_order_db_tx_ms", orderPre, orderPost},
	}
	seen := make(map[string]struct{}, len(pairs))
	var out []latencyLine
	for _, p := range pairs {
		if _, ok := seen[p.name]; ok {
			continue
		}
		seen[p.name] = struct{}{}
		ln, err := deltaLatencyLine(p.name, p.pre, p.post)
		if err != nil {
			return nil, err
		}
		out = append(out, ln)
	}
	return out, nil
}

func deltaLatencyLine(name, prePath, postPath string) (latencyLine, error) {
	ln := latencyLine{name: name}
	if strings.TrimSpace(prePath) == "" || strings.TrimSpace(postPath) == "" {
		return ln, nil
	}
	preBody, err := os.ReadFile(prePath)
	if err != nil {
		return ln, fmt.Errorf("read %s: %w", prePath, err)
	}
	postBody, err := os.ReadFile(postPath)
	if err != nil {
		return ln, fmt.Errorf("read %s: %w", postPath, err)
	}
	preH, err := benchutil.ParseHistogram(strings.NewReader(string(preBody)), name)
	if err != nil {
		return ln, err
	}
	postH, err := benchutil.ParseHistogram(strings.NewReader(string(postBody)), name)
	if err != nil {
		return ln, err
	}
	deltaH, err := benchutil.SubtractHistogram(postH, preH)
	if err != nil {
		return ln, fmt.Errorf("%s: %w", name, err)
	}
	if deltaH.Count <= 0 {
		return ln, nil
	}
	ln.n = deltaH.Count
	ln.p50 = deltaH.Quantile(0.50)
	ln.p95 = deltaH.Quantile(0.95)
	ln.p99 = deltaH.Quantile(0.99)
	return ln, nil
}

var vegetaLatencyRE = regexp.MustCompile(`([0-9]*\.?[0-9]+)(ms|s),?`)

func parseVegetaLatencies(reportPath string) (p50, p95, p99 float64, err error) {
	body, err := os.ReadFile(reportPath)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.Contains(line, "Latencies") {
			continue
		}
		matches := vegetaLatencyRE.FindAllStringSubmatch(line, -1)
		if len(matches) < 4 {
			return 0, 0, 0, fmt.Errorf("parse vegeta latencies from %s", reportPath)
		}
		p50, err = parseVegetaDuration(matches[1][1], matches[1][2])
		if err != nil {
			return 0, 0, 0, err
		}
		p95, err = parseVegetaDuration(matches[2][1], matches[2][2])
		if err != nil {
			return 0, 0, 0, err
		}
		p99, err = parseVegetaDuration(matches[3][1], matches[3][2])
		return p50, p95, p99, err
	}
	return 0, 0, 0, fmt.Errorf("no Latencies line in %s", reportPath)
}

func parseVegetaDuration(val, unit string) (float64, error) {
	var ms float64
	_, err := fmt.Sscanf(val, "%f", &ms)
	if err != nil {
		return 0, err
	}
	if unit == "s" {
		ms *= 1000
	}
	return ms, nil
}

func findLine(lines []latencyLine, name string) *latencyLine {
	for i := range lines {
		if lines[i].name == name {
			return &lines[i]
		}
	}
	return nil
}

func bottleneckHint(vegP99 float64, lines []latencyLine, dbP99 float64) string {
	gw := findLine(lines, "gateway_place_order_duration_ms")
	grpc := findLine(lines, "order_grpc_place_order_duration_ms")
	idem := findLine(lines, "order_place_order_idempotency_ms")

	if gw != nil && gw.p99 > 0 && grpc != nil && grpc.p99 > 0 {
		if vegP99-gw.p99 > 10 {
			return "vegeta_client_or_network (gateway_p99 much lower than vegeta_p99)"
		}
		if gw.p99-grpc.p99 > 5 {
			return "gateway_http_layer (json/auth/grpc_client)"
		}
	}
	if dbP99 > 0 && grpc != nil && dbP99 >= grpc.p99*0.7 {
		return "order_pg_transaction (db_tx dominates grpc)"
	}
	if idem != nil && idem.p99 > 5 && idem.p99 >= dbP99 {
		return "order_idempotency_redis_or_pg"
	}
	if grpc != nil && grpc.p99 > 0 && dbP99 > 0 && grpc.p99-dbP99 > 5 {
		return "order_service_non_db (validate/idempotency/grpc overhead)"
	}
	return "unclear_run_segmented_metrics_again"
}
