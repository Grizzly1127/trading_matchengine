// bench-report 解析 Matching Prometheus 指标并输出摘要（L2 压测报告辅助）。
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/benchutil"
)

var histogramNames = []string{
	"matching_processing_latency_ms",
	"matching_wal_append_latency_ms",
	"matching_publish_latency_ms",
	"matching_publish_match_latency_ms",
	"matching_publish_trade_latency_ms",
}

func main() {
	url := flag.String("url", "http://localhost:9101/metrics", "Prometheus metrics URL")
	label := flag.String("label", "", "报告标签（如 before/after）")
	deltaPre := flag.String("delta-pre", "", "窗口起点 Prometheus 文本文件（与 -delta-post 合用）")
	deltaPost := flag.String("delta-post", "", "窗口终点 Prometheus 文本文件")
	flag.Parse()

	if *deltaPre != "" || *deltaPost != "" {
		if *deltaPre == "" || *deltaPost == "" {
			fmt.Fprintln(os.Stderr, "delta 模式需同时指定 -delta-pre 与 -delta-post")
			os.Exit(1)
		}
		if err := printDeltaReport(os.Stdout, *deltaPre, *deltaPost, *label); err != nil {
			fmt.Fprintf(os.Stderr, "delta: %v\n", err)
			os.Exit(1)
		}
		return
	}

	body, err := fetch(*url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch metrics: %v\n", err)
		os.Exit(1)
	}
	if err := printReport(os.Stdout, body, *label); err != nil {
		fmt.Fprintf(os.Stderr, "parse: %v\n", err)
		os.Exit(1)
	}
}

func fetch(url string) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %s", resp.Status)
	}
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func readFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func printDeltaReport(w io.Writer, prePath, postPath, label string) error {
	preBody, err := readFile(prePath)
	if err != nil {
		return fmt.Errorf("read pre: %w", err)
	}
	postBody, err := readFile(postPath)
	if err != nil {
		return fmt.Errorf("read post: %w", err)
	}
	if label == "" {
		label = "load_window"
	}
	fmt.Fprintf(w, "=== %s (post − pre 直方图差分，仅含本窗口新样本) ===\n", label)

	preCounters, err := benchutil.ParseCounters(strings.NewReader(preBody),
		"matching_commands_processed_total", "matching_commands_failed_total")
	if err != nil {
		return err
	}
	postCounters, err := benchutil.ParseCounters(strings.NewReader(postBody),
		"matching_commands_processed_total", "matching_commands_failed_total")
	if err != nil {
		return err
	}
	procDelta := postCounters["matching_commands_processed_total"] - preCounters["matching_commands_processed_total"]
	failDelta := postCounters["matching_commands_failed_total"] - preCounters["matching_commands_failed_total"]
	fmt.Fprintf(w, "matching_commands_processed_delta: %.0f\n", procDelta)
	fmt.Fprintf(w, "matching_commands_failed_delta: %.0f\n", failDelta)

	for _, name := range histogramNames {
		preH, err := benchutil.ParseHistogram(strings.NewReader(preBody), name)
		if err != nil {
			return err
		}
		postH, err := benchutil.ParseHistogram(strings.NewReader(postBody), name)
		if err != nil {
			return err
		}
		deltaH, err := benchutil.SubtractHistogram(postH, preH)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if deltaH.Count <= 0 {
			fmt.Fprintf(w, "%s: (no samples in window)\n", name)
			continue
		}
		switch name {
		case "matching_processing_latency_ms":
			fmt.Fprintf(w, "%s P50: %.2f ms\n", name, deltaH.Quantile(0.50))
			fmt.Fprintf(w, "%s P95: %.2f ms\n", name, deltaH.Quantile(0.95))
			fmt.Fprintf(w, "%s P99: %.2f ms\n", name, deltaH.Quantile(0.99))
		default:
			fmt.Fprintf(w, "%s P99: %.2f ms\n", name, deltaH.Quantile(0.99))
		}
		fmt.Fprintf(w, "%s samples_in_window: %.0f\n", name, deltaH.Count)
	}
	return nil
}

func printReport(w io.Writer, body, label string) error {
	r := strings.NewReader(body)
	counters, err := benchutil.ParseCounters(r, "matching_commands_processed_total", "matching_commands_failed_total")
	if err != nil {
		return err
	}
	r2 := strings.NewReader(body)
	proc, err := benchutil.ParseHistogram(r2, "matching_processing_latency_ms")
	if err != nil {
		return err
	}
	r3 := strings.NewReader(body)
	wal, err := benchutil.ParseHistogram(r3, "matching_wal_append_latency_ms")
	if err != nil {
		return err
	}
	rPub := strings.NewReader(body)
	pub, err := benchutil.ParseHistogram(rPub, "matching_publish_latency_ms")
	if err != nil {
		return err
	}
	rPubM := strings.NewReader(body)
	pubMatch, err := benchutil.ParseHistogram(rPubM, "matching_publish_match_latency_ms")
	if err != nil {
		return err
	}
	rPubT := strings.NewReader(body)
	pubTrade, err := benchutil.ParseHistogram(rPubT, "matching_publish_trade_latency_ms")
	if err != nil {
		return err
	}
	r4 := strings.NewReader(body)
	lagMap, err := benchutil.ParseCounters(r4, "matching_kafka_lag")
	if err != nil {
		return err
	}

	if label != "" {
		fmt.Fprintf(w, "=== %s ===\n", label)
	}
	fmt.Fprintf(w, "matching_commands_processed_total: %.0f\n", counters["matching_commands_processed_total"])
	fmt.Fprintf(w, "matching_commands_failed_total: %.0f\n", counters["matching_commands_failed_total"])
	fmt.Fprintf(w, "matching_kafka_lag: %.0f\n", lagMap["matching_kafka_lag"])
	fmt.Fprintf(w, "matching_processing_latency_ms P50: %.2f ms\n", proc.Quantile(0.50))
	fmt.Fprintf(w, "matching_processing_latency_ms P95: %.2f ms\n", proc.Quantile(0.95))
	fmt.Fprintf(w, "matching_processing_latency_ms P99: %.2f ms\n", proc.Quantile(0.99))
	fmt.Fprintf(w, "matching_wal_append_latency_ms P99: %.2f ms\n", wal.Quantile(0.99))
	fmt.Fprintf(w, "matching_publish_latency_ms P99: %.2f ms\n", pub.Quantile(0.99))
	fmt.Fprintf(w, "matching_publish_match_latency_ms P99: %.2f ms\n", pubMatch.Quantile(0.99))
	fmt.Fprintf(w, "matching_publish_trade_latency_ms P99: %.2f ms\n", pubTrade.Quantile(0.99))
	return nil
}
