// bench-targets 快速生成 L3 vegeta JSON targets（替代 bash 逐条 base64 循环）。
package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var (
		out       = flag.String("out", "", "输出文件路径（必填）")
		count     = flag.Uint64("count", 0, "target 条数（必填）")
		baseURL   = flag.String("base-url", "http://localhost:8080", "Gateway 根 URL")
		token     = flag.String("token", "dev-token-change-me", "Bearer token")
		symbol    = flag.String("symbol", "BTC-USDT", "交易对")
		side      = flag.String("side", "BUY", "BUY 或 SELL")
		price     = flag.String("price", "65000", "限价")
		qty       = flag.String("qty", "0.001", "数量")
		numUsers  = flag.Uint64("users", 50, "轮询 user_id 数量")
		userBase  = flag.Uint64("user-base", 1, "起始 user_id")
		runID     = flag.String("run-id", "", "client_order_id 前缀（默认当前 Unix 秒）")
		progress  = flag.Uint64("progress-every", 100_000, "每 N 条向 stderr 打进度；0 禁用")
	)
	flag.Parse()

	if *out == "" || *count == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if *numUsers == 0 {
		fmt.Fprintln(os.Stderr, "users must be > 0")
		os.Exit(2)
	}
	if *runID == "" {
		*runID = fmt.Sprintf("%d", time.Now().Unix())
	}

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %q: %v\n", *out, err)
		os.Exit(1)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	orderURL := fmt.Sprintf("%s/v1/orders", trimRightSlash(*baseURL))
	enc := base64.StdEncoding

	for i := uint64(1); i <= *count; i++ {
		uid := *userBase + (i-1)%*numUsers
		body := fmt.Sprintf(`{"user_id":%d,"client_order_id":"bench-%s-%d","symbol":"%s","side":"%s","type":"LIMIT","price":"%s","quantity":"%s","time_in_force":"GTC"}`,
			uid, *runID, i, *symbol, *side, *price, *qty)
		b64 := enc.EncodeToString([]byte(body))

		if _, err := fmt.Fprintf(w,
			`{"method":"POST","url":"%s","header":{"Authorization":["Bearer %s"],"Content-Type":["application/json"]},"body":"%s"}`+"\n",
			orderURL, *token, b64); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			os.Exit(1)
		}

		if *progress > 0 && i%*progress == 0 {
			fmt.Fprintf(os.Stderr, "bench-targets: %d / %d\n", i, *count)
		}
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "bench-targets: wrote %d targets -> %s\n", *count, *out)
}

func trimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
