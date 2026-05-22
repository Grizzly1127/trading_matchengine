package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
)

// Config 控制 JSONL 命令循环。
type Config struct {
	DefaultSymbol string
	Input         io.Reader
	Output        io.Writer
	// UsageOutput 非空且 ShowUsageHint 为 true 时打印 Usage。
	UsageOutput   io.Writer
	ShowUsageHint bool
}

// Run 从 Input 读取 JSONL，将 Response 写入 Output，直到 EOF 或 quit。
func Run(ctx context.Context, eng *recovery.Engine, cfg Config) error {
	if cfg.Input == nil {
		return fmt.Errorf("cli: input is required")
	}
	if cfg.Output == nil {
		return fmt.Errorf("cli: output is required")
	}
	if cfg.DefaultSymbol == "" {
		cfg.DefaultSymbol = "BTC-USDT"
	}

	if cfg.ShowUsageHint && cfg.UsageOutput != nil {
		fmt.Fprint(cfg.UsageOutput, Usage())
	}

	sc := bufio.NewScanner(cfg.Input)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)

	for sc.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		resp := HandleLine(eng, line, cfg.DefaultSymbol)
		if err := WriteResponse(cfg.Output, resp); err != nil {
			return err
		}
		if resp.Op == "quit" && resp.OK {
			return nil
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read commands: %w", err)
	}
	return nil
}

// WriteResponse 将 Response 序列化为单行 JSON。
func WriteResponse(w io.Writer, resp Response) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", b)
	return err
}

// Shutdown 可选写 snapshot 并关闭引擎。
func Shutdown(eng *recovery.Engine, snapshotOnExit bool) error {
	if snapshotOnExit {
		if err := eng.SnapshotNow(); err != nil {
			return fmt.Errorf("snapshot on exit: %w", err)
		}
	}
	if err := eng.Close(); err != nil {
		return fmt.Errorf("close engine: %w", err)
	}
	return nil
}

// Usage 返回命令帮助文本。
func Usage() string {
	return `
JSONL 命令（每行一个 JSON 对象）：

  {"op":"new_order","order_id":1,"symbol":"BTC-USDT","side":"buy","price":"100","quantity":"1"}
  {"op":"cancel_order","symbol":"BTC-USDT","order_id":1}
  {"op":"snapshot"}
  {"op":"status","symbol":"BTC-USDT"}
  {"op":"quit"}

stdout 每行输出一条 JSON 结果；stderr 为日志。
`
}
