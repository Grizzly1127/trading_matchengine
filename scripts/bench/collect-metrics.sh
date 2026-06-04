#!/usr/bin/env bash
# 采集 Matching Prometheus 指标摘要；可选两次采样计算 TPS。
#
# 用法:
#   ./scripts/bench/collect-metrics.sh
#   ./scripts/bench/collect-metrics.sh --delta 60   # 间隔 60s 后算 TPS
#
# 环境变量:
#   METRICS_URL  默认 http://localhost:9101/metrics
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
METRICS_URL="${METRICS_URL:-http://localhost:9101/metrics}"
DELTA_SEC=0

usage() {
  cat <<EOF
用法: $(basename "$0") [--delta SEC]

  --delta SEC   先采样一次，等待 SEC 秒后再采样并输出 TPS 估算
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delta) DELTA_SEC="${2:-60}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

report() {
  local label="${1:-}"
  go run "$ROOT/cmd/bench-report/main.go" -url "$METRICS_URL" ${label:+-label "$label"}
}

if [[ "$DELTA_SEC" -le 0 ]]; then
  report "snapshot"
  exit 0
fi

prom_gauge() {
  local pattern="$1"
  local raw
  raw="$(curl -sf "$METRICS_URL")" || return 1
  echo "$raw" | awk -v re="$pattern" '$0 ~ re { print $2; exit }'
}

report "before"
before="$(prom_gauge '^matching_commands_processed_total ')"
sleep "$DELTA_SEC"
report "after"
after="$(prom_gauge '^matching_commands_processed_total ')"
if [[ -n "$before" && -n "$after" ]]; then
  awk -v b="$before" -v a="$after" -v s="$DELTA_SEC" 'BEGIN {
    printf "drain_tps_after_load: %.0f (/s over %ds, tail backlog only)\n", (a-b)/s, s
  }'
fi
