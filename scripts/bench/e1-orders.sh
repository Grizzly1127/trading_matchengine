#!/usr/bin/env bash
# L3 全链路基准：经 Gateway POST /v1/orders 压测（需 vegeta）。
#
# 前置:
#   ./scripts/dev.sh start --build
#   ./scripts/e2e-api.sh step deposit   # 或本脚本 --deposit
#
# 用法:
#   ./scripts/bench/e1-orders.sh
#   ./scripts/bench/e1-orders.sh --rate 500 --duration 3m --workers 30
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-dev-token-change-me}"
SYMBOL="${SYMBOL:-BTC-USDT}"
USER_ID="${USER_ID:-1}"
RATE="${RATE:-200}"
DURATION="${DURATION:-3m}"
WORKERS="${WORKERS:-50}"
PRICE="${PRICE:-65000}"
QTY="${QTY:-0.001}"
DO_DEPOSIT=false
RUN_ID="${RUN_ID:-$(date +%s)}"

usage() {
  cat <<EOF
用法: $(basename "$0") [options]

  --rate N          vegeta 速率（默认 200/s）
  --duration D      时长（默认 3m）
  --workers N       并发 worker（默认 50）
  --deposit         压测前执行 e2e deposit
  --base-url URL    Gateway（默认 http://localhost:8080）
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --deposit) DO_DEPOSIT=true; shift ;;
    --base-url) BASE_URL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage; exit 1 ;;
  esac
done

command -v vegeta >/dev/null 2>&1 || {
  echo "需要 vegeta: go install github.com/tsenart/vegeta@latest" >&2
  exit 1
}

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

if [[ "$DO_DEPOSIT" == true ]]; then
  log "充值 (e2e deposit)..."
  "$ROOT/scripts/e2e-api.sh" step deposit
fi

REPORT_DIR="$ROOT/reports/$(date +%Y%m%d-%H%M%S)-l3-e1"
mkdir -p "$REPORT_DIR"
TARGETS="$REPORT_DIR/targets.txt"
RESULTS="$REPORT_DIR/results.bin"

log "生成 targets ($RATE/s x $DURATION)..."
: >"$TARGETS"
count=$((RATE * 180))
if [[ "$DURATION" == *m ]]; then
  min="${DURATION%m}"
  count=$((RATE * min * 60))
fi
for i in $(seq 1 "$count"); do
  coid="bench-${RUN_ID}-${i}"
  printf 'POST %s/v1/orders\nAuthorization: Bearer %s\nContent-Type: application/json\n\n{"user_id":%s,"client_order_id":"%s","symbol":"%s","side":"BUY","type":"LIMIT","price":"%s","quantity":"%s","time_in_force":"GTC"}\n\n' \
    "$BASE_URL" "$TOKEN" "$USER_ID" "$coid" "$SYMBOL" "$PRICE" "$QTY" >>"$TARGETS"
done

log "vegeta attack..."
vegeta attack -targets="$TARGETS" -rate="$RATE" -duration="$DURATION" -workers="$WORKERS" >"$RESULTS"
vegeta report -type=text "$RESULTS" | tee "$REPORT_DIR/report.txt"
vegeta report -type='hist[0,2ms,5ms,10ms,25ms,50ms,100ms,250ms,500ms,1s]' "$RESULTS" | tee "$REPORT_DIR/histogram.txt"

log "Matching 指标快照..."
if curl -sf "${METRICS_URL:-http://localhost:9101/metrics}" >/dev/null 2>&1; then
  go run "$ROOT/cmd/bench-report/main.go" | tee "$REPORT_DIR/matching-metrics.txt"
fi

log "Order outbox（若 Prometheus :9104 可用）..."
curl -sf "http://localhost:9104/metrics" 2>/dev/null | grep -E 'order_outbox|order_stuck' | tee "$REPORT_DIR/order-metrics.txt" || true

log "报告: $REPORT_DIR/"
