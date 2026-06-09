#!/usr/bin/env bash
# L3 全链路基准：经 Gateway POST /v1/orders 压测（需 vegeta ≥ v12.7，JSON target 格式）。
#
# 默认每轮：停服 → reset-dev → dev.sh start --build → 按压测量自动充值 → 压测。
# 买单按 price×quantity 冻结 USDT；多用户轮询避免单用户余额耗尽。
#
# 用法:
#   ./scripts/bench/e1-orders.sh --rate 500 --duration 3m
#   ./scripts/bench/e1-orders.sh --no-reset-env --no-deposit --rate 10 --duration 2s
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-dev-token-change-me}"
SYMBOL="${SYMBOL:-BTC-USDT}"
USER_BASE="${USER_BASE:-1}"
NUM_USERS="${NUM_USERS:-50}"
RATE="${RATE:-200}"
DURATION="${DURATION:-3m}"
WORKERS="${WORKERS:-50}"
PRICE="${PRICE:-65000}"
QTY="${QTY:-0.001}"
SIDE="${SIDE:-BUY}"
DEPOSIT_HEADROOM="${DEPOSIT_HEADROOM:-1.1}"
DO_DEPOSIT=true
RESET_ENV=true
START_WAIT_SEC="${START_WAIT_SEC:-120}"
OUTBOX_DRAIN_TIMEOUT="${OUTBOX_DRAIN_TIMEOUT:-600}"
OUTBOX_DRAIN_INTERVAL="${OUTBOX_DRAIN_INTERVAL:-2}"
MATCHING_DRAIN_TIMEOUT="${MATCHING_DRAIN_TIMEOUT:-600}"
MATCHING_DRAIN_INTERVAL="${MATCHING_DRAIN_INTERVAL:-2}"
MATCHING_LAG_MAX="${MATCHING_LAG_MAX:-0}"
ORDER_METRICS_URL="${ORDER_METRICS_URL:-http://localhost:9104/metrics}"
GATEWAY_METRICS_URL="${GATEWAY_METRICS_URL:-http://localhost:9103/metrics}"
METRICS_URL="${METRICS_URL:-http://localhost:9101/metrics}"
RUN_ID="${RUN_ID:-$(date +%s)}"

usage() {
  cat <<EOF
用法: $(basename "$0") [options]

  --rate N          vegeta 速率（默认 200/s）
  --duration D      时长（默认 3m，支持 30s / 3m / 1h）
  --workers N       并发 worker（默认 50）
  --users N         压测用户数量，轮询 user_id（默认 50，从 --user-base 起）
  --user-base N     起始 user_id（默认 1）
  --price P         限价（默认 65000）
  --qty Q           数量（默认 0.001）
  --side BUY|SELL   方向（默认 BUY；SELL 充值 BTC）
  --no-deposit      跳过自动充值（仅调试）
  --no-reset-env    跳过停服/reset-dev/重启
  --drain-timeout S       Order Outbox 排空超时（默认 600s）
  --matching-drain-timeout S  Matching 追平超时（默认 600s）
  --matching-lag-max N    投递 SLA 允许的最大 matching_kafka_lag（默认 0）
  --base-url URL    Gateway（默认 http://localhost:8080）

默认会按「请求数 × 冻结额 × ${DEPOSIT_HEADROOM}」为每个压测用户充值，避免 422 余额不足。
默认每轮: dev.sh stop → reset-dev.sh -y --migrate --kafka-topics → dev.sh start --build
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --users) NUM_USERS="$2"; shift 2 ;;
    --user-base) USER_BASE="$2"; shift 2 ;;
    --price) PRICE="$2"; shift 2 ;;
    --qty) QTY="$2"; shift 2 ;;
    --side) SIDE="$2"; shift 2 ;;
    --deposit) DO_DEPOSIT=true; shift ;;
    --no-deposit) DO_DEPOSIT=false; shift ;;
    --no-reset-env) RESET_ENV=false; shift ;;
    --drain-timeout) OUTBOX_DRAIN_TIMEOUT="$2"; shift 2 ;;
    --matching-drain-timeout) MATCHING_DRAIN_TIMEOUT="$2"; shift 2 ;;
    --matching-lag-max) MATCHING_LAG_MAX="$2"; shift 2 ;;
    --base-url) BASE_URL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage; exit 1 ;;
  esac
done

command -v vegeta >/dev/null 2>&1 || {
  echo "需要 vegeta: go install github.com/tsenart/vegeta@latest" >&2
  exit 1
}

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

case "$SIDE" in
  BUY|SELL) ;;
  *) die "invalid --side: $SIDE (use BUY or SELL)" ;;
esac

api_code() {
  printf '%s' "$1" | sed -n 's/.*"code"[[:space:]]*:[[:space:]]*\(-\?[0-9]*\).*/\1/p' | head -1
}

api_post_json() {
  curl -sS -X POST "$1" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -d "$2"
}

wait_for_gateway() {
  log "等待 Gateway 就绪（最多 ${START_WAIT_SEC}s）..."
  local i=0 body code
  while (( i < START_WAIT_SEC )); do
    body="$(curl -sS "${BASE_URL}/v1/health" -H "Accept: application/json" 2>/dev/null || true)"
    code="$(api_code "$body")"
    if [[ "$code" == "0" ]]; then
      log "Gateway 已就绪"
      return 0
    fi
    sleep 2
    ((i += 2)) || true
  done
  die "Gateway 未在 ${START_WAIT_SEC}s 内就绪（${BASE_URL}/v1/health）"
}

reset_and_start() {
  log ">>> 停止微服务"
  bash "$ROOT/scripts/dev.sh" stop || true

  log ">>> 重置开发环境（Postgres/Redis/Kafka/WAL）"
  bash "$ROOT/scripts/reset-dev.sh" -y --migrate --kafka-topics

  log ">>> 启动微服务（--build）"
  bash "$ROOT/scripts/dev.sh" start --build

  wait_for_gateway
}

target_count() {
  local count
  if [[ "$DURATION" =~ ^([0-9]+)s$ ]]; then
    count=$((RATE * "${BASH_REMATCH[1]}"))
  elif [[ "$DURATION" =~ ^([0-9]+)m$ ]]; then
    count=$((RATE * "${BASH_REMATCH[1]}" * 60))
  elif [[ "$DURATION" =~ ^([0-9]+)h$ ]]; then
    count=$((RATE * "${BASH_REMATCH[1]}" * 3600))
  else
    count=$((RATE * 180))
  fi
  echo "$count"
}

# 单笔冻结额：BUY 冻结 quote(USDT)，SELL 冻结 base(BTC)
freeze_per_order() {
  if [[ "$SIDE" == "BUY" ]]; then
    awk -v p="$PRICE" -v q="$QTY" 'BEGIN { printf "%.10f", p * q }'
  else
    printf '%s' "$QTY"
  fi
}

deposit_asset() {
  if [[ "$SIDE" == "BUY" ]]; then
    echo "USDT"
  else
    echo "BTC"
  fi
}

bench_deposit() {
  local count=$1
  local freeze asset amount_per_user orders_per_user total
  freeze="$(freeze_per_order)"
  asset="$(deposit_asset)"
  orders_per_user=$(( (count + NUM_USERS - 1) / NUM_USERS ))
  amount_per_user="$(awk -v o="$orders_per_user" -v f="$freeze" -v h="$DEPOSIT_HEADROOM" \
    'BEGIN { printf "%.8f", o * f * h }')"
  total="$(awk -v n="$NUM_USERS" -v a="$amount_per_user" 'BEGIN { printf "%.8f", n * a }')"

  log "压测充值: users=${NUM_USERS} (${USER_BASE}..$((USER_BASE + NUM_USERS - 1))), 总请求≈${count}, 每用户≈${orders_per_user} 笔"
  log "冻结/笔=${freeze} ${asset}, 充值/用户=${amount_per_user} ${asset}, 合计≈${total} ${asset}"

  local uid bid body resp code u
  bid=$((RUN_ID % 1000000))
  for ((u = 0; u < NUM_USERS; u++)); do
    uid=$((USER_BASE + u))
    body="$(printf '{"user_id":%s,"asset":"%s","business":"deposit","business_id":%s,"change":"%s"}' \
      "$uid" "$asset" "$((bid * 1000 + u))" "$amount_per_user")"
    resp="$(api_post_json "${BASE_URL}/v1/balances" "$body")"
    code="$(api_code "$resp")"
    if [[ "$code" != "0" ]]; then
      log "充值失败 user_id=${uid}: $resp"
      die "deposit failed for user_id=${uid}"
    fi
  done
  log "充值完成 (${NUM_USERS} 用户)"
}

bench_user_id() {
  local i=$1
  echo $((USER_BASE + (i - 1) % NUM_USERS))
}

ensure_bench_targets() {
  local gen="$ROOT/bin/bench-targets"
  if [[ ! -x "$gen" ]]; then
    log "building $gen ..."
    (cd "$ROOT" && go build -o bin/bench-targets ./cmd/bench-targets)
  fi
}

generate_targets() {
  local count=$1
  ensure_bench_targets
  log "生成 JSON targets ($RATE/s x $DURATION, ${count} 条, ${NUM_USERS} 用户轮询)..."
  "$ROOT/bin/bench-targets" \
    -out "$TARGETS" \
    -count "$count" \
    -base-url "$BASE_URL" \
    -token "$TOKEN" \
    -symbol "$SYMBOL" \
    -side "$SIDE" \
    -price "$PRICE" \
    -qty "$QTY" \
    -users "$NUM_USERS" \
    -user-base "$USER_BASE" \
    -run-id "$RUN_ID"
}

# 从 Prometheus 文本抓取无 label 的标量（如 gauge/counter）。
prom_scalar() {
  local url="$1" metric="$2"
  curl -sf "$url" 2>/dev/null | awk -v m="$metric" '$1 == m { print $2; exit }'
}

# 轮询 order_outbox_pending_count 直至 0 或超时；stdout: "<drain_seconds> <pending_final>"。
wait_outbox_drain() {
  if ! curl -sf "$ORDER_METRICS_URL" >/dev/null 2>&1; then
    log "Order metrics 不可用（${ORDER_METRICS_URL}），跳过 Outbox 排空等待"
    echo "-1 -1"
    return 0
  fi
  local pending=-1 start elapsed
  start="$(date +%s)"
  log "等待 Outbox 排空（超时 ${OUTBOX_DRAIN_TIMEOUT}s，间隔 ${OUTBOX_DRAIN_INTERVAL}s）..."
  while true; do
    pending="$(prom_scalar "$ORDER_METRICS_URL" "order_outbox_pending_count")"
    pending="${pending:--1}"
    elapsed=$(( $(date +%s) - start ))
    if [[ "$pending" == "0" ]]; then
      log "Outbox 已排空（${elapsed}s）"
      echo "$elapsed 0"
      return 0
    fi
    if (( elapsed >= OUTBOX_DRAIN_TIMEOUT )); then
      log "WARN: Outbox 排空超时（${OUTBOX_DRAIN_TIMEOUT}s），pending=${pending}"
      echo "$elapsed ${pending}"
      return 0
    fi
    if (( elapsed > 0 && elapsed % 30 == 0 )); then
      log "Outbox pending=${pending}，已等待 ${elapsed}s..."
    fi
    sleep "$OUTBOX_DRAIN_INTERVAL"
  done
}

# 等待 Matching 消费追平；stdout: "<drain_seconds> <lag_final> <processed_final>"。
wait_matching_drain() {
  local api_success="$1"
  if ! curl -sf "$METRICS_URL" >/dev/null 2>&1; then
    log "Matching metrics 不可用（${METRICS_URL}），跳过 Matching 追平等待"
    echo "-1 -1 -1"
    return 0
  fi
  if [[ ! "$api_success" =~ ^[0-9]+$ || "$api_success" -le 0 ]]; then
    echo "-1 -1 -1"
    return 0
  fi

  local target processed lag start elapsed
  target="$(awk -v a="$api_success" 'BEGIN { printf "%d", a }')"
  start="$(date +%s)"
  log "等待 Matching 追平（目标 processed>=${target}, lag<=${MATCHING_LAG_MAX}，超时 ${MATCHING_DRAIN_TIMEOUT}s）..."
  while true; do
    processed="$(prom_scalar "$METRICS_URL" "matching_commands_processed_total")"
    lag="$(prom_scalar "$METRICS_URL" "matching_kafka_lag")"
    processed="${processed:--1}"
    lag="${lag:--1}"
    elapsed=$(( $(date +%s) - start ))

    if [[ "$processed" != "-1" && "$lag" != "-1" ]]; then
      if awk -v p="$processed" -v t="$target" -v l="$lag" -v m="$MATCHING_LAG_MAX" \
        'BEGIN { exit ! (p + 0 >= t && l + 0 <= m) }'; then
        log "Matching 已追平（${elapsed}s，processed=${processed}，lag=${lag}）"
        echo "$elapsed $lag $processed"
        return 0
      fi
    fi

    if (( elapsed >= MATCHING_DRAIN_TIMEOUT )); then
      log "WARN: Matching 追平超时（${MATCHING_DRAIN_TIMEOUT}s），processed=${processed} lag=${lag} target=${target}"
      echo "$elapsed ${lag} ${processed}"
      return 0
    fi
    if (( elapsed > 0 && elapsed % 30 == 0 )); then
      log "Matching processed=${processed}/${target} lag=${lag}，已等待 ${elapsed}s..."
    fi
    sleep "$MATCHING_DRAIN_INTERVAL"
  done
}

parse_api_success_count() {
  local report="$1"
  local n
  n="$(grep -oE '201:[0-9]+' "$report" 2>/dev/null | head -1 | cut -d: -f2 || true)"
  if [[ -n "$n" ]]; then
    echo "$n"
    return 0
  fi
  # 兼容无 201 行时从 Success 与 total 推算
  awk '/Requests/ && /total/ {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^[0-9]+,$/) { gsub(/,/, "", $i); print $i; exit }
    }
  }' "$report" 2>/dev/null || echo "0"
}

parse_api_success_ratio() {
  awk '/Success/ {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /%$/) { gsub(/%/, "", $i); print $i; exit }
    }
  }' "$1" 2>/dev/null || echo "0"
}

# 解析 vegeta P99（支持 ms 与 s 单位）。
parse_api_latency_p99_ms() {
  awk '/Latencies/ {
    n = 0
    for (i = 1; i <= NF; i++) {
      if ($i ~ /[0-9](ms|s),?$/) {
        n++
        if (n == 4) {
          v = $i
          gsub(/,/, "", v)
          if (v ~ /ms$/) {
            sub(/ms$/, "", v)
            print v
          } else if (v ~ /s$/) {
            sub(/s$/, "", v)
            print v * 1000
          } else {
            print v
          }
          exit
        }
      }
    }
  }' "$1" 2>/dev/null || echo "0"
}

snapshot_metrics() {
  local url="$1" out="$2"
  if curl -sf "$url" >"$out" 2>/dev/null; then
    return 0
  fi
  : >"$out"
  return 1
}

write_latency_breakdown() {
  local report_dir="$1"
  local breakdown="$report_dir/latency-breakdown.txt"
  if ! go run "$ROOT/cmd/bench-report" \
    -l3-breakdown \
    -vegeta-report "$report_dir/report.txt" \
    -order-pre "$report_dir/order-metrics-pre.prom" \
    -order-post "$report_dir/order-metrics-post.prom" \
    -gateway-pre "$report_dir/gateway-metrics-pre.prom" \
    -gateway-post "$report_dir/gateway-metrics-post.prom" \
    >"$breakdown" 2>/dev/null; then
    log "WARN: 无法生成 latency-breakdown.txt（需 Gateway :9103 与 Order :9104 指标）"
    return 1
  fi
  log "延迟分解: $breakdown"
  cat "$breakdown" >&2
}

write_pipeline_summary() {
  local report="$1" summary="$2"
  local api_success matching_processed completion drain_sec pending_final
  local matching_drain_sec matching_lag_final
  local success_ratio p99_ms accept_sla delivery_sla

  api_success="$(parse_api_success_count "$report")"
  matching_processed="$(prom_scalar "$METRICS_URL" "matching_commands_processed_total")"
  matching_processed="${matching_processed:--1}"
  drain_sec="$3"
  pending_final="$4"
  matching_drain_sec="$5"
  matching_lag_final="$6"

  if [[ "$api_success" =~ ^[0-9]+$ && "$api_success" -gt 0 && "$matching_processed" != "-1" ]]; then
    completion="$(awk -v m="$matching_processed" -v a="$api_success" 'BEGIN { printf "%.4f", m / a }')"
  else
    completion="N/A"
  fi

  success_ratio="$(parse_api_success_ratio "$report")"
  p99_ms="$(parse_api_latency_p99_ms "$report")"
  accept_sla="$(awk -v s="$success_ratio" -v p="$p99_ms" 'BEGIN {
    if (s + 0 >= 99.9 && p + 0 <= 50) print "PASS"; else print "FAIL"
  }')"
  if [[ "$completion" == "N/A" ]]; then
    delivery_sla="N/A"
  else
    delivery_sla="$(awk -v c="$completion" 'BEGIN { if (c + 0 >= 0.99) print "PASS"; else print "FAIL" }')"
  fi

  {
    echo "api_success_count=${api_success}"
    echo "matching_processed_count=${matching_processed}"
    echo "pipeline_completion_rate=${completion}"
    echo "outbox_drain_seconds=${drain_sec}"
    echo "outbox_pending_final=${pending_final}"
    echo "matching_drain_seconds=${matching_drain_sec}"
    echo "matching_lag_final=${matching_lag_final}"
    echo "api_success_ratio=${success_ratio}%"
    echo "api_latency_p99_ms=${p99_ms}"
    echo "accept_sla=${accept_sla}"
    echo "delivery_sla=${delivery_sla}"
  } | tee "$summary"
}

if [[ "$RESET_ENV" == true ]]; then
  reset_and_start
else
  log "跳过环境重置（--no-reset-env）"
  if ! curl -sf "${BASE_URL}/v1/health" >/dev/null 2>&1; then
    die "Gateway 不可达（${BASE_URL}），请先 ./scripts/dev.sh start --build 或去掉 --no-reset-env"
  fi
fi

REPORT_DIR="$ROOT/reports/$(date +%Y%m%d-%H%M%S)-l3-e1"
mkdir -p "$REPORT_DIR"
TARGETS="$REPORT_DIR/targets.jsonl"
RESULTS="$REPORT_DIR/results.bin"
META="$REPORT_DIR/meta.txt"

count="$(target_count)"
freeze="$(freeze_per_order)"
asset="$(deposit_asset)"
orders_per_user=$(( (count + NUM_USERS - 1) / NUM_USERS ))
amount_per_user="$(awk -v o="$orders_per_user" -v f="$freeze" -v h="$DEPOSIT_HEADROOM" \
  'BEGIN { printf "%.8f", o * f * h }')"

{
  echo "run_id=${RUN_ID}"
  echo "rate=${RATE}"
  echo "duration=${DURATION}"
  echo "workers=${WORKERS}"
  echo "target_count=${count}"
  echo "symbol=${SYMBOL}"
  echo "side=${SIDE}"
  echo "price=${PRICE}"
  echo "quantity=${QTY}"
  echo "freeze_per_order=${freeze} ${asset}"
  echo "num_users=${NUM_USERS}"
  echo "user_base=${USER_BASE}"
  echo "orders_per_user≈${orders_per_user}"
  echo "deposit_per_user=${amount_per_user} ${asset}"
  echo "deposit_headroom=${DEPOSIT_HEADROOM}"
  echo "auto_deposit=${DO_DEPOSIT}"
  echo "reset_env=${RESET_ENV}"
} >"$META"

if [[ "$DO_DEPOSIT" == true ]]; then
  bench_deposit "$count"
else
  log "跳过充值（--no-deposit）；若出现大量 422 请检查余额或去掉该选项"
fi

generate_targets "$count"

log "采集压测前 metrics 快照（order/gateway/pg）..."
snapshot_metrics "$ORDER_METRICS_URL" "$REPORT_DIR/order-metrics-pre.prom" \
  || log "WARN: Order metrics 不可用（${ORDER_METRICS_URL}）"
snapshot_metrics "$GATEWAY_METRICS_URL" "$REPORT_DIR/gateway-metrics-pre.prom" \
  || log "WARN: Gateway metrics 不可用（${GATEWAY_METRICS_URL}），请 ./scripts/dev.sh start --build 重启 gateway"
if bash "$ROOT/scripts/bench/collect-pg-stats.sh" "$REPORT_DIR/pg-stats-pre.txt"; then
  log "PG 快照: $REPORT_DIR/pg-stats-pre.txt"
else
  log "WARN: PG stats 采集失败（无 psql/postgres 容器）"
fi

log "vegeta attack（-format=json）..."
vegeta attack -format=json -targets="$TARGETS" -rate="$RATE" -duration="$DURATION" -workers="$WORKERS" >"$RESULTS"
vegeta report -type=text "$RESULTS" | tee "$REPORT_DIR/report.txt"
vegeta report -type='hist[0,2ms,5ms,10ms,25ms,50ms,100ms,250ms,500ms,1s]' "$RESULTS" | tee "$REPORT_DIR/histogram.txt"

read -r OUTBOX_DRAIN_SEC OUTBOX_PENDING_FINAL <<<"$(wait_outbox_drain)"

API_SUCCESS_COUNT="$(parse_api_success_count "$REPORT_DIR/report.txt")"
read -r MATCHING_DRAIN_SEC MATCHING_LAG_FINAL MATCHING_PROCESSED_AT_DRAIN <<<"$(wait_matching_drain "$API_SUCCESS_COUNT")"

log "采集压测后 metrics 快照（order/gateway/pg）..."
snapshot_metrics "$ORDER_METRICS_URL" "$REPORT_DIR/order-metrics-post.prom" || true
snapshot_metrics "$GATEWAY_METRICS_URL" "$REPORT_DIR/gateway-metrics-post.prom" || true
if bash "$ROOT/scripts/bench/collect-pg-stats.sh" "$REPORT_DIR/pg-stats-post.txt"; then
  bash "$ROOT/scripts/bench/collect-pg-stats.sh" --delta \
    "$REPORT_DIR/pg-stats-pre.txt" "$REPORT_DIR/pg-stats-post.txt" \
    "$REPORT_DIR/pg-stats-delta.txt" 2>/dev/null || true
  log "PG 快照: $REPORT_DIR/pg-stats-post.txt"
  log "PG 差分: $REPORT_DIR/pg-stats-delta.txt"
else
  log "WARN: PG stats 采集失败"
fi

log "Order 指标摘要（:9104）..."
{
  curl -sf "$ORDER_METRICS_URL" 2>/dev/null | grep -E 'order_outbox|order_stuck|order_place_order|order_grpc_place_order' || true
} | tee "$REPORT_DIR/order-metrics.txt"

if curl -sf "$GATEWAY_METRICS_URL" >/dev/null 2>&1; then
  curl -sf "$GATEWAY_METRICS_URL" 2>/dev/null | grep -E 'gateway_place_order' | tee "$REPORT_DIR/gateway-metrics.txt" || true
else
  log "WARN: Gateway metrics 不可用（${GATEWAY_METRICS_URL}）"
fi

write_latency_breakdown "$REPORT_DIR" || true

log "Matching 指标快照（Outbox 排空后）..."
if curl -sf "$METRICS_URL" >/dev/null 2>&1; then
  go run "$ROOT/cmd/bench-report" -url "$METRICS_URL" | tee "$REPORT_DIR/matching-metrics.txt"
else
  log "WARN: Matching metrics 不可用（${METRICS_URL}），跳过 matching-metrics.txt"
fi

write_pipeline_summary "$REPORT_DIR/report.txt" "$REPORT_DIR/pipeline_summary.txt" \
  "$OUTBOX_DRAIN_SEC" "$OUTBOX_PENDING_FINAL" \
  "$MATCHING_DRAIN_SEC" "$MATCHING_LAG_FINAL"

log "报告: $REPORT_DIR/"
log "管道摘要: $REPORT_DIR/pipeline_summary.txt"
log "延迟分解: $REPORT_DIR/latency-breakdown.txt"
log "PG IO: $REPORT_DIR/pg-stats-delta.txt"
