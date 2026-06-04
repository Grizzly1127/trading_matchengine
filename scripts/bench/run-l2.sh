#!/usr/bin/env bash
# L2 进程基准：向 order.commands 加压并采集 Matching 指标。
#
# 前置:
#   docker compose -f deploy/docker-compose.yml up -d
#   ./scripts/kafka-create-topics.sh
#   ./scripts/dev.sh start --build   # 或至少 matching 在跑
#
# 用法:
#   ./scripts/bench/run-l2.sh                    # 默认 m3, 5k/s, 5min
#   ./scripts/bench/run-l2.sh --scenario m2 --rate 3000 --duration 2m
#   ./scripts/bench/run-l2.sh --skip-seed        # 跳过卖盘 seed（已 seed 过）
#
# 环境变量见下方；报告写入 reports/<timestamp>/
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BROKERS="${BROKERS:-localhost:9092}"
TOPIC="${TOPIC:-order.commands}"
PARTITION="${PARTITION:-0}"
SYMBOL="${SYMBOL:-BTC-USDT}"
SCENARIO="${SCENARIO:-m3}"
RATE="${RATE:-2000}"
DURATION="${DURATION:-2m}"
WARMUP="${WARMUP:-500}"
SEED_DEPTH="${SEED_DEPTH:-500}"
SKIP_SEED=false
FULL_BENCH=false
RESET_L2_ENV=true
RESTART_MATCHING=true
COLLECT_PPROF=true
PROFILE_SEC=0
PROFILE_DELAY="${PROFILE_DELAY:-5}"
TRACE_SEC="${TRACE_SEC:-5}"
WAIT_LAG_TIMEOUT="${WAIT_LAG_TIMEOUT:-600}"
METRICS_URL="${METRICS_URL:-http://localhost:9101/metrics}"
PPROF_BASE="${PPROF_BASE:-http://localhost:9101}"

usage() {
  cat <<EOF
用法: $(basename "$0") [options]

选项:
  --scenario m1|m2|m3|m4   负载场景（默认 m3）
  --rate N                 目标发送速率/秒（默认 5000）
  --duration D             压测时长，如 5m、300s（默认 5m）
  --warmup N               预热条数（默认 10000）
  --seed-depth N           seed 卖盘深度（默认 500；--full 为 5000）
  --skip-seed              不执行 seed（m2/m3 前通常需要 seed）
  --full                   Phase4 全量：seed=5000 warmup=10000 rate=5000 duration=5m
  --no-wait-lag            正式压测前不等待 lag 回落（不推荐）
  --no-reset-env           不清理 WAL/Kafka（多轮 orderbook/积压会污染，仅调试）
  --no-restart             不重启 matching（累积直方图会跨轮污染，仅调试）
  --no-pprof               不在 load 窗口采集 cpu/block/trace
  --profile-sec N          CPU/block 采样秒数（默认 min(30, load时长-10)）
  --profile-delay N        load 开始后延迟 N 秒再采 pprof（默认 5）
  --brokers LIST           Kafka brokers（默认 localhost:9092）
  --partition N            分区（默认 0）
  --symbol SYM             交易对（默认 BTC-USDT）

验收参考（Phase 4）: 场景 m3 稳态 TPS>=5000, processing P99<=10ms
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario) SCENARIO="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --seed-depth) SEED_DEPTH="$2"; shift 2 ;;
    --skip-seed) SKIP_SEED=true; shift ;;
    --full) FULL_BENCH=true; shift ;;
    --no-wait-lag) WAIT_LAG_TIMEOUT=0; shift ;;
    --no-reset-env) RESET_L2_ENV=false; shift ;;
    --no-restart) RESTART_MATCHING=false; shift ;;
    --no-pprof) COLLECT_PPROF=false; shift ;;
    --profile-sec) PROFILE_SEC="$2"; shift 2 ;;
    --profile-delay) PROFILE_DELAY="$2"; shift 2 ;;
    --brokers) BROKERS="$2"; shift 2 ;;
    --partition) PARTITION="$2"; shift 2 ;;
    --symbol) SYMBOL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ "$FULL_BENCH" == true ]]; then
  RATE=5000
  DURATION=5m
  WARMUP=10000
  SEED_DEPTH=5000
fi

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# 将 1m / 90s / 5m 转为秒（供 TPS 计算）。
duration_to_sec() {
  local d="$1"
  if [[ "$d" =~ ^([0-9]+)m$ ]]; then
    echo $((${BASH_REMATCH[1]} * 60))
    return
  fi
  if [[ "$d" =~ ^([0-9]+)s$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return
  fi
  if [[ "$d" =~ ^[0-9]+$ ]]; then
    echo "$d"
    return
  fi
  echo 60
}

metric_processed_total() {
  awk '/matching_commands_processed_total:/{print $2}' "$1" | tail -1
}

metric_kafka_lag() {
  awk '/matching_kafka_lag:/{print $2}' "$1" | tail -1
}

wait_matching_lag() {
  local max_wait="$1"
  [[ "$max_wait" -le 0 ]] && return 0
  local elapsed=0
  while [[ "$elapsed" -lt "$max_wait" ]]; do
    local lag
    lag="$(curl -sf "$METRICS_URL" | awk '/^matching_kafka_lag /{print $2; exit}')"
    lag="${lag%%.*}"
    if [[ -z "$lag" || "$lag" -le 50 ]]; then
      log "matching_kafka_lag=${lag:-0}（可开始正式压测）"
      return 0
    fi
    log "等待 matching 消化 backlog，lag=$lag（${elapsed}/${max_wait}s）..."
    sleep 5
    elapsed=$((elapsed + 5))
  done
  die "lag 在 ${max_wait}s 内未降到 50 以下；请 make build && ./scripts/matching.sh restart 后重试"
}
die() { log "ERROR: $*"; exit 1; }

fetch_metrics_raw() {
  curl -sf "$METRICS_URL"
}

wait_metrics_up() {
  local i
  for i in $(seq 1 60); do
    if fetch_metrics_raw >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

if [[ "$RESET_L2_ENV" == true ]]; then
  chmod +x "$ROOT/scripts/bench/reset-l2-env.sh"
  log "L2 环境重置（WAL + Kafka，可比多轮压测）..."
  "$ROOT/scripts/bench/reset-l2-env.sh"
elif [[ "$RESTART_MATCHING" == true ]]; then
  log "跳过环境重置（--no-reset-env）；仅 stop matching 以便 restart ..."
  bash "$ROOT/scripts/matching.sh" stop || true
fi

if [[ "$RESTART_MATCHING" == true ]]; then
  log "重启 matching（清空进程内 Prometheus 直方图）..."
  "$ROOT/scripts/matching.sh" restart --build
  wait_metrics_up || die "restart 后 metrics 不可达 ($METRICS_URL)"
else
  if ! fetch_metrics_raw >/dev/null 2>&1; then
    die "Matching metrics 不可达 ($METRICS_URL)，请先启动 matching（./scripts/dev.sh start --build）"
  fi
  go build -o "$ROOT/bin/matching" ./cmd/matching 2>/dev/null || true
fi

if ! docker ps --format '{{.Names}}' | grep -qx kafka 2>/dev/null; then
  die "kafka 容器未运行，请先: docker compose -f deploy/docker-compose.yml up -d"
fi

REPORT_DIR="$ROOT/reports/$(date +%Y%m%d-%H%M%S)-l2-${SCENARIO}"
mkdir -p "$REPORT_DIR"

log "报告目录: $REPORT_DIR"
go build -o "$ROOT/bin/bench-producer" ./cmd/bench-producer
go build -o "$ROOT/bin/bench-report" ./cmd/bench-report

BP=( "$ROOT/bin/bench-producer"
  -brokers "$BROKERS" -topic "$TOPIC" -partition "$PARTITION" -symbol "$SYMBOL"
)

{
  echo "scenario=$SCENARIO rate=$RATE duration=$DURATION symbol=$SYMBOL partition=$PARTITION"
  echo "brokers=$BROKERS"
  git -C "$ROOT" rev-parse HEAD 2>/dev/null || true
  go version
} >"$REPORT_DIR/meta.txt"

log "采集压测前指标..."
"$ROOT/bin/bench-report" -url "$METRICS_URL" -label before | tee "$REPORT_DIR/metrics-before.txt"

# bash 算术不支持 1_000_000_000 下划线写法
NEXT_ORDER_ID=1000000000

case "$SCENARIO" in
  m2|m3)
    if [[ "$SKIP_SEED" != true ]]; then
      log "seed 卖盘 depth=$SEED_DEPTH (start-order-id=$NEXT_ORDER_ID) ..."
      "${BP[@]}" -scenario seed -seed-depth "$SEED_DEPTH" -start-order-id="$NEXT_ORDER_ID" \
        | tee "$REPORT_DIR/seed.log"
      NEXT_ORDER_ID=$((NEXT_ORDER_ID + SEED_DEPTH))
      wait_matching_lag "$WAIT_LAG_TIMEOUT"
    fi
    ;;
esac

if [[ "$WARMUP" -gt 0 ]]; then
  log "warmup $WARMUP (start-order-id=$NEXT_ORDER_ID) ..."
  "${BP[@]}" -scenario warmup -warmup "$WARMUP" -rate 1 -duration 1s \
    -start-order-id="$NEXT_ORDER_ID" | tee "$REPORT_DIR/warmup.log"
  NEXT_ORDER_ID=$((NEXT_ORDER_ID + WARMUP))
  wait_matching_lag "$WAIT_LAG_TIMEOUT"
fi

log "正式压测前指标（load 窗口起点）..."
fetch_metrics_raw | tee "$REPORT_DIR/metrics-pre-load.prom" >/dev/null
"$ROOT/bin/bench-report" -url "$METRICS_URL" -label pre_load | tee "$REPORT_DIR/metrics-pre-load.txt"
PROC_PRE_LOAD="$(metric_processed_total "$REPORT_DIR/metrics-pre-load.txt")"
PROC_PRE_LOAD="${PROC_PRE_LOAD%%.*}"

LOAD_SEC="$(duration_to_sec "$DURATION")"
if [[ "$PROFILE_SEC" -le 0 ]]; then
  PROFILE_SEC=$((LOAD_SEC - 10))
  [[ "$PROFILE_SEC" -gt 30 ]] && PROFILE_SEC=30
  [[ "$PROFILE_SEC" -lt 5 ]] && PROFILE_SEC=5
fi

PPROF_PID=""
if [[ "$COLLECT_PPROF" == true ]]; then
  chmod +x "$ROOT/scripts/bench/collect-pprof.sh"
  log "load 期间后台采集 pprof（cpu/block ${PROFILE_SEC}s, trace ${TRACE_SEC}s, delay ${PROFILE_DELAY}s）..."
  "$ROOT/scripts/bench/collect-pprof.sh" "$REPORT_DIR" "$PPROF_BASE" "$PROFILE_SEC" "$TRACE_SEC" "$PROFILE_DELAY" &
  PPROF_PID=$!
fi

log "开始压测 scenario=$SCENARIO rate=$RATE duration=$DURATION (${LOAD_SEC}s, start-order-id=$NEXT_ORDER_ID) ..."
LOAD_START_EPOCH="$(date +%s)"
"${BP[@]}" -scenario "$SCENARIO" -rate "$RATE" -duration="${DURATION}" \
  -warmup=0 -start-order-id="$NEXT_ORDER_ID" 2>&1 | tee "$REPORT_DIR/load.log"
LOAD_END_EPOCH="$(date +%s)"
LOAD_ELAPSED=$((LOAD_END_EPOCH - LOAD_START_EPOCH))
[[ "$LOAD_ELAPSED" -lt 1 ]] && LOAD_ELAPSED=1

if [[ -n "$PPROF_PID" ]]; then
  log "等待 pprof 采集结束..."
  wait "$PPROF_PID" 2>/dev/null || log "WARN: pprof 采集未正常结束"
fi

log "采集压测刚结束指标（load 窗口终点）..."
fetch_metrics_raw | tee "$REPORT_DIR/metrics-post-load.prom" >/dev/null
"$ROOT/bin/bench-report" -url "$METRICS_URL" -label post_load | tee "$REPORT_DIR/metrics-post-load.txt"

log "load 窗口差分指标（post.prom − pre.prom，验收请优先看此文件）..."
"$ROOT/bin/bench-report" \
  -delta-pre "$REPORT_DIR/metrics-pre-load.prom" \
  -delta-post "$REPORT_DIR/metrics-post-load.prom" \
  -label load_window | tee "$REPORT_DIR/metrics-load-window.txt"
PROC_POST_LOAD="$(metric_processed_total "$REPORT_DIR/metrics-post-load.txt")"
PROC_POST_LOAD="${PROC_POST_LOAD%%.*}"
LAG_POST_LOAD="$(metric_kafka_lag "$REPORT_DIR/metrics-post-load.txt")"
LAG_POST_LOAD="${LAG_POST_LOAD%%.*}"

LOAD_DELTA=$((PROC_POST_LOAD - PROC_PRE_LOAD))
MATCHING_TPS_LOAD=$((LOAD_DELTA / LOAD_ELAPSED))

log "等待 lag 回落（可选观察）..."
sleep 5
"$ROOT/bin/bench-report" -url "$METRICS_URL" -label after_wait | tee "$REPORT_DIR/metrics-after-wait.txt" || true

{
  echo "# L2 摘要（请优先看本文件，勿单独解读 tps-estimate 尾段采样）"
  echo "producer_rate_from_load_log: $(grep -E '^scenario=' "$REPORT_DIR/load.log" 2>/dev/null || echo n/a)"
  echo "load_window_seconds: ${LOAD_ELAPSED} (config ${LOAD_SEC}s)"
  echo "matching_processed_pre_load: ${PROC_PRE_LOAD}"
  echo "matching_processed_post_load: ${PROC_POST_LOAD}"
  echo "matching_processed_delta_in_load_window: ${LOAD_DELTA}"
  echo "matching_tps_during_load_window: ${MATCHING_TPS_LOAD} (/s)"
  echo "matching_kafka_lag_at_load_end: ${LAG_POST_LOAD}"
  echo "metrics_load_window: metrics-load-window.txt (直方图差分，优于累积 post_load)"
  echo "env_reset: ${RESET_L2_ENV} (WAL+Kafka；全量联调用 reset-dev.sh -y)"
  if [[ "$COLLECT_PPROF" == true ]]; then
    echo "pprof: cpu.prof block.prof trace.out (见 pprof-readme.txt)"
  fi
  if [[ -n "$LAG_POST_LOAD" && "$LAG_POST_LOAD" -le 50 ]]; then
    echo "steady_state_load: likely YES (lag<=50 at load end)"
  else
    echo "steady_state_load: likely NO (lag still high — consumer slower than producer)"
  fi
} | tee "$REPORT_DIR/summary.txt"

# 保留：压测完全结束后再等 10s 的采样（仅反映「扫尾」吞吐，不是 load 主窗口）
log "扫尾阶段 TPS（load 结束后 10s，通常低于 load 窗口）..."
{
  echo "# 注意：此文件为 load 结束后的 10s 扫尾采样，不能代表 --rate 压测段的 matching TPS"
  echo "# 真实压测段吞吐见 summary.txt 的 matching_tps_during_load_window"
  echo ""
} >"$REPORT_DIR/tps-estimate.txt"
METRICS_URL="$METRICS_URL" "$ROOT/scripts/bench/collect-metrics.sh" --delta 10 >>"$REPORT_DIR/tps-estimate.txt" 2>&1 || true

cat >"$REPORT_DIR/README.txt" <<EOF
L2 benchmark 报告

优先阅读:
  summary.txt              — matching_tps_during_load_window（压测段真实 TPS）
  metrics-load-window.txt  — load 段直方图差分（P99 验收优先此文件）
  metrics-pre-load.txt     — 正式 load 开始前（累积）
  metrics-post-load.txt    — 正式 load 刚结束（累积，跨轮勿比）
  metrics-*.prom           — 原始 Prometheus，可手动 bench-report -delta-pre/post
  pprof-readme.txt         — cpu/block/trace 查看方式（若未 --no-pprof）
  load.log                 — 生产者发送速率

每轮默认: reset-l2-env（WAL+Kafka）+ restart matching。
连跑不清理请加 --no-reset-env；不重启指标请加 --no-restart。L3/Order 联调前用 reset-dev.sh -y --kafka-topics。

勿单独用 tps-estimate.txt 判断压测 TPS（仅为 load 后 10s 扫尾）。

验收（architecture Phase 4）:
  - 场景 m3: rate>=5000, duration>=5m
  - matching_processing_latency_ms P99 <= 10ms
  - matching_commands_failed_total 不增长
EOF

log "完成。查看 $REPORT_DIR/"
