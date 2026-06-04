#!/usr/bin/env bash
# 在 L2 压测 load 窗口内采集 CPU / block / trace（matching :9101 已挂 pprof）。
#
# 用法:
#   ./scripts/bench/collect-pprof.sh <report_dir> [pprof_base] [profile_sec] [trace_sec] [delay_sec]
#
# 示例:
#   ./scripts/bench/collect-pprof.sh reports/xxx-l2-m3 http://localhost:9101 30 5 5
set -euo pipefail

REPORT_DIR="${1:?report_dir required}"
PPROF_BASE="${2:-http://localhost:9101}"
PROFILE_SEC="${3:-30}"
TRACE_SEC="${4:-5}"
DELAY_SEC="${5:-5}"

log() { printf '[%s] [pprof] %s\n' "$(date '+%H:%M:%S')" "$*"; }

if [[ "$DELAY_SEC" -gt 0 ]]; then
  log "等待 ${DELAY_SEC}s 后开采（避开 load 起步抖动）..."
  sleep "$DELAY_SEC"
fi

log "采集 CPU profile ${PROFILE_SEC}s ..."
curl -sf -o "$REPORT_DIR/cpu.prof" \
  "${PPROF_BASE}/debug/pprof/profile?seconds=${PROFILE_SEC}" || log "WARN: cpu.prof 失败"

log "采集 block profile ${PROFILE_SEC}s ..."
curl -sf -o "$REPORT_DIR/block.prof" \
  "${PPROF_BASE}/debug/pprof/block?seconds=${PROFILE_SEC}" || log "WARN: block.prof 失败"

log "采集 trace ${TRACE_SEC}s ..."
curl -sf -o "$REPORT_DIR/trace.out" \
  "${PPROF_BASE}/debug/pprof/trace?seconds=${TRACE_SEC}" || log "WARN: trace.out 失败"

cat >"$REPORT_DIR/pprof-readme.txt" <<EOF
# pprof 产物（load 窗口内采集）

| 文件 | 查看 |
|------|------|
| cpu.prof | go tool pprof -http=:0 $REPORT_DIR/cpu.prof |
| block.prof | go tool pprof -http=:0 $REPORT_DIR/block.prof |
| trace.out | go tool trace $REPORT_DIR/trace.out |

说明: block 需 matching 启动时开启采样（默认 1e6 ns）；WAL fsync 等 off-CPU 时间 block/trace 比 CPU 火焰图更有用。
EOF

log "完成 -> $REPORT_DIR/{cpu,block}.prof trace.out"
