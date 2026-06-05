#!/usr/bin/env bash
# L0 微基准：go test -bench 结果写入 reports/<timestamp>-l0/
# 不依赖、不启停 matching 进程（与 run-l2.sh 不同）。
#
# 用法:
#   ./scripts/bench/run-l0.sh
#   ./scripts/bench/run-l0.sh --smoke    # 短跑（CI）
#   ./scripts/bench/run-l0.sh --count 10
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

SMOKE=false
COUNT=5
BENCHTIME=""
PKGS="./internal/matching/engine/ ./pkg/skiplist/ ./pkg/wal/"

usage() {
  cat <<EOF
用法: $(basename "$0") [options]

  --smoke       -benchtime=50ms -count=1（CI 冒烟）
  --count N     基准重复次数（默认 5）
  --benchtime T 传给 go test -benchtime（如 3s）
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke) SMOKE=true; shift ;;
    --count) COUNT="$2"; shift 2 ;;
    --benchtime) BENCHTIME="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ "$SMOKE" == true ]]; then
  COUNT=1
  BENCHTIME="50ms"
fi

REPORT_DIR="$ROOT/reports/$(date +%Y%m%d-%H%M%S)-l0"
mkdir -p "$REPORT_DIR"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

{
  echo "layer=L0"
  echo "smoke=$SMOKE"
  echo "count=$COUNT"
  echo "benchtime=${BENCHTIME:-default}"
  echo "packages=$PKGS"
  git -C "$ROOT" rev-parse HEAD 2>/dev/null || true
  go version
  echo "goos=$(go env GOOS) goarch=$(go env GOARCH)"
  echo "gomaxprocs=${GOMAXPROCS:-$(nproc 2>/dev/null || echo 1)}"
} >"$REPORT_DIR/meta.txt"

ARGS=(-bench=. -benchmem -count="$COUNT")
if [[ -n "$BENCHTIME" ]]; then
  ARGS+=(-benchtime="$BENCHTIME")
fi

log "L0 微基准 → $REPORT_DIR"
log "运行: go test ${ARGS[*]} $PKGS"

# 终端仍打印；完整输出写入 bench.txt
set +e
go test "${ARGS[@]}" $PKGS 2>&1 | tee "$REPORT_DIR/bench.txt"
test_exit=${PIPESTATUS[0]}
set -e

if command -v benchstat >/dev/null 2>&1 && [[ "$COUNT" -gt 1 ]]; then
  benchstat "$REPORT_DIR/bench.txt" >"$REPORT_DIR/benchstat.txt" 2>/dev/null || true
  if [[ -s "$REPORT_DIR/benchstat.txt" ]]; then
    log "benchstat 摘要 → benchstat.txt"
  fi
fi

cat >"$REPORT_DIR/README.txt" <<EOF
L0 micro-benchmark report

主结果: bench.txt
元数据: meta.txt

对比两次优化:
  benchstat $REPORT_DIR/../<older>-l0/bench.txt $REPORT_DIR/bench.txt

安装 benchstat:
  go install golang.org/x/perf/cmd/benchstat@latest
EOF

if [[ "$test_exit" -ne 0 ]]; then
  log "go test 失败 (exit $test_exit)，见 $REPORT_DIR/bench.txt"
  exit "$test_exit"
fi

log "完成: $REPORT_DIR/"
exit 0
