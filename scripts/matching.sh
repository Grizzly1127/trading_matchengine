#!/usr/bin/env bash
# Matching 进程管理：启动 / 停止 / 重启 / 状态
#
# 用法:
#   ./scripts/matching.sh start   [--build] [--config <path>]
#   ./scripts/matching.sh stop
#   ./scripts/matching.sh restart [--build] [--config <path>]
#   ./scripts/matching.sh status
#
# 环境变量:
#   MATCHING_CONFIG  配置文件（默认 configs/matching.kafka.json）
#   MATCHING_BIN     可执行文件路径（默认 bin/matching）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${MATCHING_BIN:-$ROOT/bin/matching}"
PID_FILE="${MATCHING_PID_FILE:-$ROOT/run/matching.pid}"
STDOUT_LOG="${MATCHING_STDOUT_LOG:-$ROOT/logs/matching.stdout}"
CONFIG="${MATCHING_CONFIG:-$ROOT/configs/matching.kafka.json}"
STOP_TIMEOUT="${MATCHING_STOP_TIMEOUT:-30}"

usage() {
  cat <<EOF
用法: $(basename "$0") <command> [options]

命令:
  start    后台启动 matching
  stop     发送 SIGTERM，等待优雅退出（含 snapshot_on_exit）
  restart  stop 后 start
  status   查看是否在运行

选项（start / restart）:
  --build          启动前执行 make build
  --config <path>  指定配置文件

示例:
  MATCHING_CONFIG=$ROOT/configs/matching.json $(basename "$0") start
  $(basename "$0") start --build
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

ensure_dirs() {
  mkdir -p "$ROOT/run" "$ROOT/logs" "$ROOT/data"
}

read_pid() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  printf '%s' "$pid"
}

is_running() {
  local pid
  pid="$(read_pid)" || return 1
  kill -0 "$pid" 2>/dev/null
}

cleanup_stale_pid() {
  if [[ -f "$PID_FILE" ]] && ! is_running; then
    rm -f "$PID_FILE"
  fi
}

do_build() {
  log "building $BIN ..."
  make -C "$ROOT" build
}

cmd_start() {
  local do_build=false
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build) do_build=true; shift ;;
      --config)
        CONFIG="$2"
        shift 2
        ;;
      *)
        echo "unknown option: $1" >&2
        usage >&2
        exit 1
        ;;
    esac
  done

  cleanup_stale_pid
  if is_running; then
    log "matching already running (pid $(read_pid), config $CONFIG)"
    exit 1
  fi

  if [[ ! -f "$CONFIG" ]]; then
    log "config not found: $CONFIG" >&2
    exit 1
  fi

  ensure_dirs
  if $do_build || [[ ! -x "$BIN" ]]; then
    do_build
  fi

  log "starting matching"
  log "  bin:    $BIN"
  log "  config: $CONFIG"
  log "  stdout: $STDOUT_LOG"

  # 业务日志由 configs 中 log.file 写入；此处仅捕获进程额外 stdout/stderr
  nohup "$BIN" -config "$CONFIG" >>"$STDOUT_LOG" 2>&1 &
  echo $! >"$PID_FILE"
  sleep 0.3
  if ! is_running; then
    log "failed to start, see $STDOUT_LOG" >&2
    rm -f "$PID_FILE"
    exit 1
  fi
  log "started pid $(read_pid)"
}

cmd_stop() {
  cleanup_stale_pid
  if ! is_running; then
    log "matching is not running"
    rm -f "$PID_FILE"
    return 0
  fi

  local pid
  pid="$(read_pid)"
  log "stopping pid $pid (SIGTERM, timeout ${STOP_TIMEOUT}s) ..."
  kill -TERM "$pid" 2>/dev/null || true

  local i=0
  while is_running; do
    if (( i >= STOP_TIMEOUT )); then
      log "graceful stop timed out, sending SIGKILL"
      kill -KILL "$pid" 2>/dev/null || true
      sleep 0.5
      break
    fi
    sleep 1
    ((i++)) || true
  done

  rm -f "$PID_FILE"
  log "stopped"
}

cmd_restart() {
  cmd_stop
  cmd_start "$@"
}

cmd_status() {
  cleanup_stale_pid
  if is_running; then
    log "running  pid=$(read_pid)"
    log "  bin:    $BIN"
    log "  config: $CONFIG"
    log "  pidfile:$PID_FILE"
    return 0
  fi
  log "not running"
  return 1
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    start) cmd_start "$@" ;;
    stop) cmd_stop "$@" ;;
    restart) cmd_restart "$@" ;;
    status) cmd_status "$@" ;;
    -h|--help|help|"")
      usage
      [[ -z "$cmd" || "$cmd" == "-h" || "$cmd" == "--help" || "$cmd" == "help" ]] && exit 0 || exit 1
      ;;
    *)
      echo "unknown command: $cmd" >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
