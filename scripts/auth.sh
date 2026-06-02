#!/usr/bin/env bash
# Auth 签发服务进程管理（dev/staging 轻量 JWT）。
#
# 用法:
#   ./scripts/auth.sh start   [--build] [--config <path>]
#   ./scripts/auth.sh stop
#   ./scripts/auth.sh restart [--build] [--config <path>]
#   ./scripts/auth.sh status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${AUTH_BIN:-$ROOT/bin/auth}"
PID_FILE="${AUTH_PID_FILE:-$ROOT/run/auth.pid}"
STDOUT_LOG="${AUTH_STDOUT_LOG:-$ROOT/logs/auth.stdout}"
CONFIG="${AUTH_CONFIG:-$ROOT/configs/auth.json}"
STOP_TIMEOUT="${AUTH_STOP_TIMEOUT:-15}"

usage() {
  cat <<EOF
用法: $(basename "$0") <command> [options]

命令:
  start    后台启动 auth 签发服务（默认 :8090）
  stop     发送 SIGTERM，等待优雅退出
  restart  stop 后 start
  status   查看是否在运行

选项（start / restart）:
  --build          启动前执行 make build-auth
  --config <path>  指定配置文件

示例:
  $(basename "$0") start --build
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

ensure_dirs() {
  mkdir -p "$ROOT/run" "$ROOT/logs"
}

ensure_hs256_secret() {
  local secret="$ROOT/configs/auth-dev-hs256.secret"
  local example="$ROOT/configs/auth-dev-hs256.secret.example"
  if [[ ! -f "$secret" ]]; then
    if [[ ! -f "$example" ]]; then
      log "missing $example" >&2
      exit 1
    fi
    cp "$example" "$secret"
    log "created $secret from example"
  fi
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
  make -C "$ROOT" build-auth
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
    log "auth already running (pid $(read_pid), config $CONFIG)"
    exit 1
  fi

  if [[ ! -f "$CONFIG" ]]; then
    log "config not found: $CONFIG" >&2
    exit 1
  fi

  ensure_hs256_secret
  ensure_dirs
  if $do_build || [[ ! -x "$BIN" ]]; then
    do_build
  fi

  log "starting auth"
  log "  bin:    $BIN"
  log "  config: $CONFIG"
  log "  stdout: $STDOUT_LOG"

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
    log "auth is not running"
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
