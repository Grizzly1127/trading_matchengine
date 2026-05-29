#!/usr/bin/env bash
# 本地开发：一键启动 / 停止 / 查看所有微服务
#
# 用法:
#   ./scripts/dev.sh start   [--build] [--migrate] [--kafka-topics]
#   ./scripts/dev.sh stop
#   ./scripts/dev.sh restart [--build] [--migrate] [--kafka-topics]
#   ./scripts/dev.sh status
#
# 启动顺序（依赖由先到后）:
#   matching → order → marketdata → kline → push → gateway
#
# 停止顺序为上述逆序。
#
# 说明:
#   - WebSocket 由 Push 服务提供（默认 :8081/v1/ws），Gateway 仅 REST。
#   - --migrate / --kafka-topics 仅在 start|restart 时生效，失败则中止。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SERVICES_START=(matching order marketdata kline push gateway)

DO_BUILD=false
DO_MIGRATE=false
DO_KAFKA_TOPICS=false

usage() {
  cat <<EOF
用法: $(basename "$0") <command> [options]

命令:
  start     按依赖顺序启动全部服务
  stop      按逆序停止全部服务
  restart   stop 后 start
  status    查看各服务运行状态

选项:
  --build         各服务启动前编译对应二进制（等价于各 *.sh start --build）
  --migrate       启动前执行 scripts/migrate-up.sh（需 psql + PostgreSQL）
  --kafka-topics  启动前执行 scripts/kafka-create-topics.sh（需 docker kafka）

示例:
  $(basename "$0") start --build
  $(basename "$0") start --build --migrate
  $(basename "$0") status
  $(basename "$0") stop

WebSocket: ws://localhost:8081/v1/ws  （Push）
REST API:  http://localhost:8080       （Gateway）

联调用例: ./scripts/e2e-api.sh  （见 scripts/e2e-api.md）
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

parse_global_opts() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build) DO_BUILD=true; shift ;;
      --migrate) DO_MIGRATE=true; shift ;;
      --kafka-topics) DO_KAFKA_TOPICS=true; shift ;;
      *)
        return 0
        ;;
    esac
  done
}

service_script() {
  printf '%s/scripts/%s.sh' "$ROOT" "$1"
}

run_service() {
  local name=$1
  local cmd=$2
  shift 2
  local script
  script="$(service_script "$name")"
  if [[ ! -x "$script" ]]; then
    log "ERROR: missing or not executable: $script"
    exit 1
  fi
  local extra=()
  if $DO_BUILD && [[ "$cmd" == "start" || "$cmd" == "restart" ]]; then
    extra+=(--build)
  fi
  log ">>> $name: $cmd"
  "$script" "$cmd" "${extra[@]}" "$@"
}

services_stop_list() {
  printf '%s\n' "${SERVICES_START[@]}" | tac
}

preflight() {
  if $DO_MIGRATE; then
    log ">>> migrate-up"
    bash "$ROOT/scripts/migrate-up.sh"
  fi
  if $DO_KAFKA_TOPICS; then
    log ">>> kafka-create-topics"
    bash "$ROOT/scripts/kafka-create-topics.sh"
  fi
}

cmd_start() {
  parse_global_opts "$@"
  preflight
  local name
  for name in "${SERVICES_START[@]}"; do
    run_service "$name" start
  done
  log "all services started"
}

cmd_stop() {
  parse_global_opts "$@"
  local name
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    run_service "$name" stop || true
  done < <(services_stop_list)
  log "all services stopped"
}

cmd_restart() {
  parse_global_opts "$@"
  local name
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    run_service "$name" stop || true
  done < <(services_stop_list)
  preflight
  for name in "${SERVICES_START[@]}"; do
    run_service "$name" start
  done
  log "all services restarted"
}

cmd_status() {
  parse_global_opts "$@"
  local failed=0
  local name
  for name in "${SERVICES_START[@]}"; do
    log "--- $name ---"
    if run_service "$name" status; then
      :
    else
      ((failed++)) || true
    fi
  done
  if (( failed > 0 )); then
    log "$failed service(s) not running"
    return 1
  fi
  log "all services running"
  return 0
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
