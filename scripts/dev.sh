#!/usr/bin/env bash
# 本地开发：一键启动 / 停止 / 查看所有微服务
#
# 用法:
#   ./scripts/dev.sh start   [--build] [--migrate] [--kafka-topics] [--auth] [--jwt]
#   ./scripts/dev.sh stop
#   ./scripts/dev.sh restart [--build] [--migrate] [--kafka-topics] [--auth] [--jwt]
#   ./scripts/dev.sh status
#
# 启动顺序（依赖由先到后）:
#   matching → order → marketdata → kline → push → [auth] → gateway
#
# 选项:
#   --auth   额外启动 cmd/auth（:8090，签发服务 JWT）
#   --jwt    Gateway/Push 使用 configs/*.jwt-dev.json（需配合 --auth 或外部 IdP）
#
# 说明:
#   - WebSocket 由 Push 服务提供（默认 :8081/v1/ws），Gateway 仅 REST。
#   - --migrate / --kafka-topics 仅在 start|restart 时生效，失败则中止。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DO_BUILD=false
DO_MIGRATE=false
DO_KAFKA_TOPICS=false
DO_AUTH=false
DO_JWT=false

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
  --auth          启动轻量 JWT 签发服务（scripts/auth.sh，:8090）
  --jwt           Gateway/Push 使用 JWT 验签配置（需 --auth 或外部 IdP）

示例:
  $(basename "$0") start --build
  $(basename "$0") start --build --auth --jwt
  $(basename "$0") status
  $(basename "$0") stop

WebSocket: ws://localhost:8081/v1/ws  （Push）
REST API:  http://localhost:8080       （Gateway）
Auth 签发: http://localhost:8090       （仅 --auth）

联调:
  ./scripts/e2e-api.sh              # static token（默认）
  ./scripts/e2e-api.sh jwt          # 从 auth 取 JWT 后跑全流程

详见: scripts/e2e-api.md、docs/gateway-auth.md
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
      --auth) DO_AUTH=true; shift ;;
      --jwt) DO_JWT=true; shift ;;
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

# 启动列表（auth 在 gateway 之前）
services_start_list() {
  local names=(matching order marketdata kline push)
  if $DO_AUTH; then
    names+=(auth)
  fi
  names+=(gateway)
  printf '%s\n' "${names[@]}"
}

# 停止列表（逆序；始终尝试停 auth）
services_stop_list() {
  printf '%s\n' gateway auth push kline marketdata order matching
}

ensure_jwt_dev_configs() {
  local pair
  for pair in gateway:gateway push:push; do
    local svc="${pair%%:*}"
    local dst="$ROOT/configs/${svc}.jwt-dev.json"
    local ex="$ROOT/configs/${svc}.jwt-dev.json.example"
    if [[ ! -f "$dst" ]]; then
      if [[ ! -f "$ex" ]]; then
        log "ERROR: missing $ex (required for --jwt)" >&2
        exit 1
      fi
      cp "$ex" "$dst"
      log "created $dst from example"
    fi
  done
}

apply_jwt_env() {
  export GATEWAY_CONFIG="$ROOT/configs/gateway.jwt-dev.json"
  export PUSH_CONFIG="$ROOT/configs/push.jwt-dev.json"
  log "JWT mode: GATEWAY_CONFIG=$GATEWAY_CONFIG PUSH_CONFIG=$PUSH_CONFIG"
}

preflight() {
  if $DO_JWT; then
    ensure_jwt_dev_configs
    apply_jwt_env
  fi
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
  if $DO_JWT && ! $DO_AUTH; then
    log "WARN: --jwt without --auth; ensure external IdP or auth already running"
  fi
  preflight
  local name
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    run_service "$name" start
  done < <(services_start_list)
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
  if $DO_JWT && ! $DO_AUTH; then
    log "WARN: --jwt without --auth; ensure external IdP or auth already running"
  fi
  local name
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    run_service "$name" stop || true
  done < <(services_stop_list)
  preflight
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    run_service "$name" start
  done < <(services_start_list)
  log "all services restarted"
}

cmd_status() {
  parse_global_opts "$@"
  local failed=0
  local name
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    log "--- $name ---"
    if run_service "$name" status; then
      :
    else
      ((failed++)) || true
    fi
  done < <(services_start_list)
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
