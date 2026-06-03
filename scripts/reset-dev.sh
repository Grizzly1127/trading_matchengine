#!/usr/bin/env bash
# 将本地开发环境恢复为「初始状态」：
#   - 停止全部微服务（matching / order / marketdata / kline / push / gateway）
#   - 删除 Matching WAL、快照（data/wal、data/snapshots）
#   - 清空 PostgreSQL（DROP SCHEMA public）
#   - Redis FLUSHALL
#   - Kafka 删除并重建 order.commands / match.events / trade.events
#   - 可选：清空 logs/、run/；可选：docker compose down -v 重置 PG 卷
#
# 用法:
#   ./scripts/reset-dev.sh              # 交互确认后执行
#   ./scripts/reset-dev.sh -y           # 跳过确认
#   ./scripts/reset-dev.sh -y --migrate # 重置后执行 migrations/*.up.sql
#   ./scripts/reset-dev.sh -y --docker-volumes  # 连同 docker pgdata 卷一并销毁
#
# 重置完成后启动:
#   docker compose -f deploy/docker-compose.yml up -d   # 若用了 --docker-volumes
#   ./scripts/reset-dev.sh -y --migrate --kafka-topics
#   ./scripts/dev.sh start --build
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT/deploy/docker-compose.yml}"
DATABASE_URL="${DATABASE_URL:-postgres://trading:trading@localhost:5432/trading?sslmode=disable}"
REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"
KAFKA_TOPICS=(order.commands match.events trade.events)
# 开发环境各服务 consumer group（__consumer_offsets 残留会导致“空 WAL 仍重放历史”）
KAFKA_CONSUMER_GROUPS=(matching-shard-0 order-service marketdata-service kline-service)

ASSUME_YES=false
KEEP_LOGS=false
DOCKER_VOLUMES=false
SKIP_STOP=false
DO_MIGRATE=false
DO_KAFKA_TOPICS=false

usage() {
  cat <<EOF
用法: $(basename "$0") [options]

将开发环境数据清零（数据库、Redis、Kafka、WAL/快照、PID 等）。

选项:
  -y, --yes              不询问，直接执行
  --keep-logs            保留 logs/ 目录内容
  --no-stop              不停止运行中的微服务（不推荐）
  --docker-volumes       docker compose down -v 后重新 up（销毁 Postgres 数据卷）
  --migrate              重置 DB 后执行 scripts/migrate-up.sh
  --kafka-topics         重置 Kafka 后执行 scripts/kafka-create-topics.sh
  -h, --help             显示本帮助

环境变量:
  DATABASE_URL           默认 postgres://trading:trading@localhost:5432/trading?sslmode=disable
  REDIS_ADDR             默认 localhost:6379（host:port）
  COMPOSE_FILE           默认 deploy/docker-compose.yml

示例:
  $(basename "$0") -y --migrate --kafka-topics
  $(basename "$0") -y --docker-volumes --migrate --kafka-topics
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  log "ERROR: $*"
  exit 1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -y|--yes) ASSUME_YES=true; shift ;;
      --keep-logs) KEEP_LOGS=true; shift ;;
      --docker-volumes) DOCKER_VOLUMES=true; shift ;;
      --no-stop) SKIP_STOP=true; shift ;;
      --migrate) DO_MIGRATE=true; shift ;;
      --kafka-topics) DO_KAFKA_TOPICS=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: $1 (try --help)" ;;
    esac
  done
}

confirm() {
  if $ASSUME_YES; then
    return 0
  fi
  cat <<EOF

即将清理以下内容（不可恢复）:
  - 微服务进程（dev.sh 管理的 6 个服务）
  - $ROOT/data/wal、$ROOT/data/snapshots（Matching WAL / 快照）
  - $ROOT/run/*.pid
  $( $KEEP_LOGS || echo "  - $ROOT/logs/*" )
  - PostgreSQL 库 trading 内 public schema 全部对象
  - Redis 当前 DB 全部 key（FLUSHALL）
  - Kafka topic: ${KAFKA_TOPICS[*]}
  - Kafka consumer groups: ${KAFKA_CONSUMER_GROUPS[*]}
$( $DOCKER_VOLUMES && echo "  - Docker 卷 pgdata（Postgres 整库数据卷）" )

EOF
  read -r -p "确认继续? [y/N] " ans
  [[ "${ans,,}" == "y" || "${ans,,}" == "yes" ]] || die "已取消"
}

stop_services() {
  if $SKIP_STOP; then
    log "skip: 不停止微服务 (--no-stop)"
    return 0
  fi
  log ">>> 停止微服务"
  if [[ -x "$ROOT/scripts/dev.sh" ]]; then
    bash "$ROOT/scripts/dev.sh" stop || true
  else
    local s
    for s in gateway push kline marketdata order matching; do
      local script="$ROOT/scripts/${s}.sh"
      if [[ -x "$script" ]]; then
        bash "$script" stop || true
      fi
    done
  fi
}

# 删除撮合引擎 WAL / 快照（与手动 rm -rf data/wal data/snapshots 等价）。
clean_matching_wal() {
  log ">>> 删除 Matching WAL / 快照: rm -rf data/wal data/snapshots"
  rm -rf "$ROOT/data/wal" "$ROOT/data/snapshots"
}

clean_local_files() {
  log ">>> 清理本地数据目录 / PID"
  clean_matching_wal
  mkdir -p "$ROOT/data" "$ROOT/run" "$ROOT/logs"
  rm -f "$ROOT"/run/*.pid

  if ! $KEEP_LOGS; then
    log ">>> 清理 logs/"
    find "$ROOT/logs" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
  else
    log "保留 logs/ (--keep-logs)"
  fi
}

docker_compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "$COMPOSE_FILE" "$@"
  else
    die "未找到 docker compose / docker-compose"
  fi
}

ensure_infra_up() {
  if $DOCKER_VOLUMES; then
    log ">>> docker compose down -v"
    docker_compose_cmd down -v
    log ">>> docker compose up -d"
    docker_compose_cmd up -d
    log "等待 Postgres / Kafka 就绪..."
    sleep 3
    return 0
  fi

  if ! docker_compose_cmd ps --status running 2>/dev/null | grep -q postgres; then
    log ">>> 启动基础设施 (postgres redis kafka)"
    docker_compose_cmd up -d
    sleep 2
  fi
}

reset_postgres() {
  log ">>> 重置 PostgreSQL (DROP SCHEMA public CASCADE)"
  if $DOCKER_VOLUMES; then
    log "已通过 docker volume 重建，跳过 DROP SCHEMA"
    return 0
  fi

  if command -v psql >/dev/null 2>&1; then
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO trading;
GRANT ALL ON SCHEMA public TO public;
SQL
    return 0
  fi

  local pg_cid
  pg_cid="$(docker ps -qf 'name=postgres' | head -1)"
  if [[ -z "$pg_cid" ]]; then
    pg_cid="$(docker ps -qf 'ancestor=postgres:16-alpine' | head -1)"
  fi
  if [[ -z "$pg_cid" ]]; then
    die "无法连接 Postgres：请安装 psql 或启动 docker compose postgres"
  fi

  docker exec -i "$pg_cid" psql -U trading -d trading -v ON_ERROR_STOP=1 <<'SQL'
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO trading;
GRANT ALL ON SCHEMA public TO public;
SQL
}

redis_host() {
  local host port
  host="${REDIS_ADDR%%:*}"
  port="${REDIS_ADDR##*:}"
  if [[ "$host" == "$port" ]]; then
    port=6379
  fi
  # 必须带换行：否则 `read` 读到 EOF 会返回非零，在 set -e 下会让脚本在 reset_kafka 之前提前退出
  printf '%s %s\n' "$host" "$port"
}

reset_redis() {
  log ">>> 清空 Redis (FLUSHALL)"
  local host port
  read -r host port < <(redis_host)

  if command -v redis-cli >/dev/null 2>&1; then
    if redis-cli -h "$host" -p "$port" FLUSHALL 2>/dev/null; then
      return 0
    fi
    log "本机 redis-cli 连接 $host:$port 失败，尝试 docker 容器"
  fi

  local redis_cid
  redis_cid="$(docker ps -qf 'ancestor=redis:7.2-alpine' | head -1)"
  if [[ -z "$redis_cid" ]]; then
    redis_cid="$(docker ps -qf 'name=redis' | head -1)"
  fi
  if [[ -z "$redis_cid" ]]; then
    die "无法执行 FLUSHALL：请安装 redis-cli 或启动 redis 容器"
  fi
  docker exec "$redis_cid" redis-cli FLUSHALL
}

kafka_cid() {
  docker ps -qf 'name=^kafka$' | head -1
}

reset_kafka() {
  log ">>> 重置 Kafka topics"
  if $DOCKER_VOLUMES; then
    log "Kafka 容器已重建，topic 为空"
    return 0
  fi

  local cid
  cid="$(kafka_cid)"
  if [[ -z "$cid" ]]; then
    die "kafka 容器未运行，请先: docker compose -f deploy/docker-compose.yml up -d kafka"
  fi

  # 先删 consumer group，再删 topic（topic 先删会导致 __consumer_offsets 元数据失效，--delete --group 报 GroupIdNotFound）
  reset_kafka_consumer_groups "$cid"

  local topic
  for topic in "${KAFKA_TOPICS[@]}"; do
    if docker exec "$cid" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --describe --topic "$topic" >/dev/null 2>&1; then
      log "删除 topic: $topic"
      docker exec "$cid" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete --topic "$topic"
    else
      log "topic 不存在，跳过: $topic"
    fi
  done

  # 等待 topic 删除完成（Kafka 删除为异步）
  sleep 2
}

reset_kafka_consumer_groups() {
  local cid="${1:-}"
  if [[ -z "$cid" ]]; then
    cid="$(kafka_cid)"
  fi
  if [[ -z "$cid" ]]; then
    return 0
  fi
  log ">>> 删除 Kafka consumer groups"
  local group
  for group in "${KAFKA_CONSUMER_GROUPS[@]}"; do
    if ! docker exec "$cid" /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --describe --group "$group" >/dev/null 2>&1; then
      log "consumer group 不存在，跳过: $group"
      continue
    fi
    log "删除 consumer group: $group"
    local out
    if out="$(docker exec "$cid" /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --delete --group "$group" 2>&1)"; then
      continue
    fi
    if echo "$out" | grep -q 'GroupIdNotFoundException'; then
      log "consumer group 已不存在（可忽略）: $group"
    else
      log "警告: 删除 consumer group 失败: $group — $out"
    fi
  done
}

run_migrate() {
  log ">>> 执行数据库迁移"
  bash "$ROOT/scripts/migrate-up.sh"
}

run_kafka_topics() {
  log ">>> 创建 Kafka topics"
  bash "$ROOT/scripts/kafka-create-topics.sh"
}

print_next_steps() {
  cat <<EOF

========================================
开发环境已重置为初始状态。
========================================

建议下一步:

  1. 确认基础设施:
     docker compose -f deploy/docker-compose.yml ps

  2. 若未带 --migrate / --kafka-topics，可手动:
     ./scripts/migrate-up.sh
     ./scripts/kafka-create-topics.sh

  3. 启动服务:
     ./scripts/dev.sh start --build

  注意: 勿只清库而不跑本脚本；本脚本会自动 rm -rf data/wal data/snapshots。

  4. 联调:
     ./scripts/e2e-api.sh
     # JWT: ./scripts/dev.sh start --build --auth --jwt && ./scripts/e2e-api.sh jwt

EOF
}

main() {
  parse_args "$@"
  confirm
  stop_services
  clean_local_files
  ensure_infra_up
  reset_postgres
  reset_redis
  reset_kafka
  $DO_MIGRATE && run_migrate
  # 删除 topic 后必须重建；与 --kafka-topics 无关，避免 Outbox 写入失败。
  run_kafka_topics
  # 若上文在 Redis/Kafka 步骤失败退出，WAL 已在 clean_local_files 删过；此处再删一次以防 --no-stop 等边缘情况。
  clean_matching_wal
  print_next_steps
}

main "$@"
