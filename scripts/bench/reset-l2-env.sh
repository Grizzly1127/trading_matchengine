#!/usr/bin/env bash
# L2 压测前轻量环境重置：清理 Matching 依赖的 WAL/快照与 Kafka（默认不碰 PostgreSQL/Redis）。
#
# 原因:
#   - 仅 restart matching 会从上轮 WAL 恢复 orderbook，且 Kafka offset/积压会影响 lag/TPS
#   - 累积 WAL 会拉长 fsync 尾延迟，使多轮 P99 不可比
#   - L3 全链路压测后 Order DB 残留大量 PENDING 订单；matching 启动对账会因 gRPC 超限或 diff 失败
#
# 用法:
#   ./scripts/bench/reset-l2-env.sh
#   ./scripts/bench/reset-l2-env.sh --no-stop          # matching 已停时
#   ./scripts/bench/reset-l2-env.sh --with-db          # L3 后切 L2：清 PostgreSQL 并 migrate
#   ./scripts/bench/run-l2.sh --with-db ...            # 同上，经 run-l2 透传
#
# 全量联调（Order/Gateway/Redis 等）请用: ./scripts/reset-dev.sh -y --migrate --kafka-topics
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

DATABASE_URL="${DATABASE_URL:-postgres://trading:trading@localhost:5432/trading?sslmode=disable}"
STOP_MATCHING=true
WITH_DB=false
KAFKA_TOPICS=(order.commands match.events trade.events)
# 与 configs/matching.json group_id 一致
KAFKA_CONSUMER_GROUPS=(matching-shard-0)

usage() {
  cat <<EOF
用法: $(basename "$0") [options]

L2 压测前重置 WAL / 快照 / Kafka（可选清 PostgreSQL）。

选项:
  --with-db    清空 PostgreSQL public schema 并执行 migrate（L3 后切 L2 时推荐）
  --no-stop    不停止 matching（已手动 stop 时）
  -h, --help   显示本帮助

环境变量:
  DATABASE_URL  默认 postgres://trading:trading@localhost:5432/trading?sslmode=disable
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-db) WITH_DB=true; shift ;;
    --no-stop) STOP_MATCHING=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage >&2; exit 1 ;;
  esac
done

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
die() { log "ERROR: $*"; exit 1; }

kafka_cid() {
  docker ps -qf 'name=^kafka$' | head -1
}

reset_kafka_consumer_groups() {
  local cid="$1"
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

reset_kafka_topics() {
  local cid
  cid="$(kafka_cid)"
  if [[ -z "$cid" ]]; then
    die "kafka 容器未运行: docker compose -f deploy/docker-compose.yml up -d kafka"
  fi
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
    fi
  done
  sleep 2
  log "重建 Kafka topics ..."
  bash "$ROOT/scripts/kafka-create-topics.sh"
}

clean_matching_wal() {
  log "删除 WAL / 快照 / Event Outbox: data/wal data/snapshots data/event_outbox"
  rm -rf "$ROOT/data/wal" "$ROOT/data/snapshots" "$ROOT/data/event_outbox"
  mkdir -p "$ROOT/data"
}

reset_postgres() {
  log "清空 PostgreSQL (DROP SCHEMA public CASCADE)"
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

run_migrate() {
  log "执行数据库迁移 ..."
  bash "$ROOT/scripts/migrate-up.sh"
}

main() {
  if $STOP_MATCHING; then
    log "停止 matching ..."
    bash "$ROOT/scripts/matching.sh" stop || true
  fi
  clean_matching_wal
  if $WITH_DB; then
    reset_postgres
    run_migrate
  fi
  reset_kafka_topics
  if $WITH_DB; then
    log "L2 环境已重置（WAL + Kafka + PostgreSQL）；接下来 run-l2 会 restart matching。"
  else
    log "L2 环境已重置（WAL + Kafka）；接下来 run-l2 会 restart matching。"
  fi
}

main "$@"
