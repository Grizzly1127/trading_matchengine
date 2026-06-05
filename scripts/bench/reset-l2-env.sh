#!/usr/bin/env bash
# L2 压测前轻量环境重置：仅清理 Matching 依赖的 WAL/快照与 Kafka（不碰 PostgreSQL/Redis）。
#
# 原因:
#   - 仅 restart matching 会从上轮 WAL 恢复 orderbook，且 Kafka offset/积压会影响 lag/TPS
#   - 累积 WAL 会拉长 fsync 尾延迟，使多轮 P99 不可比
#
# 用法:
#   ./scripts/bench/reset-l2-env.sh
#   ./scripts/bench/reset-l2-env.sh --no-stop   # matching 已停时
#
# 全量联调（Order/Gateway/DB）请用: ./scripts/reset-dev.sh -y --kafka-topics
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

STOP_MATCHING=true
KAFKA_TOPICS=(order.commands match.events trade.events)
# 与 configs/matching.json group_id 一致
KAFKA_CONSUMER_GROUPS=(matching-shard-0)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-stop) STOP_MATCHING=false; shift ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *) echo "unknown: $1" >&2; exit 1 ;;
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
    docker exec "$cid" /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --delete --group "$group" 2>/dev/null || true
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
  log "删除 WAL / 快照: data/wal data/snapshots"
  rm -rf "$ROOT/data/wal" "$ROOT/data/snapshots"
  mkdir -p "$ROOT/data"
}

main() {
  if $STOP_MATCHING; then
    log "停止 matching ..."
    bash "$ROOT/scripts/matching.sh" stop || true
  fi
  clean_matching_wal
  reset_kafka_topics
  log "L2 环境已重置（WAL + Kafka）；接下来 run-l2 会 restart matching。"
}

main "$@"
