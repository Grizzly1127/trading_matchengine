#!/usr/bin/env bash
# 在 docker compose Kafka 启动后创建开发用 topic（单分区）。
# apache/kafka 镜像内脚本路径为 /opt/kafka/bin/
set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin/kafka-topics.sh}"

cid="$(docker ps -qf 'name=^kafka$' | head -1)"
if [ -z "$cid" ]; then
  echo "error: kafka container not running (docker compose up -d kafka)" >&2
  exit 1
fi

for topic in order.commands match.events trade.events index.price kline.raw; do
  docker exec "$cid" "$KAFKA_BIN" \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
  echo "ok: $topic"
done

echo "topics ready @ $BROKER"
