# Kafka 接入设计（第 3 步 3.2）

**日期**: 2026-05-22  
**状态**: 已批准

## 目标

在保留 3.1 本地 JSONL 模式的前提下，接入 Kafka：消费 `order.commands`，WAL fsync 成功后提交 offset，发布 `match.events` 与 `trade.events`。

## 架构

- **客户端**: `segmentio/kafka-go`
- **模式切换**: `config.kafka.enabled=true` 时跑 Kafka 循环；否则保持 JSONL
- **包结构**:
  - `pkg/kafka` — Reader/Writer 封装与测试用 fake
  - `internal/matching/consumer` — 解码命令、调用 `recovery.Engine`、提交 offset
  - `internal/matching/publisher` — 根据撮合结果构建并发布事件
- **消息格式**: Protobuf `OrderCommandEnvelope`（oneof new_order / cancel_order）；`MatchEvent` / `TradeEvent`

## 数据流

1. Consumer 拉取 `order.commands` 消息
2. 解析 envelope，写入 `kafka_partition` / `kafka_offset` 到命令
3. `recovery.ApplyNewOrder` / `ApplyCancel`（内部先 WAL fsync）
4. Publisher 发布 `match.events`、`trade.events`
5. 手动 `Commit` Kafka offset

## 事件语义（最小集）

| 场景 | match.events | trade.events |
|------|--------------|--------------|
| 新单受理（非重复） | `ORDER_ACCEPTED` | — |
| 每笔成交 | 吃单方/挂单方 `FILLED` 或 `PARTIAL` | `TradeEvent` |
| 撤单成功 | `ORDER_CANCELED` | — |

## 恢复

- 启动时扫描 WAL 取各 partition 最大 `kafka_offset`，Consumer seek 到 `offset+1`
- WAL 回放不重复发布 Kafka 事件（仅恢复内存）

## 配置

```json
"kafka": {
  "enabled": true,
  "brokers": ["localhost:9092"],
  "group_id": "matching-shard-0",
  "command_topic": "order.commands",
  "match_topic": "match.events",
  "trade_topic": "trade.events",
  "partition": 0
}
```

## 不在本阶段

- 多 partition / 多分片路由
- Order Service Outbox
- `OrderRejected` 与对账 scheduler
