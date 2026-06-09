# Matching 事件异步发布 + Outbox 设计方案

**版本**: 1.0  
**日期**: 2026-06-07  
**状态**: 已实现（P0–P2）；见 `pkg/eventoutbox`、`internal/matching/eventrelay`  
**关联**: [architecture-spec.md](./architecture-spec.md) §4.1 / §5 · [l2-optimization-roadmap.md](./l2-optimization-roadmap.md) §4.2 / §6 选项 B · [l3-optimization-roadmap.md](./l3-optimization-roadmap.md)

---

## 1. 背景与目标

### 1.1 现状

组提交（WAL batch fsync）+ `PublishBatch` 之后，L2 报告 `20260607-003907` 仍显示：

- `matching_publish_latency_ms` P99 ≈ **24 ms / 批**
- 稳态 TPS ≈ **4373/s**（目标 5000/s），`matching_kafka_lag` 在高压下上涨

热路径仍为：

```text
Stage → WAL Sync → Apply → 同步 Kafka Publish → Commit order.commands offset
```

Kafka 墙钟阻塞 consumer 单 goroutine，是单 symbol 吞吐的主矛盾之一。

### 1.2 目标

| 目标 | 说明 |
|------|------|
| **缩短热路径** | `processing` 墙钟不再包含 Kafka RTT + broker ack |
| **不丢事件** | 命令已撮合产生的 `match.events` / `trade.events` **可恢复、可重投** |
| **最终一致** | 与 spec L5/L6 一致；不承诺跨服务强一致 |
| **幂等** | 重复投递对 Order / MarketData 安全 |
| **遵守热路径 SLA** | 单 symbol 单 goroutine；热路径禁止 PostgreSQL / Redis |
| **可观测** | pending、relay lag、失败可告警 |

### 1.3 非目标

- 不改为「先改内存后写 WAL」
- 不在热路径引入跨服务 2PC / XA
- 不保证「撮合完成 ⟺ Kafka 立即可见」（允许 relay 延迟）
- 不替代 Order Service 已有 `order_outbox`（命令投递方向不变）

---

## 2. 设计原则（安全前提）

以下不变式**必须**在实现与评审中可验证；违反任一条视为数据安全风险。

### 2.1 核心不变式

| ID | 不变式 |
|----|--------|
| **I1** | **先命令 WAL durable，再改 orderbook**（与现 spec 一致） |
| **I2** | **事件写入本地 Event Outbox 并 durable 之后**，才允许 commit 对应 `order.commands` 的 Kafka offset |
| **I3** | **禁止**在事件未 durable 时 commit offset（否则崩溃会永久丢事件） |
| **I4** | Relay 对 Kafka 为 **至少一次**；重复消息由下游幂等吸收 |
| **I5** | 单 symbol 命令与事件 **保序**：同一 symbol 的 outbox 记录按 `outbox_seq` 严格递增投递 |
| **I6** | 热路径 **禁止** PostgreSQL / Redis；Outbox 仅本地磁盘 |
| **I7** | 崩溃恢复后：orderbook 状态与「已 commit 的 Kafka offset」一致；未发布事件可从 Outbox 继续投递 |

### 2.2 与 Order Outbox 的对称性

| 方向 | 存储 | 热路径 | 冷路径 |
|------|------|--------|--------|
| Order → `order.commands` | PostgreSQL `order_outbox` | API 事务内 INSERT | `outbox.Relay` |
| Matching → `match.events` / `trade.events` | **本地 Event Outbox** | consumer 批内 append + fsync | `eventrelay.Relay` |

思想一致：**业务状态已提交（命令已 apply）与消息投递解耦**，靠 durable outbox + relay 保证至少一次。

---

## 3. 一致性模型变更

### 3.1 分层（建议在 architecture-spec §4.1 增补）

| 层级 | 范围 | 保证 | 实现 |
|------|------|------|------|
| L4 | Matching 进程内命令 | **命令级原子** | 命令 WAL fsync → apply |
| **L4b**（新增） | Matching 进程内事件 | **事件至少落盘一次** | Event Outbox fsync |
| L4c | `order.commands` offset | **不超过已落盘事件边界** | offset commit 规则（§5） |
| L5 | Matching → Order | **最终一致** | Relay + 幂等消费 |
| L6 | 全链路用户语义 | **最终一致** | 状态机 + 补偿 + 对账 |

**不保证**：撮合 apply 完成与 Kafka 上可见事件在同一时刻。

### 3.2 与现「同步 Publish 后 commit」的差异

| 维度 | 同步 Publish（现状） | Event Outbox（目标） |
|------|----------------------|----------------------|
| offset commit 条件 | Kafka publish 成功 | **Outbox durable** |
| Kafka 慢的影响 | 拖住撮合 consumer | 仅增大 `event_outbox_pending` |
| 崩溃丢事件风险 | 低（未 commit 则重投命令） | 低（**若 I2/I3 正确**） |
| 端到端可见延迟 | 较短 | 略增（多一跳 relay） |

---

## 4. 总体架构

```text
┌─────────────────────────────────────────────────────────────────┐
│ Matching 进程（单 shard / 单 consumer goroutine 热路径）           │
│                                                                 │
│  order.commands ──► Consumer.ProcessBatch                       │
│                         │                                       │
│                         ├─► Command WAL (stage + Sync)          │
│                         ├─► Engine.Apply (撮合)                  │
│                         ├─► Event Outbox Append + Sync          │
│                         └─► Commit order.commands offset        │
│                                                                 │
│  Event Relay（独立 goroutine，可配置多 worker，按 symbol 保序）      │
│       │                                                         │
│       ├─► Claim unpublished from Event Outbox                   │
│       ├─► Kafka WriteBatch (match.events / trade.events)        │
│       └─► MarkPublished (outbox_seq)                            │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
   Order / MarketData / Kline（幂等消费，最终一致）
```

---

## 5. 存储设计：本地 Event Outbox

### 5.1 路径与格式

```text
data/event_outbox/{shard_id}/
  outbox_000001.log      # 顺序追加段文件（与 WAL 分段策略类似）
  meta.pb                # 元数据：last_outbox_seq, last_published_seq, ...
```

**帧格式**（与 `pkg/wal` 对齐，便于复用编解码与 CRC）：

```text
[4 bytes len][outbox_seq uint64][wal_seq uint64][ts int64][flags byte][topic_id byte]
[kafka_partition uint32][kafka_offset uint64][partition_key len+bytes][payload len+bytes][crc32]
```

| 字段 | 说明 |
|------|------|
| `outbox_seq` | shard 内单调递增，Relay 游标 |
| `wal_seq` | 产生该事件的命令 WAL seq（溯源、对账） |
| `topic_id` | `1=match.events`, `2=trade.events` |
| `partition_key` | symbol |
| `payload` | 已序列化的 `MatchEvent` 或 `TradeEvent` protobuf |

**不采用 PostgreSQL**：遵守热路径 SLA；本地追加写 + `fdatasync` 与命令 WAL 同级可靠性（同盘或同 tmpfs 策略）。

### 5.2 组提交

Event Outbox Writer 支持独立组提交参数（可与命令 WAL 相同配置，默认值建议与 `wal_group_commit` 对齐）：

- `sync_every_records` / `sync_interval_ms`
- **每个 `ProcessBatch` 结束前**：该批产生的全部 outbox 记录必须已 `Sync()`，方可 commit 本批涉及的 Kafka offset（I2）

可选优化（Phase 2+）：命令 WAL 与 Event Outbox **同一 `fdatasync` 边界**（两个文件 `sync` 同一批次）——实现复杂，首版可分开两次 sync，仍显著优于同步 Kafka。

### 5.3 压缩与回收

- 段文件满 **100MB** 或 **10 分钟**滚动（与命令 WAL 一致）
- `last_published_outbox_seq` 之前的段，在快照 + manifest 更新后可删除（类似 WAL 回收）
- 保留最近 N 个 outbox 段用于审计（可配置，默认 3）

---

## 6. 热路径流程

### 6.1 ProcessBatch（组提交路径）

```text
1. FOR each msg in batch:
     decode → Engine.StageNewOrder / StageCancel   # 仅写命令 WAL 缓冲
2. CommandWAL.Sync()
3. FOR each staged item IN ORDER:
     apply → trades + match events（内存）
     IF duplicate / readOnly: 仍记录 outcome，可能无 outbound
4. BuildOutboundBatch(outcomes) → []EventRecord
5. EventOutbox.AppendBatch(records)                # 写缓冲
6. EventOutbox.Sync()                              # durable（I2）
7. FOR each msg in batch:
     Kafka.Commit(order.commands, offset)          # 仅步骤 6 成功后
8. （异步）Relay 由独立 goroutine 拉取，不在此等待
```

**单条路径**（`sync_every_records=1` 或未启用组提交）：逻辑相同，批次大小为 1。

### 6.2 与命令 WAL 的顺序关系

```text
命令 WAL record  ──sync──►  apply  ──►  event outbox record  ──sync──►  commit offset
     ^ durable              ^ 内存           ^ durable
```

- apply **永远**在命令 WAL `Sync()` 之后
- event outbox `Sync()` **永远**在 apply 之后、offset commit 之前
- **禁止** apply 后直接将事件只放内存队列而不落盘就 commit offset

### 6.3 无事件命令

| 情况 | Outbox | offset |
|------|--------|--------|
| 重复 `order_id`（幂等跳过） | 不写 | 可 commit（命令已「处理」） |
| symbol 只读拒单 | 不写 | 可 commit（避免阻塞 partition） |
| Cancel 成功无 match 事件 | 写 `OrderCanceled` 等 | 正常流程 |
| 纯挂单无成交 | 写 `OrderAccepted` | 正常流程 |

---

## 7. Event Relay（冷路径）

### 7.1 职责

参考 `internal/order/outbox/relay.go`，实现 `internal/matching/eventrelay/`：

| 步骤 | 行为 |
|------|------|
| Claim | 按 `outbox_seq` 升序读取 `published_seq < outbox_seq` 的记录（`FOR UPDATE SKIP LOCKED` 的本地等价：单进程用游标 + 内存锁） |
| Group | 按 `(topic, partition_key)` 分组，`WriteBatch` |
| Publish | `required_acks=all`（生产）；压测可配置 `one` |
| Mark | Kafka 成功后将 `published_seq` 推进（批量） |
| Retry | 失败 `retry_count++`，指数退避；超 `max_retry` 告警 |

### 7.2 保序

- **同一 symbol** 的 outbox 记录必须按 `outbox_seq` 顺序进入 Kafka（同一 partition）
- 不同 symbol 可并行（多 worker 时按 `partition_key` 哈希分 worker，每 worker 内保序）

### 7.3 与 match / trade 双 topic

- 单条 outbox 记录只对应 **一个 topic 的一条消息**
- 一个命令可产生多条 outbox 记录（1×Accepted + N×Trade + M×Fill），共享同一 `wal_seq`，`outbox_seq` 递增
- Relay 按 `outbox_seq` 全局顺序 claim，分组 batch 时不得打乱同 symbol 相对顺序

---

## 8. Kafka offset commit 规则

### 8.1 正式定义

对 `order.commands` 分区中 offset **O** 的命令消息 **M**：

```text
Commit(O) 当且仅当：
  (1) M 对应命令已写入命令 WAL 且已 fsync；
  (2) M 已 apply（含幂等跳过、只读跳过）；
  (3) M 产生的所有 Event Outbox 记录已 fsync；
  (4) 若 M 无 outbound，(3)  vacuously 成立。
```

### 8.2 与 `enable.auto.commit`

保持 **`enable.auto.commit=false`**，仅由 consumer 在 `ProcessBatch` 末尾显式 commit（与现实现一致）。

### 8.3 崩溃场景

| 崩溃点 | 命令 WAL | orderbook | Outbox | offset | 恢复 |
|--------|----------|-----------|--------|--------|------|
| A: Sync 前 | 无 durable | 未改 | 无 | 未 commit | 重投 M，幂等 |
| B: Sync 后、apply 前 | durable | 未改 | 无 | 未 commit | 重放 WAL apply |
| C: apply 后、outbox Sync 前 | durable | 已改 | 无/未 durable | 未 commit | 重投 M 或 WAL 重放；**order_id 幂等** |
| D: outbox Sync 后、commit 前 | durable | 已改 | durable | 未 commit | 重投 M；幂等跳过 apply；**relay 发事件** |
| E: commit 后、relay 前 | durable | 已改 | durable | committed | **relay 继续发**，不重新消费 M |

**C 点**依赖 `order_id` 幂等：重复 apply 不得重复撮合。现 `seen` 集合 + WAL 恢复已保证。

---

## 9. 幂等性

### 9.1 Matching 内部

| 对象 | 机制 |
|------|------|
| 命令 | `order_id` / `seen` map；WAL 重放跳过已见 |
| Outbox 记录 | `outbox_seq` 全局唯一；append-only |
| Relay | `published_seq` 游标；已 mark 的不重发（除非显式 reset 运维） |

### 9.2 Kafka 消息

| 字段 | 用途 |
|------|------|
| Message Key | `symbol`（保序） |
| Headers（建议新增） | `outbox_seq`, `wal_seq`, `shard_id` |
| `MatchEvent` | `order_id` + `event_type` + `wal_seq` |
| `TradeEvent` | `trade_id`（`DeriveTradeID(wal_seq, maker, taker)`，见 `docs/matching-api.md` §5.3） |

### 9.3 下游

| 服务 | 幂等键 |
|------|--------|
| Order | `trade_id` 唯一索引；match 事件按 `order_id`+状态机 CAS |
| MarketData / Kline | `trade_id` / 事件序号去重 |

**重复投递安全**：Relay 重试、Kafka 重复、消费者重启均可接受。

---

## 10. 恢复与启动

### 10.1 启动顺序

```text
1. 加载快照 + 命令 WAL 回放 → 恢复 orderbook（与现 §5.5 一致）
2. 加载 Event Outbox meta：last_published_outbox_seq
3. 启动 Event Relay（追平未发布事件）
4. （可选）等待 pending < 阈值 或 超时后继续
5. 启动 order.commands consumer，seek 到 manifest.recovered_kafka_offset + 1
6. Recovery Verify（§5.6，与现一致）
```

### 10.2 修订 architecture-spec §5.5 回放说明

现文档：「WAL 回放不重新发布 Kafka 事件」。**修订为**：

- 命令 WAL 回放：**仅**恢复 orderbook，不向 Kafka 发事件
- 事件恢复：由 **Event Outbox + Relay** 负责；若 outbox 已有记录则 relay，**禁止**从 apply 重复推导发布（避免双通道）

### 10.3 ShardManifest 扩展

在 `matching.v1.ShardManifest` 增加：

```protobuf
uint64 last_published_outbox_seq = 5;
uint64 last_committed_kafka_offset = 6;  // order.commands 已 commit 的最大 offset
```

快照时一并持久化 `manifest.pb`。

---

## 11. 故障矩阵（Matching 事件方向）

| 故障 | orderbook | Event Outbox | Kafka 事件 | 恢复 |
|------|-----------|--------------|------------|------|
| apply 后进程崩溃，outbox 未 sync | 已从 WAL 可恢复 | 可能缺失 | 未发 | offset 未 commit → 重投；或 WAL 重放 |
| outbox sync 后 relay 未发 | 一致 | 有记录 | 未发 | Relay 重试 |
| relay 成功，mark 前崩溃 | 一致 | 有记录 | 可能已发 | 重启 relay：Kafka 幂等 + 下游去重；或 claim 前查重 |
| relay 成功且 mark | 一致 | 已 mark | 已发 | 正常 |
| 磁盘损坏 outbox CRC 失败 | — | — | — | 从命令 WAL **仅恢复 orderbook**；**人工对账** §5.6；禁止静默继续 |

---

## 12. 可观测性与 SLO

### 12.1 Prometheus 指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `matching_event_outbox_pending_count` | Gauge | 未 `published` 的 outbox 条数 |
| `matching_event_outbox_append_latency_ms` | Histogram | 热路径 append+sync |
| `matching_event_relay_batch_size` | Histogram | 每批 claim 条数 |
| `matching_event_relay_dispatch_latency_ms` | Histogram | claim → Kafka ack |
| `matching_event_relay_published_total` | Counter | 已发布条数 |
| `matching_event_publish_lag_seconds` | Gauge | `now - 最老未发布记录的 ts` |
| `matching_processing_latency_ms` | Histogram | **不含** Kafka（仅 stage+sync+apply+outbox） |

### 12.2 告警（建议）

| 条件 | 动作 |
|------|------|
| `event_outbox_pending_count` > 阈值持续 60s | 扩容 relay / 查 Kafka |
| `event_publish_lag_seconds` > 30s | 告警；L5 滞后 |
| relay `max_retry` 耗尽 | 死信 + 人工 |
| pending 涨而 processing TPS 正常 | Kafka 或 relay 瓶颈 |

---

## 13. 配置项（草案）

`configs/matching.json` 新增：

```json
{
  "event_outbox": {
    "enabled": true,
    "data_dir": "data/event_outbox",
    "sync_every_records": 64,
    "sync_interval_ms": 5
  },
  "event_relay": {
    "poll_interval_ms": 2,
    "batch_size": 256,
    "max_retry": 20,
    "workers": 2,
    "required_acks": "all"
  }
}
```

`event_outbox.enabled=false` 时回退同步 Publish（兼容压测对比与回滚）。

---

## 14. 实现阶段

| 阶段 | 内容 | 验收 |
|------|------|------|
| **P0** | `pkg/eventoutbox` 追加写 + Sync + CRC；单元测试崩溃恢复 | 不变式 I1–I3 单测 |
| **P1** | `ProcessBatch` 集成；关闭同步 `PublishBatch` | L2 `processing` P99 下降 |
| **P2** | `eventrelay` + 指标 | `event_outbox_pending` 可追平 |
| **P3** | manifest / 启动恢复 / 段回收 | kill -9 后无丢事件集成测试 |
| **P4** | architecture-spec 定稿；L2 5000/s + L3 delivery SLA 复测 | benchmark §1 / §6.1 |

---

## 15. 预期收益（参考 L2 报告 20260607-003907）

| 指标 | 现状（同步 Publish） | 预期（Outbox） |
|------|-------------------|----------------|
| `processing` P99 | ~0.5 ms | **~0.1–0.3 ms** |
| 稳态 TPS（单 symbol） | ~4373/s | **6000–8000/s**（保守） |
| `matching_kafka_lag`（L3） | 易积压 | 显著下降（consumer 不再等 Kafka） |
| L5 事件可见延迟 | ~ms 级 | **+5–50 ms**（relay 排队） |

---

## 16. 禁止事项

- 热路径直连 PostgreSQL / Redis 存 outbox
- 未 Event Outbox durable 就 commit `order.commands` offset
- 先 commit offset 再写 outbox
- 为降延迟使用 `acks=none` 作为**唯一**投递保证（可仅用于 dev 压测配置）
- Relay 乱序发送同一 symbol 的事件
- 从命令 WAL 回放时**重复生成**与 outbox 重复的事件流

---

## 17. 开放问题（评审）

| # | 问题 | 建议 |
|---|------|------|
| 1 | 命令 WAL 与 Event Outbox 两次 fsync 是否合并为一次 `fdatasync` 边界 | 首版分开；P1 后 profile |
| 2 | Relay 多 worker 与 symbol 保序的 worker 分配 | 按 `hash(symbol) % workers` |
| 3 | L3 `delivery_sla` 是否增加「event_outbox_pending=0」等待 | 可选；与 order outbox drain 对称 |
| 4 | 压测配置 `required_acks` | dev 可用 `one`；生产 `all` |

---

## 18. 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-06-07 | 初版：本地 Event Outbox + Relay；不变式 I1–I7；恢复与幂等 |
