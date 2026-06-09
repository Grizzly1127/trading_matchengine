# L3 全链路性能优化路线图（Order Outbox 投递）

**版本**: 1.0  
**日期**: 2026-06-05  
**关联**: [architecture-spec.md](./architecture-spec.md) §4.3 · [benchmark.md](../benchmark.md) §6 · [l2-optimization-roadmap.md](./l2-optimization-roadmap.md)

本文记录 L3 压测（`scripts/bench/e1-orders.sh`）暴露的瓶颈与分阶段优化方案，供实现前评审。约束以架构 SLA 为准：**Transactional Outbox**、**禁止 DB 事务提交前直发 Kafka**、**`acks=all`**（生产）。

---

## 1. 达标定义与当前状态

### 1.1 L3 双指标（接单 vs 投递）

L3 与 L2 **分开评估**（见 [benchmark.md](../benchmark.md) §6）。全链路应拆成两项：

| 指标 | 目标 | 口径 |
|------|------|------|
| **L3 接单吞吐** | 压测目标 rate（如 500/s） | `vegeta report` Success ≥ 99.9% |
| **L3 接单延迟** | P99 ≤ 50 ms（dev 参考） | `vegeta report` Latencies |
| **L3 投递完成率** | ≥ 99% | `matching_commands_processed_total` / API 成功数（压测结束 + 排空等待后） |
| **Outbox 稳态积压** | ≈ 0 | `order_outbox_pending_count` |
| **Matching 消费** | lag ≈ 0 | `matching_kafka_lag` |

### 1.2 参考报告 `reports/20260605-173958-l3-e1/`

场景：`500/s × 3m`，50 用户轮询，自动充值，`reset_env=true`。

| 维度 | 结果 | 是否达标 |
|------|------|----------|
| API 成功率 | **100%**（90,000 × 201） | ✅ 接单 |
| API 吞吐 | **499.99/s** | ✅ |
| API P99 延迟 | **12 ms** | ✅ |
| Matching 已处理 | **13,911 / 90,000**（15.5%） | ❌ 投递 |
| Outbox 积压 | **74,263** | ❌ |
| Matching kafka lag | **0** | ✅ 消费非瓶颈 |
| Matching processing P99 | **18.8 ms** | 略超 L2 10ms，样本仅 1.4 万笔 |

**结论（2026-06-05）**：

- **Gateway → Order 写库 + 冻结**：500/s 稳定，~4 ms 均值延迟。
- **瓶颈在 Order Outbox Relay → Kafka**：入队 ~500/s，出队 ~77/s（粗算 13,911 ÷ 180s）。
- **Matching 能跟上 Kafka 投喂**（lag=0）；**不要优先优化 Matching**。

### 1.3 对比：余额问题已解决

报告 `reports/20260605-172648-l3-e1/`（改进前）为 **98% 422 余额不足**，与性能无关。  
改进后（自动充值 + 多用户）见 `173958`，才暴露真实管道瓶颈。

---

## 2. 瓶颈分解

```text
vegeta 500/s
  → Gateway (~4ms)
  → Order PlaceOrder（单 PG 事务：幂等 + 冻结 + orders + outbox）
  → order_outbox 积压
  → Outbox Relay（单 goroutine，串行 3 次 I/O/条）  ← 瓶颈
  → Kafka order.commands
  → Matching（lag=0，~77/s 实测消费）
```

### 2.1 Relay 热路径（现状）

实现：`internal/order/outbox/relay.go`

每条 Outbox 记录**串行**执行：

1. `GetOrderStatus` — 1× PG 查询  
2. `WriteAt` — 1× Kafka 写入  
3. `MarkPublished` — 1× PG 更新  

默认参数（**硬编码**，`configs/order.json` 未暴露）：

| 参数 | 默认值 |
|------|--------|
| `PollInterval` | 200 ms |
| `BatchSize` | 50 |

单批 50 条 × ~6 ms/条 ≈ 300 ms > 200 ms 轮询间隔 → 理论上限约 **170–250/s**；实测投递 ~77/s 与实现一致。

### 2.2 其他次要因素

| 项 | 说明 |
|----|------|
| `insertNewOrderOutbox` | INSERT 空 payload + UPDATE，PlaceOrder 事务多 1 次写 |
| 单 Relay goroutine | 无法并行追赶积压 |
| API 与 Relay 共用 `pgxpool` | 500/s 写可能挤占 Relay 查询连接 |
| `vegeta report` 延迟 | 仅反映 API，**看不到** Outbox 积压 |

---

## 3. 优化方案（按优先级）

### 3.1 Phase 1 — 快速见效（1–2 天）

**目标：投递 ~77/s → 300–500/s**

#### P0-1 有积压时连续 poll（不 sleep）— **已实现（2026-06-05）**

- **文件**：`internal/order/outbox/relay.go`
- **行为**：`pollOnce` 若本批取满 `batch_size`，**立即再 poll**；仅 backlog 为空或未取满时等待 `poll_interval`
- **收益**：消除 200 ms 空等，追赶积压时吞吐接近连续处理

#### P0-2 Kafka 批量写 — **已实现（2026-06-05）**

- **文件**：`internal/order/outbox/relay.go`，复用 `pkg/kafka/writer.go` 已有 `WriteBatchAt`
- **行为**：按 `(topic, partition, partition_key)` 分组，一次 `WriteBatchAt` 替代 N 次 `WriteAt`
- **配置**：`configs/order.json` → `kafka.batch_size`、`kafka.batch_timeout_ms`（默认 500 / 5ms）— **已实现（2026-06-05）**

#### P0-3 PG 批量标记已发布 — **已实现（2026-06-05）**

- **文件**：`internal/order/repository/repository.go`
- **新增**：`MarkPublishedBatch(ctx, ids []uint64)`
- **SQL**：

```sql
UPDATE order_outbox SET published_at = now()
WHERE id = ANY($1) AND published_at IS NULL
```

#### P1-1 Relay 可配置化 — **已实现（2026-06-05）**

- **文件**：`internal/order/config/config.go`、`internal/order/config/outbox_relay.go`、`configs/order.json`、`cmd/order/main.go`
- **新增配置块**：

```json
"outbox_relay": {
  "poll_interval_ms": 20,
  "batch_size": 500,
  "max_retry": 100
}
```

#### P1-2 批量校验订单状态 — **已实现（2026-06-05）**

- **文件**：`internal/order/repository/repository.go`、`internal/order/outbox/relay.go`
- **新增**：`GetOrderStatusesBatch(ctx, orderIDs []uint64) map[uint64]string`
- **行为**：一次 `SELECT id, status FROM orders WHERE id = ANY($1)` 替代 N 次 `GetOrderStatus`
- **语义**：与 architecture-spec §4.3「投递前检查 PENDING/CANCELING」一致

| Phase 1 项 | 优先级 | 预期收益 |
|------------|--------|----------|
| 连续 poll | P0 | 2–3× |
| Kafka 批量写 | P0 | 2–5× |
| 批量 MarkPublished | P0 | 2–3× |
| 配置化 + 调参 | P1 | 运维可调 |
| 批量 GetOrderStatus | P1 | 1.5–2× |

---

### 3.2 Phase 2 — 结构性提升（3–5 天）

**目标：投递 500/s → 800–1500/s**

#### P2-1 多 Worker + `FOR UPDATE SKIP LOCKED` — **已实现（2026-06-06）**

- **Fetch SQL**（`repository/outbox_claim.go`）：事务内 `FOR UPDATE SKIP LOCKED` 领取；Kafka 成功后 `Commit`，失败 `Rollback` 释放锁
- **Relay**：`Run()` 启动 `workers` 个 goroutine（默认配置 `1`，`configs/order.json` 推荐 `4`）
- **配置**：`outbox_relay.workers`

```sql
SELECT ... FROM order_outbox
WHERE published_at IS NULL
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED
```

#### P2-2 Relay 与 API 连接池隔离 / 调大 MaxConns ✅

- `database.max_conns`（默认 50）与 `database.relay_max_conns`（默认 20）分池
- `RelayStore` 使用 Relay 专用 pool，API 写路径用主 pool

#### P2-3 下单写路径：Outbox 单次 INSERT ✅

- **文件**：`internal/order/repository/outbox_insert.go`、`repository.go`、`cancel_apply.go`
- 事务内 `nextval` 预分配 id，一次 INSERT 带完整 payload

#### P2-4 新增 Relay 可观测指标 ✅

| 指标 | 用途 |
|------|------|
| `order_outbox_relay_batch_size` | 每批实际条数 |
| `order_outbox_relay_dispatch_latency_ms` | 单批 fetch→kafka→mark 耗时 |
| `order_outbox_relay_published_total` | 累计发布条数（可算 rate） |

---

### 3.3 Phase 3 — 压测与验收改造（1 天）

#### P3-1 `e1-orders.sh` 管道等待与报告 ✅

压测结束后：

- 轮询 `order_outbox_pending_count` 直至 0 或超时（默认 10 min，`--drain-timeout`）
- 再采 `matching_commands_processed_total`

报告目录新增 `pipeline_summary.txt`：

- `api_success_count`
- `matching_processed_count`
- `pipeline_completion_rate`
- `outbox_drain_seconds`
- `outbox_pending_final`

#### P3-2 更新 [benchmark.md](../benchmark.md) L3 验收口径 ✅

1. **接单 SLA**：vegeta Success ≥ 99.9%，P99 ≤ 50 ms  
2. **投递 SLA**：排空后 `matching_processed / api_success ≥ 99%`

---

### 3.4 Phase 4 — 架构级（按需，1–2 周）

仅在 Phase 1–2 仍不足时考虑。

| 方案 | 说明 | 风险 |
|------|------|------|
| Relay 独立进程/Deployment | API 与投递解耦扩缩 | 运维复杂度 |
| Order 水平扩展 + 分库 | 多实例按 user_id 分片 | 大改，偏离当前单库设计 |
| 压测专用 `acks=1` | 换 Kafka 吞吐 | **仅 dev**；生产仍 `acks=all` |

---

## 4. 不建议 / 禁止

| 项 | 原因 |
|----|------|
| 优先优化 Matching | L3 报告 lag=0，消费不是瓶颈 |
| 绕过 Outbox 直写 Kafka | 违反 architecture-spec §4.3、§846 |
| 用 vegeta 延迟评判全链路完成度 | 只反映 API，掩盖 Outbox 积压 |
| 单用户小额充值压 9 万买单 | 422 余额不足，测不出管道能力 |
| 生产为压测降低 `acks` | 破坏可靠投递语义 |

---

## 5. 建议实施顺序

```text
1. Phase 1 P0：连续 poll + WriteBatchAt + MarkPublishedBatch
2. Phase 1 P1：outbox_relay 配置化 + GetOrderStatusesBatch
3. L3 复测：
   ./scripts/bench/e1-orders.sh --rate 500 --duration 3m
   对比 outbox_pending、matching_processed、pipeline_completion_rate
4. Phase 2（若投递仍 < 500/s）：SKIP LOCKED 多 worker + Outbox 单次 INSERT
5. Phase 3：压测脚本排空等待 + benchmark.md 双指标验收
6. Phase 4：仅在前序不足且需更高上限时评审
```

### 5.1 阶段收益预期

| 阶段 | 工作量 | 投递吞吐预期 |
|------|--------|-------------|
| Phase 1 | 小 | **300–500/s** |
| Phase 2 | 中 | **500–1500/s** |
| Phase 3 | 小 | 验收可信 |
| Phase 4 | 大 | 按 Matching 上限扩展 |

---

## 6. 已完成项（L3 压测基建）

| 项 | 状态 | 说明 |
|----|------|------|
| vegeta JSON target（≥ v12.7） | ✅ | `e1-orders.sh -format=json` |
| 默认 reset-dev + 重启 | ✅ | 每轮干净环境 |
| 自动充值 + 多用户轮询 | ✅ | 避免 422 |
| `meta.txt` 压测参数 | ✅ | `reports/*-l3-e1/meta.txt` |
| Outbox Relay P0（连续 poll + 批量 Kafka + 批量 Mark） | ✅ | 2026-06-05 |
| Outbox Relay P1（配置化 + 批量 GetOrderStatus + Kafka batch 配置） | ✅ | 2026-06-05 |
| Outbox Relay P2-1（多 worker + SKIP LOCKED） | ✅ | 2026-06-06 |
| Outbox Relay P2-2（API/Relay 连接池隔离） | ✅ | 2026-06-06 |
| Outbox Relay P2-3（Outbox 单次 INSERT） | ✅ | 2026-06-06 |
| Outbox Relay P2-4（Relay Prometheus 指标） | ✅ | 2026-06-06 |
| L3 管道排空 + pipeline_summary | ✅ | 2026-06-06 |
| L3 benchmark.md 双 SLA 验收口径 | ✅ | 2026-06-06 |

---

## 7. 文档与代码索引

| 资源 | 路径 |
|------|------|
| L3 压测脚本 | `scripts/bench/e1-orders.sh` |
| Outbox Relay | `internal/order/outbox/relay.go` |
| Outbox 仓储 | `internal/order/repository/repository.go` |
| Kafka Writer（含 WriteBatchAt） | `pkg/kafka/writer.go` |
| Order 配置 | `configs/order.json` |
| Order 启动 | `cmd/order/main.go` |
| 架构 Outbox 语义 | [architecture-spec.md](./architecture-spec.md) §4.3 |
| L3 压测说明 | [benchmark.md](../benchmark.md) §6 |

---

## 8. 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-06-05 | 初版：基于 `173958` 报告（API 500/s OK，Outbox ~77/s 瓶颈） |
| 1.1 | 2026-06-05 | P0-1/P0-2/P0-3 已实现 |
| 1.2 | 2026-06-05 | P1-1/P1-2 已实现（含 kafka batch 配置） |
| 1.3 | 2026-06-06 | P2-1 多 worker + SKIP LOCKED |
| 1.4 | 2026-06-06 | P2-2 连接池隔离、P2-3 单次 INSERT、P2-4 Relay 指标 |
| 1.5 | 2026-06-06 | P3-1 排空等待 + pipeline_summary；P3-2 benchmark L3 SLA |
