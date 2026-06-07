# L2 性能优化路线图（Phase 4 未达标项）

**版本**: 1.0  
**日期**: 2026-06-04  
**关联**: [architecture-spec.md](./architecture-spec.md) · [benchmark.md](../benchmark.md) · [development-checklist.md](./development-checklist.md) §4.3 · **[优化历程复盘](./l2-optimization-journey.md)**（100+ → 3600+ TPS）

本文记录 L2 压测**未达标**时的瓶颈结论与优化方向，供实现前评审。已落地的步骤说明见 [l2-optimization-journey.md](./l2-optimization-journey.md)。约束以架构 SLA 为准：**单 symbol 单 goroutine 串行**、**先 WAL durable 再改内存**、**WAL fsync 成功后再 commit Kafka offset**。

---

## 1. 达标定义与当前状态

### 1.1 Phase 4 L2 验收（见 [benchmark.md](../benchmark.md) §1）

| 指标 | 目标 | 口径 |
|------|------|------|
| 吞吐 | 单 symbol **≥ 5,000 cmd/s** | `matching_tps_during_load_window` / 差分样本 |
| 延迟 | **P99 ≤ 10 ms** | `metrics-load-window.txt` → `matching_processing_latency_ms` |
| WAL（参考） | SSD 上 P99 宜 **≤ 5 ms** | `matching_wal_append_latency_ms` |
| Kafka lag | 稳态 **≈ 0** | load 结束 `matching_kafka_lag` ≤ 50 |
| 场景 | **M3**，1 symbol / 1 partition，**≥ 5 min** | `run-l2.sh` |

### 1.2 参考报告（清环境 + load 窗口差分）

以 `reports/20260604-153903-l2-m3/` 为例（`env_reset=true`，`--rate 80`，60s）：

| 指标 | 结果 | 是否达标 |
|------|------|----------|
| processing P99 | **9.98 ms** | ✅ 延迟 |
| processing P50 | **7.51 ms** | — |
| WAL P99 | 4.56 ms | ✅（宜 ≤5 ms） |
| Publish P99 | 8.82 ms | — |
| matching TPS | ~96/s | ❌ 吞吐 |
| lag @ load end | 0 | ✅ 稳态 |

**结论（2026-06）**：

- **延迟 SLA（P99≤10ms）** 在 dev 负载（~80/s）下已接近满足。
- **吞吐 SLA（5000/s）** 未满足；与延迟指标**不能混为一谈**（见 §2）。

高发送速率但 lag 爆炸的报告（如 `154321`，lag≈9万）**不能**用其 P99 代表 5000/s 稳态能力。

---

## 2. 单 symbol 串行：TPS 与 P99 的关系

同一交易对命令在 consumer 内**串行**执行，稳态吞吐近似：

```text
TPS_max ≈ 1 / 平均每条墙钟耗时（processing，含 WAL + 撮合 + Publish）
```

要 **TPS ≥ 5000/s**，需要：

```text
平均耗时 ≤ 1000ms / 5000 = 0.2 ms/条
```

| 结论 | 说明 |
|------|------|
| **P99 ≤ 10 ms** | 只约束最慢 1% 样本，**不保证** 5000/s |
| **P50 ≈ 7.5 ms** | 串行理论 TPS ≈ **130/s**，与实测 ~80–130/s 一致 |
| **5000/s + 串行** | 必须让**大多数**命令平均耗时降到 **亚毫秒级**，或改变持久化/发布摊销策略 |

**分布上可同时成立**（少数极慢、多数极快）：例如 99%≈0.1ms、1%≈10ms → 平均≈0.2ms、P99≤10ms、TPS≈5000。  
**当前分布**是 P50/P95/P99 都在 ~7–10ms 带，故 TPS 只有一百多/s。

---

## 3. 热路径与瓶颈分解

```text
解码 proto → ApplyNewOrder/ApplyCancel（WAL fsync + 撮合）→ BuildEvents → Kafka Publish → Commit offset
```

| 阶段 | 典型观测（153903 load 窗口） | 判断 |
|------|------------------------------|------|
| WAL | P99 ~4.5 ms；**每条** `Append` 后 `fdatasync`（`pkg/wal/writer.go`） | I/O 主因之一 |
| Publish | P99 ~8.8 ms；已 match/trade **并行** `WriteBatch` | **Kafka RTT + ack** 主因 |
| 撮合 | L0 微基准通常亚毫秒级 | **非** 5000/s 主矛盾 |
| 环境 | WSL2/慢盘会放大 WAL；lag>0 时勿解读 P99 | 压测方法 |

**端到端 P50≈7.5 ms** 时，距离 5000/s 约差 **37 倍**，主要靠 **摊销 fsync 与 Kafka 墙钟**，而非再抠 matcher CPU。

---

## 4. 优化方案（按优先级）

### 4.1 P0 — WAL 组提交 / 批量 fsync（ROI 最大）— **已实现（2026-06）**

**实现要点**：

- `pkg/wal.FileWriter`：`sync_every_records` / `sync_interval_ms`；`Sync()` 前 append 仅写缓冲，**不**改 orderbook。
- `recovery.Engine`：`StageNewOrder` / `StageCancel` + `CommitBatch()`（先 `wal.Sync()` 再按序 apply）。
- Kafka：`consumer.Run` 在组提交启用时按 `consumer_batch_max` / `consumer_batch_wait_ms` 凑批调用 `ProcessBatch`。
- 配置：`configs/matching.json` → `wal_group_commit`（默认 dev：`sync_every_records=32`）。**每条 fsync** 仍可用 `sync_every_records: 1`（或不写该块）。

**语义边界**（崩溃时）：

1. 未 `Sync()` 的记录：不可 apply；Kafka offset 未 commit → 重投 + 幂等。  
2. `Sync()` 成功后：按序 apply；与「先 WAL durable 再内存」一致。  
3. 禁止在 `Sync()` 前 commit offset 或改 orderbook。

**验证**：

- `go test ./pkg/wal/ ./internal/matching/recovery/ ./internal/matching/consumer/`
- L1：`go test -bench=BenchmarkFileWriter_appendFsync ./pkg/wal/`
- L2：`metrics-load-window.txt` 中 `wal_append` P50/P99 与 `processing` TPS 是否同向改善。

---

### 4.2 P0 — Kafka 发布墙钟（ROI 大，协议内优先）— **已实现（2026-06）**

**现状（优化前）**：`required_acks: one`、并行双 topic `WriteBatch` 后 Publish P99 仍 ~8–9 ms；组提交 WAL 已摊销，但 **ProcessBatch 仍逐条 Publish**（每命令 1–2 次 Kafka 往返）。

**实现要点**：

- `Publisher.PublishBatch`：合并整批 `Outbound`，按 symbol 分组后各 topic **一次或少量** `WriteBatch`；match/trade 仍并行刷盘。
- `consumer.ProcessBatch`：stage 时解码一次、CommitBatch 后 **单次 PublishBatch**（消除逐条发布与 `publishOutcome` 二次 `Unmarshal`）。
- 配置（已有）：`kafka.batch_size` / `batch_timeout_ms`；`compression`: `lz4` | `zstd`（localhost RTT 主导时可不开）。
- 序列化：`publisher/marshal.go` 的 `MarshalAppend` + `sync.Pool` scratch 复用（单条/批量共用）。

**语义**：仍「WAL durable → 全批发布成功 → 再逐条 commit offset」；发布失败整批不 commit。

**未做（刻意）**：按命令单条 outbound 改协议；异步发布 + Outbox（§6 架构待定）。

| 项 | 说明 | SLA |
|----|------|-----|
| `batch_size` / `batch_timeout_ms` | 与生产者节奏对齐 | 配置 |
| 压缩 | lz4/zstd | 配置 |
| 批级聚合发布 | 组提交批内 N 命令 → ~2 次 WriteBatch（match+trade） | 已实现 |
| 序列化 scratch 复用 | 热路径少分配 | 已实现 |
| 异步发布 + Outbox | 发布与 commit 解耦 | **架构变更**，需 spec §6.2 |

**禁止**：未 fsync、未达发布语义就 commit offset（除非改为合规的 Outbox 方案）。

---

### 4.3 P1 — 环境与可观测（支撑准确定位）

| 项 | 说明 |
|----|------|
| `data_dir` | 生产路径 SSD；dev 可对比 **tmpfs**（仅 bench，不代表生产） |
| `run-l2.sh` 默认 | `reset-l2-env`（WAL+Kafka）+ restart；验收看 **`metrics-load-window.txt`** |
| pprof | `block.prof` / `trace.out` 查 fdatasync、WriteBatch；CPU 火焰图对 WAL 往往很薄 |
| 快照 | 压测时增大 `snapshot_every`、评估 `snapshot_on_exit` 对 fsync 干扰 |
| L1 | 裸 fsync bench 区分「盘慢」vs「每条 sync 策略慢」 |

---

### 4.4 P2 — 撮合与事件（CPU 已够时少投）

| 项 | 说明 |
|----|------|
| matcher / skiplist | L0 对比 `benchstat`；非 5000/s 主因时不优先 |
| `BuildNewOrderEvents` | 已 `FindOrder` O(1)；进一步减事件条数属产品/协议权衡 |
| handler/engine | 合并重复 `proto.Marshal`（收益小于 I/O） |

---

### 4.5 不建议 / 禁止

| 项 | 原因 |
|----|------|
| 单 symbol 多 goroutine 改 orderbook | 违反热路径 SLA |
| 热路径直连 PostgreSQL / Redis | 禁止 |
| 仅提高 `bench-producer --rate` 而 matching TPS 不变 | 只涨 lag，不证明稳态 5000/s |
| 用累积 `metrics-post-load` 跨轮比 P99 | 需差分或每轮 reset |

---

## 5. 建议实施顺序

```text
1. L1：BenchmarkFileWriter_appendFsync + 磁盘/tmpfs 对比
2. 架构评审：WAL group commit 语义 → 更新 architecture-spec → 实现 + 恢复测试
3. Kafka：batch/压缩调参 → 事件条数/序列化 →（可选）协议级每命令单条 outbound
4. L2 正式验收：
   ./scripts/bench/run-l2.sh --scenario m3 --rate 5000 --duration 5m
5. 判定（同时满足）：
   - summary: matching_tps_during_load_window ≥ 5000，lag ≤ 50
   - metrics-load-window: processing P99 ≤ 10ms，commands_failed_delta = 0
```

---

## 6. 架构与指标口径待确认（评审项）

[development-checklist.md](./development-checklist.md) 写的是 **「单交易对 TPS ≥ 5000」**，与当前实现 **「每命令 fsync + 同步 Publish 成功再 commit」** 组合时，单 symbol 串行要达到 5000/s 在物理上需要 **平均 ~0.2ms/条**，必须与下列之一对齐：

| 选项 | 说明 |
|------|------|
| A. 允许 **WAL group commit**（及测试） | 摊销 fsync，保持恢复语义 |
| B. 允许 **异步发布 + Transactional Outbox** | 缩短热路径墙钟，commit 规则写清 |
| C. 将 **5000/s 定义为 shard 多 symbol 合计** | 非单 BTC-USDT 链 |
| D. 下调单 symbol TPS 目标至 **~100–200/s**（ms 级延迟产品） | 与现状设计一致 |

**在 A/B/C/D 未定论前**，实现侧默认：**不破坏**「先 WAL 再内存」「单 symbol 串行」「fsync 后 commit offset」。

---

## 7. 已完成优化（备忘，避免重复投入）

| 项 | 状态 |
|----|------|
| Kafka `WriteBatch` + 缩短 `BatchTimeout` | 已做 |
| match/trade 并行 `WriteBatch` | 已做 |
| `FindOrder` O(1) 构建 maker 事件 | 已做 |
| WAL：`fdatasync`、写缓冲、`frame_pool`、`AppendNext` | 已做 |
| L2：`reset-l2-env`、load 窗口差分、pprof 采集 | 已做 |
| Publish / WAL 拆分 Prometheus 指标 | 已做 |
| 组提交批内 `PublishBatch` 聚合 Kafka 发布 | 已做（§4.2） |

---

## 8. 文档与脚本索引

| 资源 | 路径 |
|------|------|
| 压测方案 | [docs/benchmark.md](../benchmark.md) |
| L3 Outbox 优化 | [l3-optimization-roadmap.md](./l3-optimization-roadmap.md) |
| L2 脚本 | `scripts/bench/run-l2.sh` |
| 环境重置 | `scripts/bench/reset-l2-env.sh` |
| WAL 实现 | `pkg/wal/writer.go` |
| 发布 | `internal/matching/publisher/publisher.go` |
| 消费热路径 | `internal/matching/consumer/handler.go` |

---

## 9. 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-06-04 | 初版：基于 153903/154321 报告与 Phase 4 差距分析 |
