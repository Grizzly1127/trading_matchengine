# Benchmark 与压测方案

**版本**: 1.0  
**关联**: [architecture-spec.md](./develop_docs/architecture-spec.md) Phase 4 · [development-checklist.md](./develop_docs/development-checklist.md) §4.3 · [l2-optimization-roadmap.md](./develop_docs/l2-optimization-roadmap.md)（L2 未达标时的优化路线图）· [l2-optimization-journey.md](./develop_docs/l2-optimization-journey.md)（**已做优化逐步复盘**）

本文描述本仓库 **L0～L3** 性能测试分层、验收指标与可执行命令。开发环境使用本地 Docker Compose，无需云托管。

---

## 1. 验收指标

| 指标 | 目标 | 口径 |
|------|------|------|
| 撮合吞吐 | 单 symbol **≥ 5,000 cmd/s** | `rate(matching_commands_processed_total[1m])` |
| 撮合延迟 | **P99 ≤ 10 ms** | `matching_processing_latency_ms`（含 WAL + match + publish） |
| WAL | 可观测，SSD 上 P99 宜 **≤ 5 ms** | `matching_wal_append_latency_ms` |
| Kafka lag | 稳态 **≈ 0** | `matching_kafka_lag` |

**正式验收场景**：L2 **M3**（70% 吃单 + 30% 挂单），固定 1 个 symbol、1 个 Kafka partition，压测 ≥ 5 分钟。

---

## 2. 分层模型

```text
L0  微基准     go test -bench          engine / skiplist / wal
L1  组件       WAL fsync               pkg/wal *_bench_test.go
L2  进程       Kafka → Matching        cmd/bench-producer + scripts/bench/run-l2.sh
L3  全链路     Gateway → Order → …     scripts/bench/e1-orders.sh (vegeta)
```

优化顺序：**L0 → L1 → L2（SLA）→ L3（容量）**。

---

## 3. 环境准备

```bash
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh
make build
./scripts/reset-dev.sh -y --migrate --kafka-topics
./scripts/dev.sh start --build
```

压测建议：

- Matching `data_dir` 在 **SSD** 或 tmpfs。
- `log.level` 设为 `warn` / `error`。
- `snapshot_every` 调大，减少快照干扰。
- 压测 symbol 与 `configs/shards.json` 的 `kafka_partition` 一致（默认 `BTC-USDT` → partition `0`）。

---

## 4. L0：微基准

**不需要**启动 matching / Kafka / Docker；仅 `go test -bench`，**不会**停止或影响已运行的 matching。

若跑 L0 时发现 matching 不在，常见原因是 `dev.sh` 曾先起 matching、后起 order，导致启动对账失败（见 `logs/matching.log` 中 `recovery verify failed`）。处理：`./scripts/order.sh start` 后再 `./scripts/matching.sh start`。

```bash
make bench-l0
# 报告目录: reports/<timestamp>-l0/（bench.txt、meta.txt）
# 或
./scripts/bench/run-l0.sh --count 10
```

| 基准 | 文件 | 说明 |
|------|------|------|
| `BenchmarkMatch_restingBuy` | `matcher_bench_test.go` | 纯挂单 |
| `BenchmarkMatch_takeSell` | `matcher_bench_test.go` | 吃单 |
| `BenchmarkFileWriter_appendFsync` | `writer_bench_test.go` | WAL 落盘 |
| `BenchmarkSkipList_*` | `skiplist_bench_test.go` | 跳表 |

对比两次优化：

```bash
go install golang.org/x/perf/cmd/benchstat@latest
make bench-l0    # → reports/20260101-120000-l0/bench.txt
# 优化后再跑一次，对比：
benchstat reports/<old>-l0/bench.txt reports/<new>-l0/bench.txt
```

---

## 5. L2：进程基准（主战场）

### 5.1 负载场景

| 场景 | `bench-producer -scenario` | 说明 |
|------|---------------------------|------|
| Seed | `seed` | 预置卖盘（m2/m3 前） |
| M1 | `m1` | 100% 非交叉挂单 |
| M2 | `m2` | 100% 吃单 |
| **M3** | `m3` | 70% 吃单 + 30% 挂单（**SLA 验收**） |
| M4 | `m4` | 挂单 + 撤单交替 |

### 5.2 一键脚本

```bash
chmod +x scripts/bench/*.sh
make build-bench    # 更新 bench-producer 后必做
./scripts/bench/run-l2.sh
./scripts/bench/run-l2.sh --scenario m3 --rate 5000 --duration 5m
./scripts/bench/run-l2.sh --no-reset-env  # 不清 WAL/Kafka（多轮可比性差）
./scripts/bench/run-l2.sh --no-restart    # 不重启 matching（累积直方图污染）
./scripts/bench/run-l2.sh --no-pprof      # 跳过 load 内 cpu/block/trace 采集
```

**默认行为**（`run-l2.sh`）：

1. **`reset-l2-env.sh`** — 删 `data/wal`、`data/snapshots`，重置 `order.commands` / `match.events` / `trade.events` 与 `matching-shard-0` consumer group（**不**清 PostgreSQL/Redis）  
2. **`matching.sh restart --build`** — 空 WAL 冷启动 + 清空进程内 Prometheus 直方图  
3. **`metrics-load-window.txt`** — load 段直方图差分，**P99 验收优先此文件**  
4. **load 期间自动 pprof** — `cpu.prof` / `block.prof` / `trace.out`

仅 `restart` **不够**：上轮 WAL 会恢复 orderbook，Kafka 积压/offset 会让 lag、TPS 失真（你连跑 3 轮时 `processed_total` 累加到 15k 即为此例）。

全链路（Order/Gateway/DB）压测前用：`./scripts/reset-dev.sh -y --kafka-topics`。单独 L2 用：`./scripts/bench/reset-l2-env.sh` 或 `run-l2.sh --no-reset-env` 跳过清理。

报告目录：`reports/<timestamp>-l2-<scenario>/`（含 `metrics-load-window.txt`、`metrics-*.prom`、`load.log`、可选 pprof）。

**seed / warmup 进度**：`bench-producer` 会向 stderr 打印 `seed progress: …`。若长时间无输出，请 `make build-bench` 后重试。

**matching 处理慢（lag 数万、P99 秒级）**：曾是 Kafka 发布逐条 + `BatchTimeout=1s`；修复后需 **`make build && ./scripts/matching.sh restart`**，P99 应降到 **几十 ms** 量级。

**`load.log` avg_rate 只有 ~180/s（目标 2000）**：曾是 `bench-producer` 压测阶段逐条 `WriteAt`（~5ms/条 ≈ 200/s 上限）。请 **`make build-bench`** 后用批量发送版本再跑。

**matching 真实 TPS**：看 **`summary.txt`** 里的 `matching_tps_during_load_window`（`metrics-pre-load` → `metrics-post-load` 增量 ÷ load 秒数）。**不要**用 `tps-estimate.txt` 的 `drain_tps_after_load` 判断压测段——那是 load 结束后 10s「扫尾」消化 backlog，往往只有 ~25/s，会误以为 matching 很慢。

单 symbol 串行、P99≈25ms 时理论约 **40/s**；若 `--rate 80` 且 `matching_tps_during_load_window≈75～80`，说明 **已跟上生产者**。**Phase 4 的 5000 TPS** 仍需架构优化。

### 5.3 手动步骤

```bash
make build-bench-producer

# 1. 预置卖盘（m2/m3）
./bin/bench-producer -scenario seed -seed-depth 5000 -symbol BTC-USDT -partition 0

# 2. 预热
./bin/bench-producer -scenario warmup -warmup 10000 -rate 1 -duration 1s

# 3. 压测（M3）
./bin/bench-producer -scenario m3 -rate 5000 -duration 5m -warmup 0

# 4. 指标
./bin/bench-report -url http://localhost:9101/metrics
./scripts/bench/collect-metrics.sh --delta 60
```

### 5.4 Prometheus（可选）

```promql
rate(matching_commands_processed_total[1m])
histogram_quantile(0.99, rate(matching_processing_latency_ms_bucket[5m]))
histogram_quantile(0.99, rate(matching_wal_append_latency_ms_bucket[5m]))
matching_kafka_lag
```

---

## 6. L3：全链路

需安装 [vegeta](https://github.com/tsenart/vegeta)：

```bash
go install github.com/tsenart/vegeta@latest
./scripts/bench/e1-orders.sh --deposit --rate 200 --duration 3m
```

观察：

- Gateway 延迟 / 错误率（`vegeta report`）
- `http://localhost:9101/metrics` — Matching
- `http://localhost:9104/metrics` — `order_outbox_pending_count`

L3 与 L2 SLA **分开评估**：全链路受 PostgreSQL、Outbox 影响，不必与 5k cmd/s 直接对比。

---

## 7. Makefile 目标

| 目标 | 说明 |
|------|------|
| `make bench-l0` | L0 微基准 |
| `make bench-l0-smoke` | CI 短跑（`-benchtime=50ms`） |
| `make build-bench` | 编译 `bench-producer`、`bench-report` |
| `make bench-l2` | 调用 `scripts/bench/run-l2.sh`（默认会 restart matching） |

---

## 8. 报告模板

每次正式压测归档 `reports/` 子目录，并记录：

- Git SHA、Go 版本、`GOMAXPROCS`
- 磁盘类型、`data_dir` 路径
- 场景、目标 rate、实际 TPS（`collect-metrics.sh --delta`）
- **`metrics-load-window.txt`** 中 **P99** 是否 ≤ 10ms（勿仅用累积的 `metrics-post-load.txt`）
- PASS / FAIL

---

## 9. 定位 P99 耗时（profile）

`matching_processing_latency_ms` 是**整条命令**耗时（`consumer.Handler.Process`）：

```text
解码 proto → ApplyNewOrder/ApplyCancel（含 WAL fsync + 撮合）→ BuildEvents → Kafka Publish
```

已有拆分指标：

| 指标 | 含义 |
|------|------|
| `matching_processing_latency_ms` P99 | 端到端 |
| `matching_wal_append_latency_ms` P99 | 仅 WAL `Append+fsync` |
| `matching_publish_latency_ms` P99 | 并行发布时为 max(match, trade) 墙钟；仅单 topic 时为该次 `WriteBatch` |
| `matching_publish_match_latency_ms` P99 | 仅 `match.events` |
| `matching_publish_trade_latency_ms` P99 | 仅 `trade.events` |
| `matching_publish_match_events` / `trade_events` | 每命令发布条数分布 |

对比（优先 **`metrics-load-window.txt`**，仅含 load 60s 内新样本）：

```text
Publish（粗算）≈ publish_latency P99
撮合+其它（粗算）≈ processing P99 − wal_append P99 − publish P99
```

手动差分：

```bash
curl -s localhost:9101/metrics -o /tmp/pre.prom   # load 前
# ... 压测 ...
curl -s localhost:9101/metrics -o /tmp/post.prom  # load 后
./bin/bench-report -delta-pre /tmp/pre.prom -delta-post /tmp/post.prom -label load_window
```

`configs/matching.json` 中 Kafka 发布可调：`required_acks`（dev 默认 `one`）、`batch_size`、`batch_timeout_ms`、`compression`。

若 **P50 很低、P99 很高** 且用的是**累积** `metrics-post-load.txt`，多为 seed/历史样本污染 — 改用 **`metrics-load-window.txt`** 或每轮 **`run-l2.sh` 默认 restart**。

### 9.1 Prometheus 快速看

```bash
curl -s localhost:9101/metrics | grep -E 'matching_(processing|wal_append|publish)_'
./bin/bench-report -url http://localhost:9101/metrics
```

### 9.2 CPU profile（matching 已挂 pprof）

`run-l2.sh` 默认在 load 窗口写入报告目录下的 `cpu.prof`（无需另开终端）。手动采集：

```bash
# 采 30s CPU（需先 make build && ./scripts/matching.sh restart）
go tool pprof -http=:0 http://localhost:9101/debug/pprof/profile?seconds=30

# 或生成文件后本地看
curl -o /tmp/cpu.prof "http://localhost:9101/debug/pprof/profile?seconds=30"
go tool pprof -http=:0 reports/<dir>/cpu.prof
```

在浏览器里看 **Top / Flame Graph / Source**，关注：

- `wal.(*FileWriter).Append` / `fsync`
- `publisher.(*KafkaPublisher).Publish` / `WriteBatch`
- `engine.(*OrderBook).Match` / `skiplist`
- `proto.Marshal` / `Unmarshal`
- `BuildNewOrderEvents` / `FindOrder`（吃单多 maker 时）

### 9.3 阻塞 profile（等锁、I/O）

matching 启动时已 `SetBlockProfileRate(1e6)` 并暴露 `/debug/pprof/block`。L2 报告内 `block.prof`：

```bash
go tool pprof -http=:0 reports/<dir>/block.prof
```

WAL **fdatasync** 多在 block/trace 里可见，CPU 火焰图往往很薄。

### 9.4 执行 trace（看时间线）

L2 报告内 `trace.out`：

```bash
go tool trace reports/<dir>/trace.out
```

### 9.5 无运行进程时：L0 + CPU profile

```bash
go test -bench=BenchmarkMatch_takeSell -cpuprofile=/tmp/cpu.prof -benchtime=3s ./internal/matching/engine/
go tool pprof -http=:0 /tmp/cpu.prof
```

只覆盖**纯撮合内存路径**，不含 WAL/Kafka。

### 9.6 L2 未达标时的优化路线图

完整方案（瓶颈分解、P0/P1/P2、架构评审项、已完成项）：**[l2-optimization-roadmap.md](./develop_docs/l2-optimization-roadmap.md)**。

摘要：

1. **吞吐**：单 symbol 串行要 5000/s ⇒ 平均 **≤0.2ms/条**；P99≤10ms **不推出** 5000/s（当前 P50≈7.5ms ⇒ ~130/s）。
2. **P0**：WAL **group commit**（摊销 fsync）；Kafka **墙钟**（batch/事件条数/协议，禁止违规先 commit）。
3. **P1**：磁盘/tmpfs、L1 fsync bench、`metrics-load-window` + block/trace。
4. **验收**：`run-l2.sh --scenario m3 --rate 5000 --duration 5m`，同时 TPS≥5000、lag≤50、P99≤10ms。

---

## 10. 常见干扰

| 现象 | 处理 |
|------|------|
| P99 高、L0 低 | 查 WAL / 磁盘；看 `metrics-load-window.txt` 的 `wal_append` |
| 连跑多轮 P50 趋近 0 | 未 restart 导致累积直方图污染；用默认 `run-l2.sh` 或 `--no-restart` 仅调试 |
| TPS 低、lag 涨 | 生产者 rate 不足或 partition 不对 |
| `commands_failed` 涨 | `client_order_id` / `order_id` 冲突，换 `-start-order-id` |
| JSONL 模式 | 不作为正式 benchmark |

---

## 11. 目录索引

```text
docs/develop_docs/l2-optimization-roadmap.md  # L2 优化路线图（Phase 4 吞吐/延迟）
cmd/bench-producer/     # Kafka 命令生产者
cmd/bench-report/       # Prometheus 指标摘要
pkg/benchutil/          # 命令构造 + 直方图解析
scripts/bench/          # run-l0.sh, run-l2.sh, reset-l2-env.sh, collect-pprof.sh
reports/                # 压测输出（git 忽略 *.txt/*.bin，保留 .gitkeep）
internal/matching/engine/matcher_bench_test.go
pkg/wal/writer_bench_test.go
pkg/skiplist/skiplist_bench_test.go
```
