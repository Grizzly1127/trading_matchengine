# Order 交易闸门（Trading Gate）+ Matching L1 HA 设计方案

**版本**: 1.0  
**日期**: 2026-06-07  
**状态**: 设计评审（未实现）  
**关联**: [architecture-spec.md](./architecture-spec.md) §3.1 / §4.5 / §4.6 / §5 · [rest-api.md](../rest-api.md) §1.3 · [development-checklist.md](./development-checklist.md)

---

## 1. 背景与目标

### 1.1 问题

当前 Order → Matching 命令路径经 **Transactional Outbox + Kafka** 异步投递，Order **不在下单热路径感知 Matching 存活**。Matching 宕机或 Kafka 消费 lag 过高时：

- Order 仍接受新单，返回 HTTP/gRPC 成功，`status=PENDING`，余额已冻结；
- 命令持续写入 Kafka 积压；
- 用户误以为「下单成功 = 已挂单/可成交」；
- Reconciler 在 Matching Admin 不可达时会 **跳过** 超时拒单，PENDING 窗口被拉长。

详见架构 §4.6：**API 成功不保证已撮合**；本方案在 Matching 异常时改为 **明确拒收新单**，而非 silent PENDING。

### 1.2 已确认决策

| 项 | 决策 |
|----|------|
| **熔断粒度** | **按 symbol**（非全局、非仅 shard） |
| **lag 阈值** | **配置文件**可调（含 open/close 双阈值与 hysteresis） |
| **Matching HA** | **L1 + Gate**（StatefulSet + PVC 进程级恢复；**不**做 L3 冷备 Standby） |

### 1.3 目标

| 目标 | 说明 |
|------|------|
| **拒收新单** | Matching 不可用或积压过大时，`PlaceOrder` 返回 **503**，不写入 DB/Outbox |
| **允撤单** | `CancelOrder`、查单、充值等不受影响（对齐 `shardmgr.MigrationHaltOnlyCancel`） |
| **按 symbol** | 仅熔断异常 symbol；同 shard 内其他 symbol 可继续交易 |
| **可配置** | lag、探测间隔、hysteresis 等均由 `order.json` 配置 |
| **多副本一致** | 多个 Order Pod 通过 Redis 共享闸门状态 |
| **可观测** | Prometheus 指标 + 结构化日志，便于告警与演练 |

### 1.4 非目标（本阶段）

- 不实现 Matching 双活 / 热备 / WAL 实时复制
- 不实现跨节点冷备 Standby Pod（L3）
- 不改变命令路径（仍 Kafka + Outbox，不用 gRPC 直调撮合）
- 不熔断 `CancelOrder`（已有 PENDING/ACCEPTED/PARTIAL 单仍需可撤）
- 不在 Gateway 层单独维护闸门（逻辑集中在 Order Service）

---

## 2. 总体架构

```text
                    ┌─────────────────────────────────────┐
                    │           Order Service              │
  PlaceOrder ──────►│  validate → Shards.AssertPlaceOrder │
                    │           → TradingGate.Assert(symbol)│──► 503 TRADING_SUSPENDED
                    │           → InsertPending + Outbox    │──► 200 PENDING
                    │                                      │
                    │  TradingGateMonitor (后台)            │
                    │    poll MatchingAdmin.GetShardStatus│
                    │    per shard → 展开为 per-symbol 状态  │
                    │    写入 Redis + 本地缓存               │
                    └──────────────┬──────────────────────┘
                                   │ gRPC (内网)
                    ┌──────────────▼──────────────────────┐
                    │      Matching Engine (L1)            │
                    │  StatefulSet + PVC, WAL + Snapshot   │
                    │  consumer group 单实例消费 partition  │
                    │  Admin :50061 GetShardStatus (新增)   │
                    └──────────────┬──────────────────────┘
                                   │
                              Kafka order.commands
```

### 2.1 与现有机制关系

| 已有能力 | 本方案 |
|----------|--------|
| `shardmgr.AssertPlaceOrder` / 迁移 `halt_only_cancel` | Gate 与之 **AND**；迁移停牌优先 |
| Matching `SetSymbolReadOnly`（§5.6 对账失败） | 经 `GetShardStatus.symbols[].read_only` 驱动 symbol OPEN |
| `reconciler` 60s 超时拒单 | **保留**作兜底；Gate 减少无效 PENDING |
| `matching_kafka_lag` 指标 | Admin RPC 内嵌同源数据，供 Gate 决策 |

---

## 3. 熔断信号与 symbol 粒度映射

### 3.1 信号来源

Monitor 按 **shard** 轮询 Matching Admin（`shards.json` → `node` / 配置中的 `grpc_addr`），将 shard 级信号 **展开** 到该 shard 下的每个 symbol。

| 信号 | 判定 | 影响范围 |
|------|------|----------|
| Admin gRPC 不可达 | 连续 N 次失败（配置 `admin_fail_threshold`） | 该 shard **全部 symbol** OPEN |
| `consumer_running=false` | Matching 自报 | 该 shard 全部 symbol OPEN |
| `kafka_lag > lag_open_threshold` | 配置阈值 | 该 shard 全部 symbol OPEN |
| `last_command_applied_at` 超时 | 超过 `stale_apply_seconds` 无新命令 apply | 该 shard 全部 symbol OPEN |
| `symbols[].read_only=true` | 对账失败等 | **仅该 symbol** OPEN |

> **说明**：Kafka partition 按 shard 共享时，lag 为 partition 级；暂以 shard 级 lag 驱动该 shard 下所有 symbol 熔断。若未来热门 symbol 独占 partition，可演进为 partition→symbol 一一映射。

### 3.2 状态机（per symbol）

```text
CLOSED（允许 PlaceOrder）
  │
  ├─ 任一 OPEN 条件满足 ──► OPEN（拒新单）
  │
OPEN
  │
  └─ 全部恢复条件连续满足 normal_window_seconds ──► CLOSED

恢复条件（须同时满足）：
  - Admin 可达
  - consumer_running = true
  - kafka_lag ≤ lag_close_threshold
  - symbol.read_only = false
  - last_command_applied 未超时
```

**Hysteresis**：`lag_open_threshold` > `lag_close_threshold`，避免 lag 在阈值附近抖动导致频繁开关。

---

## 4. Matching 侧变更

### 4.1 Proto 扩展

文件：`proto/matching/v1/admin.proto`

```protobuf
service MatchingAdminService {
  rpc GetOrderPresence(GetOrderPresenceRequest) returns (GetOrderPresenceResponse);
  rpc ReconcileOrders(ReconcileOrdersRequest) returns (ReconcileOrdersResponse);
  // 新增：供 Order Trading Gate 轮询
  rpc GetShardStatus(GetShardStatusRequest) returns (GetShardStatusResponse);
}

message GetShardStatusRequest {
  string shard_id = 1; // 可选；空则返回本进程 shard_id
}

message GetShardStatusResponse {
  string shard_id = 1;
  bool consumer_running = 2;
  int64 kafka_lag = 3;
  uint64 last_committed_offset = 4;
  uint64 high_watermark = 5;
  int64 last_command_applied_at_unix_ms = 6;
  repeated SymbolTradingState symbols = 7;
}

message SymbolTradingState {
  string symbol = 1;
  bool read_only = 2;
  string read_only_reason = 3;
}
```

### 4.2 实现要点

| 字段 | 来源 |
|------|------|
| `consumer_running` | Kafka consumer loop 是否在运行（含恢复完成且未 panic） |
| `kafka_lag` | 现有 `metrics.KafkaLag` / consumer `ReadLag` |
| `last_committed_offset` | consumer 已 commit 的 offset |
| `high_watermark` | broker high watermark |
| `last_command_applied_at_unix_ms` | 最近一次成功 apply 命令的 wall clock |
| `symbols[]` | `Engine.Shard()` 各 symbol 的 `IsReadOnly` / `ReadOnlyReason` |

**性能**：`GetShardStatus` 为只读、无 WAL/网络 I/O；允许 Order 每 5s 轮询。

### 4.3 Matching Readiness（L1 加固）

K8s **readinessProbe** 建议调用 Admin gRPC 或 HTTP health（可选 `/ready`）：

- `consumer_running == true`
- `kafka_lag <= readiness_lag_threshold`（可与 Gate 共用配置思路，值可更宽松）

**livenessProbe** 保持轻量（进程存活即可）。  
Pod 崩溃后 K8s 在同 PVC 上重启 → WAL + Snapshot 恢复 → 续消费；Gate 在恢复期间保持 OPEN。

---

## 5. Order 侧变更

### 5.1 包结构

```text
internal/order/tradinggate/
  config.go      // 配置解析与默认值
  monitor.go     // 按 shard 轮询 GetShardStatus，展开 symbol 状态
  gate.go        // AssertAcceptNewOrder(symbol)、状态机
  redis_store.go // 多副本共享（可选：Monitor 写，Gate 读）
  metrics.go     // Prometheus
```

### 5.2 接入点

**PlaceOrder**（`internal/order/service/order.go` → `validatePlaceOrder`）：

```text
Shards.AssertPlaceOrder(symbol)     // 已有：迁移停牌
  → TradingGate.AssertAcceptNewOrder(symbol)   // 新增
  → InsertPending ...
```

**CancelOrder**：不调用 Gate。

**错误映射**（handler 已有 `ErrUnavailable` → gRPC `UNAVAILABLE` / HTTP 503）：

| 场景 | gRPC | HTTP | 业务码 |
|------|------|------|--------|
| symbol 熔断 | `UNAVAILABLE` | `503` | `TRADING_SUSPENDED` |
| Gate 未启用 / Redis 不可用 | 可配置 fail-open 或 fail-closed（**默认 fail-closed**） | | |

### 5.3 Monitor 与 shard 地址

Monitor 从 `shards.json`（`pkg/shardmgr`）读取：

```json
{
  "shard_id": "shard-0",
  "kafka_partition": 0,
  "node": "matching-shard-0-0",
  "symbols": ["BTC-USDT", "ETH-USDT"]
}
```

**gRPC 地址解析**（`order.json` 扩展）：

```json
{
  "matching": {
    "shards": [
      { "shard_id": "shard-0", "grpc_addr": "matching-shard-0:50061" }
    ],
    "dial_timeout_seconds": 3,
    "request_timeout_seconds": 2
  },
  "trading_gate": { ... }
}
```

若 `shards` 未配置，回退到现有单一 `matching.grpc_addr`（单 shard 部署）。

### 5.4 多 Order 副本：Redis 共享状态

**Key**：`trading:gate:symbol:{symbol}`  
**Value**（JSON）：

```json
{
  "state": "OPEN",
  "reason": "kafka_lag_exceeded",
  "shard_id": "shard-0",
  "kafka_lag": 12000,
  "updated_at_unix_ms": 1749254400000,
  "updated_by": "order-pod-abc"
}
```

**TTL**：`poll_interval_ms * 3`（Monitor 每次 tick 刷新 CLOSED/OPEN）

**读写策略**：

- Monitor（每 Pod 可运行，但建议 **leader 选举** 或 **均写同值**）：poll Matching → 计算 symbol 状态 → SET Redis
- PlaceOrder：GET Redis（miss 时读本地 cache；均无则 **fail-closed**）

> **简化 M1**：可先单写（每 Pod 均 poll + 写 Redis，幂等 SET）；后续可加 Redis lock 减重复 poll。

---

## 6. 配置规范

### 6.1 `order.json` — `trading_gate`

```json
{
  "trading_gate": {
    "enabled": true,
    "poll_interval_ms": 5000,
    "lag_open_threshold": 5000,
    "lag_close_threshold": 100,
    "stale_apply_seconds": 30,
    "admin_fail_threshold": 3,
    "normal_window_seconds": 30,
    "redis_key_prefix": "trading:gate:symbol",
    "fail_closed_on_redis_error": true
  }
}
```

| 字段 | 默认 | 说明 |
|------|------|------|
| `enabled` | `false` | 总开关 |
| `poll_interval_ms` | `5000` | Monitor 轮询间隔 |
| `lag_open_threshold` | `5000` | lag 超过则 OPEN（**按配置文件，环境可调**） |
| `lag_close_threshold` | `100` | lag 低于此值且其他条件满足才可恢复 CLOSED |
| `stale_apply_seconds` | `30` | 无命令 apply 超时（shard 级） |
| `admin_fail_threshold` | `3` | 连续 Admin 失败次数后 OPEN |
| `normal_window_seconds` | `30` | 恢复前须连续正常的时间窗 |
| `redis_key_prefix` | `trading:gate:symbol` | Redis key 前缀 |
| `fail_closed_on_redis_error` | `true` | Redis 不可读时是否拒新单 |

**环境建议**：

| 环境 | `lag_open_threshold` | `lag_close_threshold` |
|------|----------------------|------------------------|
| 开发 | 10000 | 500 |
| 压测 | 5000 | 100 |
| 生产 | 2000～5000（按 SLA 调） | 50～100 |

### 6.2 `matching.json` — readiness（可选）

```json
{
  "readiness": {
    "enabled": true,
    "max_lag": 1000
  }
}
```

---

## 7. API 契约变更

### 7.1 REST（Gateway 转发）

**熔断中下单**：

```http
HTTP/1.1 503 Service Unavailable
Content-Type: application/json

{
  "code": "TRADING_SUSPENDED",
  "message": "Trading temporarily suspended for BTC-USDT: matching backlog or unavailable",
  "data": {
    "symbol": "BTC-USDT",
    "reason": "kafka_lag_exceeded"
  }
}
```

**成功下单**（不变）：

```json
{
  "code": "OK",
  "data": {
    "order_id": "1000000001",
    "status": "PENDING",
    ...
  }
}
```

### 7.2 文档更新

- [rest-api.md](../rest-api.md) §1.3：补充 `TRADING_SUSPENDED` / 503 说明
- [architecture-spec.md](./architecture-spec.md) §4.6：补充 Gate 语义

---

## 8. 可观测性

### 8.1 Order Prometheus 指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `order_trading_gate_open` | Gauge `{symbol}` | 1=OPEN，0=CLOSED |
| `order_trading_gate_open_reason` | Gauge `{symbol,reason}` | 可选 info 型 |
| `order_trading_gate_poll_errors_total` | Counter `{shard_id}` | Admin  poll 失败 |
| `order_trading_gate_reject_total` | Counter `{symbol,reason}` | PlaceOrder 被拒次数 |

### 8.2 日志

Monitor OPEN/CLOSED 切换时：

```text
level=warn symbol=BTC-USDT state=OPEN reason=kafka_lag_exceeded lag=12000 threshold=5000 shard_id=shard-0
level=info  symbol=BTC-USDT state=CLOSED lag=50 normal_window_s=30
```

### 8.3 告警建议

| 告警 | 条件 |
|------|------|
| Symbol 长时间 OPEN | `order_trading_gate_open{symbol="BTC-USDT"}==1` 持续 > 5min |
| Matching Admin 不可达 | `order_trading_gate_poll_errors_total`  rate > 0 |
| lag 超 open 阈值 | `matching_kafka_lag` > 配置值（Matching 侧已有） |

---

## 9. Matching L1 HA（本方案范围）

### 9.1 模型

```text
┌─────────────────────────────────────────┐
│  K8s StatefulSet (replicas=1 per shard) │
│  Pod: matching-shard-0-0                │
│  PVC: WAL + Snapshot (ReadWriteOnce)    │
│  Consumer Group: 单实例消费 partition   │
└─────────────────────────────────────────┘
         │ crash / OOM / node loss
         ▼
  K8s 重启 Pod（同 PVC 或同 AZ 重挂载）
         │
         ▼
  WAL replay + Snapshot → seek Kafka offset → 续消费
         │
         ▼
  Trading Gate: lag 下降 + normal_window → CLOSED
```

### 9.2 RTO / RPO 预期

| 项 | 目标 | 说明 |
|----|------|------|
| **RTO** | 30s～2min | 取决于 WAL 增量与恢复 verify |
| **RPO** | 0（命令级） | Kafka + WAL fsync；已 commit offset 不丢 |
| **用户感知** | Gate OPEN 期间 503 | 无 silent PENDING 堆积 |

### 9.3 故障时间线（示例）

| 时间 | 事件 |
|------|------|
| T0 | Matching Pod 崩溃 |
| T+5s | Order Monitor 检测 Admin 失败 / lag 停涨 → symbol OPEN |
| T+5s～ | 新 PlaceOrder → 503；Cancel 仍可用 |
| T+30s～2min | Matching 恢复消费，lag 下降 |
| T+恢复+30s | normal_window 满足 → symbol CLOSED |
| T+ | Kafka 积压命令按序撮合（限价单；市价单需注意延迟成交价） |

### 9.4 本阶段不做的 HA

- 冷备 Standby Pod、S3 快照自动切换、跨 AZ PVC 复制（可列入后续 roadmap）

---

## 10. 与 Reconciler 的协作

| 场景 | Gate | Reconciler |
|------|------|------------|
| Matching 全挂 | OPEN，**无新 PENDING** | 存量 PENDING 在 Admin 不可达时仍 skip reject |
| Matching 恢复中 | OPEN（lag 高） | 可正常 GetOrderPresence |
| Matching 正常 | CLOSED | 60s 超时兜底 |

**Gate 上线后收益**：Matching 故障期间 **不再产生新 PENDING**，Reconciler 压力与用户困惑显著降低。  
**后续可选**：Matching 全挂超过 `pending_accept_timeout` 且 Gate 已 OPEN 时，Reconciler 在无 Admin 时也拒单（需单独 ADR，本方案不纳入）。

---

## 11. 测试计划

### 11.1 单元测试

- `gate.go`：状态机 OPEN/CLOSED、hysteresis、read_only 单 symbol
- `monitor.go`：shard 信号 → symbol 展开；Admin 失败计数
- `PlaceOrder`：Gate OPEN → `ErrUnavailable`

### 11.2 集成 / E2E

| 用例 | 步骤 | 期望 |
|------|------|------|
| Matching 停止 | `matching.sh stop` | PlaceOrder 503；Cancel 200 |
| Matching 恢复 | 重启 matching | lag 消化后 PlaceOrder 200 PENDING |
| 单 symbol read_only | 模拟对账失败 | 仅该 symbol 503 |
| 多 Order 副本 | 2× order pod | 两 Pod 均 503/均恢复 |
| lag 阈值 | 压测抬高 lag | 超 `lag_open_threshold` 后 503 |

### 11.3 演练脚本（建议）

`scripts/e2e/trading-gate.sh`：health → 下单 OK → stop matching → 下单 503 → start matching → 等待 CLOSED → 下单 OK。

---

## 12. 实施分期

| 阶段 | 内容 | 交付 |
|------|------|------|
| **M1** | `GetShardStatus` + Order Monitor + Gate + PlaceOrder 拦截 | 单 Pod 可演示 503 |
| **M2** | Redis 共享 + 指标 + 配置项 | 多副本 + 告警 |
| **M3** | REST 文档 + Gateway 503 体 + E2E 脚本 + checklist | 可合并主干 |
| **M4**（可选） | Matching readinessProbe | K8s 清单更新 |

**预估**：M1～M3 约 **7～10 人日**。

---

## 13. 风险与缓解

| 风险 | 缓解 |
|------|------|
| Gate 误 OPEN（lag 毛刺） | hysteresis + `normal_window_seconds` |
| Gate 误 CLOSED（Matching 假活） | 多信号 AND；`consumer_running` + lag + apply 时间 |
| Redis 故障导致全线拒单 | 监控 Redis；`fail_closed` 可配置（生产建议 true） |
| 共享 partition 下「一 symbol 慢拖全 shard」 | 后续热门 symbol 独占 partition；Gate 仍按 symbol 展示但共享 lag |
| 恢复后市价单延迟成交价偏差 | 文档告知；可选 Gate CLOSED 前要求 lag=0 |

---

## 14. architecture-spec 建议增补（评审通过后）

在 §4.5 后增加 **§4.8 交易闸门（Trading Gate）** 摘要：

1. Order 在 `PlaceOrder` 前查询 per-symbol 闸门；OPEN 时返回 503，不写入 Outbox。
2. 信号来自 Matching `GetShardStatus`；阈值由 `order.json` 配置。
3. Matching HA 采用 L1（StatefulSet + PVC + Gate），不引入双活消费。

在 §10 风险表增加：

| R6 | Matching 长时间宕机，Gate OPEN 期间用户无法下新单 | 预期行为；监控 `order_trading_gate_open`；加快 L1 恢复 |

---

## 15. 参考

- [architecture-spec.md](./architecture-spec.md) §4.3 Outbox、§4.5 补偿、§5 恢复
- [matching-api.md](../matching-api.md) — Matching 边界
- [benchmark.md](../benchmark.md) — `matching_kafka_lag` 稳态 ≈ 0
- 现有代码：`pkg/shardmgr.AssertPlaceOrder`、`internal/order/reconciler`、`internal/matching/admin/server.go`

---

**评审清单**

- [ ] Proto `GetShardStatus` 字段是否足够
- [ ] `lag_open_threshold` / `lag_close_threshold` 默认值
- [ ] Redis fail-closed vs fail-open 生产策略
- [ ] REST `TRADING_SUSPENDED` 响应体
- [ ] 是否与 shard 迁移 `halt_only_cancel` 优先级一致（Gate AND 迁移）
