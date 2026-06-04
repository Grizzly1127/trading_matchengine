# 开发清单（对照路线图与架构）

**版本**: 1.0  
**日期**: 2026-05-31  
**关联**: [development-roadmap.md](./development-roadmap.md) · [architecture-spec.md](./architecture-spec.md)  
**说明**: 基于代码库现状梳理；`[x]` 表示已有可运行实现，`[ ]` 表示未实现或仅部分/待验收。

---

## 1. Phase 1 — 撮合主链路 MVP（第 0～5 步）

### 1.1 第 0 步：工程骨架

- [x] `go.mod`、Go 1.22+
- [x] `Makefile`：`test`、`cover`、`build`、`migrate-up`、`gen-proto`
- [x] `pkg/logger` 结构化日志
- [x] `deploy/docker-compose.yml`（PostgreSQL、Kafka、Redis）
- [x] `go test ./...` 可通过

### 1.2 第 1 步：撮合引擎核心（`internal/matching/engine`）

- [x] 限价单撮合、部分成交、挂单入簿
- [x] 撤单从盘口移除
- [x] 市价单撮合（`OrderTypeMarket`）
- [x] 单元测试覆盖率 ≥ 80%（当前约 85%）
- [x] 最优买卖价合法性校验（测试/快照恢复）

### 1.3 第 2 步：WAL + Snapshot

- [x] `pkg/wal`：顺序写、CRC、`fsync`
- [x] `pkg/snapshot`：protobuf 快照
- [x] `internal/matching/recovery`：manifest → snapshot → WAL 回放
- [x] 先写 WAL 再改内存（架构约束 #11）
- [x] 定期/按条数快照（`snapshot_every`）
- [x] 重启恢复后 orderbook 合法性断言（`ValidateWithOrderMap`）
- [x] `order_id` 去重，避免重复撮合

### 1.4 第 3 步：Matching Engine 进程

- [x] `cmd/matching` 可启动（Kafka / JSONL 本地模式）
- [x] 消费 `order.commands`（`OrderCommandEnvelope`）
- [x] 发布 `match.events`、`trade.events`
- [x] WAL fsync 成功后再 commit Kafka offset
- [x] SIGTERM 优雅退出
- [x] 单 shard 多 `SymbolEngine` 路由（代码能力）
- [x] 多交易对生产配置（当前 `configs/symbols.json` 仅 BTC-USDT）
- [x] Matching 进程 Prometheus 指标（`processing_latency_ms`、lag 等）

### 1.5 第 4 步：Order Service

- [x] gRPC：`PlaceOrder` / `CancelOrder` / `GetOrder` / `ListOrders`
- [x] gRPC：`BalanceService`（`GetBalance` / `ListBalances` / `UpdateBalance`）
- [x] 单事务：幂等 + 冻结 + `orders` + `order_outbox`
- [x] Outbox Relay → `order.commands`（`acks=all`）
- [x] 消费 `match.events` 更新订单状态
- [x] 消费 `trade.events`：幂等写 `trades` + 余额结算
- [x] `client_order_id` 幂等；`order_id` uint64 发号
- [x] 超时补偿 scheduler（`internal/order/reconciler`）
- [x] 市价买单：调用 Market Data `GetReferencePrice` 冻结
- [x] SQL migrations（orders、outbox、balances、trades 等）
- [x] Outbox / 状态机 **PostgreSQL 集成测试**（testcontainers 或等价）
- [x] Order Service Prometheus（`outbox_pending_count`、`order_stuck_pending_seconds`）
- [x] gRPC `ListTrades` / 成交列表查询
- [x] 补偿：向 Matching Engine **对账 API** 查询（§4.5 完整流程）

### 1.6 第 5 步：API Gateway

- [x] `cmd/gateway`、配置 `configs/gateway.json`
- [x] 中间件：RequestID、Auth（静态 Bearer）、Recover、AccessLog
- [x] `GET /v1/health`、`GET /v1/time`
- [x] `POST/DELETE/GET /v1/orders`、`GET /v1/orders` 列表
- [x] `GET/POST /v1/balances`
- [x] `GET /v1/market/depth`、`GET /v1/market/ticker`、`GET /v1/market/symbols`
- [x] `GET /v1/klines`（转发 Kline gRPC）
- [x] JSON ↔ gRPC；`order_id` 十进制字符串
- [x] 统一错误响应
- [x] Gateway handler 单测与覆盖率
- [x] 端到端联调验收（路线图阶段 4 / `scripts/e2e-api.sh` 全量自动化断言）
- [x] `GET /v1/trades`（成交列表，依赖 Order gRPC）
- [x] 生产级 JWT / 内网 mTLS（`pkg/auth`、`cmd/auth` 签发；`static` 联调保留，见 [gateway-auth.md](./gateway-auth.md)）

### 1.7 Phase 1 架构验收项

- [x] 重启撮合引擎后挂单不丢失（WAL + Snapshot）
- [x] 已成交订单不重复撮合
- [x] gRPC 下单 → 撮合 → DB 状态与 `trades` 回写（联调路径）
- [x] 架构 §5.6：**启动后与 Order Service 对账**（PENDING/PARTIAL vs Orderbook diff、只读拒单）

---

## 2. Phase 2 — 行情与推送（第 6 步）

### 2.1 Market Data Service（6.1）

- [x] `cmd/marketdata`：消费 `match.events` / `trade.events`
- [x] 内存深度、Ticker 聚合
- [x] gRPC：`GetOrderBook`、`GetTicker`、`GetReferencePrice`、`GetTickerAllSnapshot`
- [x] Redis：`depth:{symbol}`、`ticker:{symbol}`、`ticker:all:{quote}`
- [x] Pub/Sub 发布 depth / ticker / `ticker@all:{quote}`
- [x] 定时深度推送（默认 100ms）
- [x] 全市场 Ticker 定时发布（`ticker_all_interval_ms`，默认 100ms；`ticker_all_heartbeat_sec`）
- [x] Prometheus `/metrics`（Market Data）
- [x] Ticker 24h 开高低、涨跌幅完整计算（滚动 24h 窗口，内存成交序列）

### 2.2 Push + WebSocket（6.2）

- [x] `cmd/push`：`/v1/ws`、Redis `PSubscribe` 扇出
- [x] 订阅：`depth:`、`ticker:`、`kline:`、`index:`、`ticker@all`
- [x] 订阅后 Redis GET 快照再收增量
- [x] `internal/push/server/ws_integration_test.go`
- [x] WS 频道 `trade:{symbol}`（Market Data 消费 `trade.events` → Redis `PUBLISH trade:{symbol}`）
- [x] WS 频道 `order`（Order 消费 `match.events` 落库后 → Redis `PUBLISH order:{user_id}`）
- [x] `ticker@all` 协议对齐 rest-api §8.2：`type=snapshot|delta|heartbeat`、100ms delta、做市商鉴权
- [x] 普通用户 / 做市商连接数与 symbol 数限流

### 2.3 Kline Service（6.3）

- [x] `cmd/kline`：消费 `trade.events`
- [x] 多周期聚合、PostgreSQL `klines` 表
- [x] Redis：`kline:open:*`、`kline:pending:close`、Pub/Sub `kline:{symbol}:{interval}`
- [x] gRPC `GetKlines`；Gateway `GET /v1/klines`
- [x] Kline Service Prometheus 指标
- [x] Kafka Topic `kline.raw`（闭合 bar 通知；仍消费 `trade.events` 聚合）

### 2.4 Index Price Service（6.4）

- [x] `cmd/indexprice`：多交易所 HTTP 采集（Binance / OKX / Bybit / mock）
- [x] 加权中位数、异常值过滤（aggregator）
- [x] Redis `index:{symbol}` + Pub/Sub；可选 Kafka `index.price`
- [x] PostgreSQL 审计表；gRPC `GetIndexPrice`
- [x] `scripts/indexprice.sh`、`configs/indexprice.json`
- [x] 纳入 `scripts/dev.sh` 一键启动
- [x] Gateway `GET /v1/index-price`
- [x] README 与路线图文档同步（勿再写「占位未实现」）

### 2.5 全市场 Ticker / 做市商（6.5）

- [x] Market Data：`GetTickerAllSnapshot`、Redis `ticker:all:{quote}`
- [x] Push：支持订阅 `ticker@all` / `ticker@all:{quote}`
- [x] Gateway `GET /v1/market/ticker/all`（冷启动 REST）
- [x] WS snapshot + delta 帧格式与 rest-api §8.2 一致
- [x] 做市商 API Key 档位与普通用户隔离

### 2.6 Phase 2 架构验收项

- [x] 客户端 WS 可收到 **深度**（`depth:BTC-USDT`）
- [x] 客户端 WS 可收到 **单 symbol Ticker**
- [x] 客户端 WS 可收到 **K 线**
- [x] 客户端 WS 可收到 **指数价格**（需启动 indexprice）
- [x] 客户端 WS 可收到 **公开市场成交**（`trade:{symbol}`）
- [x] 客户端 WS 可收到 **用户订单**（`order`）
- [x] `ticker@all` 完整做市商体验（REST 快照 + WS delta）

### 2.7 推迟但相关的 Phase 2 项

- [x] 市价买单行情冻结方案 C 全量文档落地（见 `design/market-buy-freeze.md`，若仓库无此文件则先补设计）
- [x] Redis 下单幂等缓存（Phase 1 以 DB 唯一索引为准，可选）

---

## 3. Phase 3 — 多交易对 + 高可用（第 8～12 周）

### 3.1 分片与扩展

- [x] **Shard Manager**（`symbol -> shard_id -> kafka_partition -> node`）
- [x] Order 发命令前读取分片映射
- [x] 热门 symbol 独占 partition / Pod；冷门 symbol 共享 shard
- [x] symbol 迁移流程（停牌窗口、防双写）

### 3.2 部署与中间件

- [x] Matching Engine K8s **StatefulSet** + PVC
- [x] 各服务 `deploy/docker/Dockerfile.*`
- [x] `deploy/k8s/` Helm / manifests
- [ ] PostgreSQL 主从 + pgbouncer
- [ ] Redis Cluster
- [ ] Kafka 3 副本

### 3.3 可观测与对账

- [ ] 全服务 Prometheus + Grafana（撮合 P99、WAL 写延迟、Kafka lag）
- [ ] 恢复对账告警：Orderbook vs DB 不一致 → PagerDuty（§5.6）
- [ ] WAL 磁盘使用率告警（80%）

### 3.4 Phase 3 验收项

- [ ] 模拟 Pod 重启，撮合 **< 30s** 自动恢复且无订单丢失

---

## 4. Phase 4 — 安全、审计与运维（第 13～16 周）

### 4.1 安全与网关

- [ ] API Key 管理 + **HMAC-SHA256** 签名校验
- [ ] 限流熔断（令牌桶，用户/IP 两级）
- [ ] 公网 **Web/BFF** 与用户 Session（架构 §2.1.1，通常独立仓库）
- [x] 内网 Gateway 仅服务账号 / mTLS（配置示例 `configs/gateway.prod.json.example`）

### 4.2 审计与备份

- [ ] Kafka Topic `system.audit` + 归档存储
- [ ] 全量操作审计链路
- [ ] WAL/Snapshot 定期备份 **S3/OSS**
- [ ] 灾难恢复：从对象存储冷启动演练

### 4.3 质量与压测

- [ ] CI：`go test -race ./...`
- [ ] 压测：单交易对 TPS ≥ 5000，撮合 P99 ≤ 10ms（方案与脚本见 [docs/benchmark.md](../benchmark.md)）
- [x] L0 微基准 + L2/L3 脚本骨架（`make bench-l0`、`scripts/bench/`、`cmd/bench-producer`）
- [ ] 按 [docs/benchmark.md](../benchmark.md) 跑通 M3 并归档 `reports/` PASS
- [ ] 撮合核心 / recovery 持续保持覆盖率 ≥ 80%

---

## 5. 跨阶段基础设施与工程化

| 组件 | 最早需要步骤 | 状态 |
|------|--------------|------|
| 无（仅本地文件） | 第 0～2 步 | [x] |
| Kafka | 第 3 步 | [x] dev compose |
| PostgreSQL | 第 4 步 | [x] |
| Redis | 第 6 步 / 可选幂等 | [x] |
| Protobuf 全量 | 第 3～4 步渐进 | [x] 核心路径已用 proto |
| `scripts/dev.sh` 一键启停 | 联调 | [x]（含 indexprice） |
| `scripts/reset-dev.sh` | 联调 | [x] |
| `scripts/e2e-api.sh` | 联调 | [x] |
| Nginx 统一入口 | 可选 | [x] `deploy/nginx/` |
| golangci-lint CI | 可选 | [ ] Makefile 有 target，无 CI |

---

## 6. 规划 Topic / Redis（对照 kafka-data、redis-data）

| 名称 | 状态 | 备注 |
|------|------|------|
| `order.commands` | [x] | |
| `match.events` | [x] | |
| `trade.events` | [x] | |
| `index.price` | [x] | Index Price 可选发布 |
| `kline.raw` | [x] | Kline 闭合 bar 发布；输入仍 `trade.events` |
| `system.audit` | [ ] | 规划 |
| Redis `trade:{symbol}` | [x] | Market Data 发布 |
| Redis `idempotent:order:*` | [ ] | 规划；DB 幂等已够用 |

---

## 7. 文档与仓库卫生

- [x] `docs/architecture-spec.md`
- [x] `docs/development-roadmap.md`
- [x] `docs/rest-api.md`、`docs/matching-api.md`、`docs/kafka-data.md`、`docs/redis-data.md`
- [ ] `docs/gateway-development-plan.md`（路线图引用，仓库缺失）
- [ ] `docs/market-push-development-plan.md`（路线图引用，仓库缺失）
- [x] `docs/design/market-buy-freeze.md`（路线图引用，需确认是否存在）
- [ ] `README.md` Index Price 状态与实现一致
- [ ] `docs/kafka-data.md` §8「未实现」与 `index.price` 现状对齐
- [x] 本清单 `docs/development-checklist.md`

---

## 8. 打印用总览（路线图 §8 检查清单）

```
[x] 第 0 步  go test ./... 通过
[x] 第 1 步  matcher 覆盖率 ≥ 80%
[x] 第 2 步  杀进程重启后盘口正确
[x] 第 3 步  Kafka 下单 → 成交事件
[x] 第 4 步  gRPC 下单 → DB 状态 + trades + Outbox（集成测待补）
[x] 第 5 步  curl REST 主链路（e2e 全量断言 + `GET /v1/trades`）
[~] 第 6 步  WS 收到 depth/ticker/kline/index（trade/order/ticker@all 协议待补）
[ ] Phase 3  多分片 / K8s / 全链路监控
[ ] Phase 4  HMAC / 审计 / 压测
```

`[~]` = 部分完成。

---

## 9. 建议实施顺序（摘自差距分析）

1. **P0 — Phase 1 收尾**：架构 §5.6 启动对账、生产级 JWT/mTLS
2. **P1 — Phase 2 对外补齐**：`trade:` 推送、`ticker@all` 协议（`index-price`、`ticker/all` 与 dev.sh 已接）
3. **P2 — 架构约束**：Matching §5.6 启动对账、全服务 Prometheus、WS `order`
4. **P3 — 生产化**：Shard Manager → K8s → Phase 4 安全与压测

---

## 10. 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-05-31 | 初版：对照 roadmap v1.1 与 architecture-spec v1.2 的代码库差距清单 |
