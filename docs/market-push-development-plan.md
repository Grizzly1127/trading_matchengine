# 第 6 步：行情与推送开发计划（Phase 2）

**版本**: 1.0  
**日期**: 2026-05-26  
**状态**: 待开发  
**预估工期**: 3～4 周（单人全职；可并行子模块时压缩至约 3 周）  
**关联**: [development-roadmap.md §第 6 步](./development-roadmap.md#第-6-步行情与推送phase-2约-3-4-周) · [architecture-spec.md](./architecture-spec.md) · [rest-api.md](./rest-api.md) · [design/market-buy-freeze.md](./design/market-buy-freeze.md) · [gateway-development-plan.md](./gateway-development-plan.md)

本文档是路线图 **第 6 步** 的可执行开发计划：按 **6.1 → 6.5** 拆分任务、目录、契约与验收，与架构 Phase 2 及对外 REST/WebSocket 契约对齐。

---

## 1. 目标与前置条件

### 1.1 目标（架构 Phase 2 验收）

> 客户端订阅 WebSocket 后，**实时**收到深度、成交、K 线、指数价格；REST 可查询深度/Ticker/K 线/指数价。

对应 [rest-api.md](./rest-api.md) §4（行情）、§5（K 线）、§6（指数价）、§8（WebSocket）。

### 1.2 前置条件（必须已满足）

| 项 | 说明 | 自检 |
|----|------|------|
| 第 3 步 Matching | 稳定发布 `match.events`、`trade.events` | `go run ./cmd/matching` + 下单可见 Kafka 事件 |
| 第 4 步 Order | 消费 match/trade，DB 状态与成交正确 | `trade_id` 幂等、余额结算 |
| 第 5 步 Gateway | REST 订单链路可用 | `curl` 下单/查单 |
| 基础设施 | Kafka + Redis + PostgreSQL | `docker compose -f deploy/docker-compose.yml up -d` |
| 固定联调交易对 | 建议 `BTC-USDT` | 与文档示例一致 |

### 1.3 本步不做（明确推迟）

| 项 | 归属 | 原因 |
|----|------|------|
| API Key + HMAC 全量鉴权 | Phase 4 | 第 6 步可用 Bearer + 配置「做市商 token 列表」模拟 |
| Push 多副本会话迁移（Redis 存连接态） | Phase 3 | 先单实例 Push；Gateway 反代到单 Push |
| `order` WS 频道（私有订单推送） | 可与 6.2 并行 stub | 依赖 Order 发 Redis/Kafka；MVP 可仅 REST 轮询 |
| Protobuf WS 全量帧 | 6.5 做市商路径优先 | 零售 JSON 先通 |
| Shard Manager / 多 symbol 分片 | Phase 3 | 内存表按 symbol 隔离即可，Kafka 仍可按单 partition 消费 |
| 生产级限流、独立做市商域名 | Phase 3+ | 配置项预留 |

---

## 2. 总体架构与数据流

```
Matching Engine
  ├─ match.events  ──▶ Market Data（Orderbook 镜像、深度 diff）
  └─ trade.events  ──▶ Market Data（Ticker 24h）
                    ──▶ Kline（OHLCV）
                    ──▶（可选）Push trade:{symbol} 直转

Market Data
  ├─ 内存：per-symbol Orderbook 镜像 + Ticker 表 + 全市场 Ticker 聚合
  ├─ Redis Key：depth:{symbol} 快照、ticker:{symbol}、ticker:all:{quote}
  ├─ Redis Pub/Sub：depth:{symbol}、ticker:{symbol}、ticker@all:{quote}
  └─ gRPC：GetOrderBook、GetTicker、GetReferencePrice（供 Gateway / Order）

Kline Service
  ├─ 消费 trade.events
  ├─ Redis Hash：未闭合 Bar
  ├─ PostgreSQL：闭合 K 线
  └─ Redis Pub/Sub：kline:{symbol}:{interval}

Index Price Service
  ├─ 外部 HTTP 拉价（Binance/OKX/Bybit 等）
  ├─ Redis：index:{symbol}
  └─ Redis Pub/Sub：index:{symbol}（或统一 channel）

Push Service
  └─ 订阅 Redis Pub/Sub → 按客户端订阅路由 → WebSocket 帧

API Gateway
  ├─ REST：/v1/market/*、/v1/klines、/v1/index-price（gRPC 或读 Redis）
  └─ WS：/v1/ws（升级连接，转发至 Push 或内嵌 hub，二选一见 §4.2）
```

**设计约束**（与 [architecture-spec.md](./architecture-spec.md)、仓库 SLA 一致）：

- Market Data / Kline / Index Price **不**访问 Order/Matching 的数据库。
- 消费 Kafka：**处理成功后再 commit offset**（复用 `pkg/kafka` + `internal/order/consumer` 模式）。
- 行情聚合在 **独立进程**；非撮合热路径，但仍避免无界 channel、热路径反射。
- 深度/Ticker 推送允许 **100ms 级** 合并；不追求撮合纳秒级延迟。

---

## 3. 子模块拆分（6.1～6.5）

| 顺序 | 模块 | 核心交付 | 阻塞关系 |
|------|------|----------|----------|
| **6.1** | Market Data Service | 深度、Ticker、gRPC、Redis、参考价 | 阻塞 6.2 REST 行情、6.5 全市场、市价买冻结 |
| **6.2** | Push + Gateway WS | `depth:`、`ticker:`、`trade:` | 依赖 6.1 的 Redis Pub/Sub |
| **6.3** | Kline Service | 消费 trade、落库、WS `kline:` | 可与 6.2 并行（仅依赖 trade.events） |
| **6.4** | Index Price Service | 外部价、REST/WS 指数 | 可与 6.1 并行 |
| **6.5** | `ticker@all` | 全市场快照 + delta、做市商鉴权 | 依赖 6.1 全市场聚合 |

**推荐实施顺序**：`6.1 → 6.2 → 6.3` 与 `6.4` 并行 → `6.5` → 收尾（Order 市价买方案 C、文档）。

---

## 4. 目录与基础设施

### 4.1 新建目录（对齐 architecture-spec §7）

| 路径 | 职责 |
|------|------|
| `cmd/marketdata/main.go` | Market Data 入口（当前仅占位 package，需改为 `main`） |
| `cmd/kline/main.go` | Kline 入口 |
| `cmd/indexprice/main.go` | Index Price 入口 |
| `cmd/push/main.go` | Push 入口（或 Gateway 内嵌 WS hub，见下） |
| `internal/marketdata/consumer/` | Kafka match + trade 消费 |
| `internal/marketdata/aggregator/` | Orderbook 镜像、Ticker、全市场表 |
| `internal/marketdata/handler/` | gRPC server |
| `internal/marketdata/publisher/` | 写 Redis Key + Pub/Sub |
| `internal/kline/aggregator/` | 时间窗口 OHLCV |
| `internal/kline/repository/` | `klines` 表 |
| `internal/kline/consumer/` | trade.events |
| `internal/kline/publisher/` | Redis Pub/Sub |
| `internal/indexprice/collector/` | 外部 HTTP |
| `internal/indexprice/aggregator/` | 过滤 + 加权中位数 |
| `internal/indexprice/publisher/` | Redis + 可选 Kafka |
| `internal/push/hub/` | WS 连接、订阅表 |
| `internal/push/subscriber/` | Redis 订阅 → 广播 |
| `internal/gateway/handler/market.go` | REST 行情代理 |
| `internal/gateway/handler/ws.go` | WS 升级与协议 |
| `internal/gateway/client/marketdata_grpc.go` | Market Data gRPC |
| `proto/marketdata/v1/marketdata.proto` | gRPC 契约 |
| `proto/kline/v1/kline.proto` | gRPC 契约 |
| `proto/indexprice/v1/indexprice.proto` | gRPC 契约 |
| `pkg/redis/` | **新建**：连接、Pub/Sub、JSON 序列化 helper |
| `configs/marketdata.json` | 各服务配置 |
| `configs/kline.json` | |
| `configs/indexprice.json` | |
| `configs/push.json` | |

### 4.2 Push 部署形态（二选一，当前采用 B）

| 方案 | 说明 | 适用 |
|------|------|------|
| **A. 独立 `cmd/push`** | Gateway 只做 HTTP；WS 客户端连 Push 端口（或 Nginx 按路径分流） | 后续水平扩展可切换 |
| **B. Gateway 内嵌 `internal/push/hub`** | 单进程，联调简单 | 本地 MVP；后续再拆 |

**当前决策**：先使用 **B（Gateway 内嵌）**，由 Gateway 直接暴露 `/v1/ws`；`internal/push/*` 作为可复用模块保留，后续如需独立扩容再切回 A。

### 4.3 需新增的 pkg

| 包 | 内容 |
|----|------|
| `pkg/redis` | `Client`、`Publish`、`Subscribe`、`Set`/`Get`（带 context 超时） |
| `pkg/postgres` | （可选）Kline 若复用 Order 的 pgx 模式可暂缓 |

`Makefile` 增加：`build-marketdata`、`build-kline`、`build-indexprice`、`build-push`；`scripts/gen-proto.sh` 纳入新 proto。

### 4.4 数据库迁移（Kline + Index 审计）

| 表 | 服务 | 要点 |
|----|------|------|
| `klines` | Kline | 主键 `(symbol, interval, open_time)`；索引 `(symbol, interval, open_time DESC)` |
| `index_prices` | Index Price | 审计历史，保留 30 天（可定时 job 删除） |

迁移文件建议：`internal/kline/repository/migrations/` 或根目录 `migrations/` 与 Order 区分前缀。

---

## 5. 契约设计（实现前冻结）

### 5.1 Kafka 消费

| Topic | 消费者 | 用途 |
|-------|--------|------|
| `match.events` | Market Data | `ORDER_ACCEPTED` / 撤单 / 部分成交时更新 Orderbook 镜像（结合 `order` 字段或后续 depth 专用事件） |
| `trade.events` | Market Data、Kline | Ticker `last_price`/24h 量；K 线 OHLCV |
| `trade.events` | Push（可选） | 公开市场成交 `trade:{symbol}`，减少一跳 |

**消费组建议**：

- `marketdata-match-{instance}`
- `marketdata-trade-{instance}`
- `kline-trade-{instance}`

**幂等**：按 `(topic, partition, offset)` 或业务键 `trade_id` / `wal_seq` 去重（内存 LRU + 可选 Redis SET，防止重启重复累加 24h volume）。

> **实现说明**：当前 `MatchEvent` 带 `common.v1.Order`，可用于维护挂单变化；全量深度以 **周期快照 + 增量 diff** 为主（architecture-spec §3.4）。若 match 事件不足以表达每笔挂单变动，MVP 可 **仅 trade 更新 Ticker**，深度以 **定时从镜像 orderbook 全量 diff**（接受最多 100ms 延迟）。

### 5.2 Redis Key / Channel 命名

| 类型 | Key / Channel | 内容 |
|------|---------------|------|
| Key | `depth:{symbol}` | 最新深度 JSON 快照 + `last_update_id` |
| Key | `ticker:{symbol}` | 单 symbol Ticker JSON |
| Key | `ticker:all:{quote_asset}` | 全市场快照 blob（JSON/protobuf） |
| Key | `kline:open:{symbol}:{interval}` | Hash：未闭合 bar |
| Key | `index:{symbol}` | 指数价 + ts + sources |
| Pub/Sub | `depth:{symbol}` | 深度 snapshot/delta 消息 |
| Pub/Sub | `ticker:{symbol}` | 单对 Ticker 更新 |
| Pub/Sub | `ticker@all:{quote_asset}` | 全市场 delta（[rest-api.md §8.2](./rest-api.md#82-全市场-tickertickerall做市商)） |
| Pub/Sub | `kline:{symbol}:{interval}` | K 线 bar 更新 |
| Pub/Sub | `index:{symbol}` | 指数价更新 |
| Pub/Sub | `trade:{symbol}` | 可选，成交广播 |

Pub/Sub 消息体建议统一信封：

```json
{
  "type": "snapshot|delta",
  "symbol": "BTC-USDT",
  "seq": 10293485721,
  "ts": 1716192000123,
  "data": { }
}
```

### 5.3 gRPC（Market Data）— 最小接口集

```protobuf
service MarketDataService {
  rpc GetOrderBook(GetOrderBookRequest) returns (GetOrderBookResponse);
  rpc GetTicker(GetTickerRequest) returns (GetTickerResponse);
  rpc GetReferencePrice(GetReferencePriceRequest) returns (GetReferencePriceResponse);
  // 6.5 可选：仅内部，Gateway 读 Redis 时不暴露
  rpc GetTickerAllSnapshot(GetTickerAllSnapshotRequest) returns (GetTickerAllSnapshotResponse);
}
```

- **GetOrderBook**：对齐 [rest-api.md §4.1](./rest-api.md#41-深度order-book)（`bids`/`asks` 字符串对、`last_update_id`）。
- **GetTicker**：单 symbol 或批量（≤100）。
- **GetReferencePrice**：供 [market-buy-freeze.md](./design/market-buy-freeze.md)：`BEST_ASK` → `MARK` → `LAST` 降级；返回 `updated_at_ms`。

### 5.4 WebSocket 协议（Gateway / Push）

实现 [rest-api.md §8](./rest-api.md#8-websocket)：

| op | 行为 |
|----|------|
| `auth` | Bearer / API Key（做市商列表配置） |
| `subscribe` / `unsubscribe` | 频道列表 |
| `ping` / `pong` | 30s 心跳 |

频道 MVP 顺序：`depth:{symbol}` → `ticker:{symbol}` → `trade:{symbol}` → `kline:{symbol}:{interval}` → `index:{symbol}` → `ticker@all:{quote}`（6.5）。

---

## 6. 分步开发任务

### 6.1 Market Data Service（第 1～1.5 周）

#### 6.1.1 任务清单

- [x] `proto/marketdata/v1/marketdata.proto` + `make gen-proto`
- [x] `pkg/redis` 基础封装 + 单测（miniredis 或 docker Redis）
- [x] `internal/marketdata/consumer`：订阅 `trade.events`，更新 per-symbol Ticker（`last_price`、累计 `volume`）
- [x] `internal/marketdata/aggregator/orderbook.go`：消费 `match.events` 维护买卖盘镜像（可与 engine 侧 level 结构对齐，使用 `decimal` / 字符串）
- [x] 每 **100ms** 深度快照：写 Redis Key / Pub/Sub `depth:{symbol}`（MVP 先发 snapshot，diff 后续优化）
- [x] gRPC `GetOrderBook` / `GetTicker` / `GetReferencePrice`
- [x] 进程：`cmd/marketdata/main.go`、配置、metrics（消费事件、Redis 发布计数；Prometheus 后续接入）
- [x] **全市场 Ticker 内存表**（为 6.5 预埋）：按 `quote_asset` 分桶，每 100～500ms 刷 Redis `ticker:all:USDT`

#### 6.1.2 参考价（市价买冻结）

完成 6.1 后执行 [market-buy-freeze.md §8](./design/market-buy-freeze.md#8-实现清单market-data-就绪后)：

- [x] Order：`internal/order/marketdata` gRPC client
- [x] `ComputeFreeze` 市价买分支；去掉「必须传 price」
- [x] migration：`freeze_price` / `frozen_amount` 等
- [x] 集成测：mock MD；MD 不可用时 `Unavailable`

#### 6.1.3 验收

```bash
# 启动：matching + order + marketdata + redis + kafka
grpcurl -plaintext localhost:50052 marketdata.v1.MarketDataService/GetOrderBook \
  -d '{"symbol":"BTC-USDT","limit":20}'
grpcurl ... GetReferencePrice -d '{"symbol":"BTC-USDT","kind":"BEST_ASK"}'
redis-cli GET ticker:BTC-USDT
# 下单成交后 Ticker last_price 变化；深度 bids/asks 与撮合侧一致（抽样对比）
```

---

### 6.2 Push Service + Gateway WS（第 1.5～2.5 周）

#### 6.2.1 任务清单

- [x] `internal/push/hub`：连接注册、每连接订阅集合、写缓冲上限（防慢客户端拖垮）
- [x] `internal/push/subscriber`：按 channel 模式订阅 Redis（`PSUBSCRIBE depth:*` 等）
- [x] 协议：`subscribe` 确认、`snapshot` 首包（深度/Ticker 从 Redis Key 读）、后续增量广播
- [x] Gateway：`GET /v1/market/depth`、`/v1/market/ticker` → Market Data gRPC
- [x] Gateway：`GET /v1/ws` WebSocket 升级（入口在 Gateway，连接/订阅/广播逻辑复用 push 模块）
- [x] 单测/集成测：hub 路由 + Redis publish → WS 客户端收到

#### 6.2.2 深度频道语义

1. 客户端 `subscribe depth:BTC-USDT`
2. 服务端推送 **snapshot**（来自 `depth:BTC-USDT` Key）
3. 之后推送 **delta**（`last_update_id` 单调递增；客户端丢包则重订阅拉 snapshot）

#### 6.2.3 验收

```bash
# websocat / wscat
wscat -c "ws://localhost:8080/v1/ws"
> {"op":"auth","args":["<token>"]}
> {"op":"subscribe","args":["depth:BTC-USDT","ticker:BTC-USDT"]}
# 另终端撮合成交 → 收到 depth/ticker 帧
```

---

### 6.3 Kline Service（第 2～3 周，可与 6.2 并行）

#### 6.3.1 任务清单

- [ ] `proto/kline/v1/kline.proto`：`GetKlines`
- [ ] migration：`klines` 表
- [ ] 消费 `trade.events`，按 symbol + interval 聚合（MVP 先做 `1m`、`1h`、`1d`）
- [ ] Redis 未闭合 bar；窗口闭合时 `INSERT` PostgreSQL
- [ ] Pub/Sub `kline:{symbol}:{interval}`
- [ ] Gateway：`GET /v1/klines` → Kline gRPC
- [ ] WS：`kline:BTC-USDT:1m` 订阅

#### 6.3.2 聚合规则要点

- 使用成交事件 **成交时间**（若无则用消费时间，文档注明）。
- 同一 bar 内：`high = max`，`low = min`，`close = 最后一笔`，`volume += qty`。
- 重启：从 Redis 恢复未闭合 bar；缺失时从 DB 最后闭合 bar 之后重放 Kafka（需记录 consumer offset 与 bar 对齐策略，MVP 可接受「重启后仅 forward 新 trade」并文档说明限制）。

#### 6.3.3 验收

```bash
curl "http://localhost:8080/v1/klines?symbol=BTC-USDT&interval=1m&limit=10"
# WS 订阅 kline:BTC-USDT:1m，连续成交后收到 bar 更新
```

---

### 6.4 Index Price Service（第 2～3 周，可与 6.1 并行）

#### 6.4.1 任务清单

- [ ] `internal/indexprice/collector`：2～3 个交易所 REST ticker（配置 API base URL，无需密钥的公开接口）
- [ ] `aggregator`：偏差 >3% 剔除，加权中位数
- [ ] 每秒（可配置）写 Redis `index:{symbol}`，Pub/Sub 推送
- [ ] 可选：写 `index_prices` 审计表
- [ ] gRPC `GetIndexPrice` + Gateway `GET /v1/index-price`
- [ ] WS：`index:BTC-USDT` 或统一 `index` 频道（与 rest-api 对齐后二选一）

#### 6.4.2 失败策略

- 某数据源超时：跳过该源，不少于 2 个有效源才输出指数。
- 全部失败：保留上一帧 Redis 值，metrics 告警；REST 返回 `503` 或带 `stale=true`（实现前在 rest-api 补充一句）。

#### 6.4.3 验收

```bash
curl "http://localhost:8080/v1/index-price?symbol=BTC-USDT"
# WS 订阅后每秒级收到更新（视配置）
```

---

### 6.5 全市场 Ticker `ticker@all`（第 3～4 周）

#### 6.5.1 任务清单

- [ ] Market Data：内存全市场表 + `snapshot_id` 生成（单调递增或内容哈希）
- [ ] 每 **100ms** diff → Redis Pub/Sub `ticker@all:USDT`
- [ ] 写 Redis `ticker:all:USDT`（与 WS snapshot **同源**）
- [ ] Gateway：`GET /v1/market/ticker/all` **优先读 Redis**；`If-None-Match` / `snapshot_id` → 304
- [ ] WS：`ticker@all` / `ticker@all:USDT`；subscribe 后 **先 snapshot 再 delta**
- [ ] 鉴权：配置 `market_maker_tokens`；普通 token 订阅 `ticker@all` 返回错误 op
- [ ] 可选：`encode=protobuf`（做市商）；MVP 可仅 JSON

#### 6.5.2 验收

```bash
curl -s "http://localhost:8080/v1/market/ticker/all?quote_asset=USDT" -H "Authorization: Bearer <mm-token>"
wscat -c ws://localhost:8080/v1/ws
> {"op":"subscribe","args":["ticker@all:USDT"]}
# 收到 snapshot + 100ms delta；snapshot_id 与 REST 一致
```

---

## 7. Gateway REST 扩展一览

| 方法 | 路径 | 上游 | 步驟 |
|------|------|------|------|
| GET | `/v1/market/depth` | Market Data gRPC | 6.1 |
| GET | `/v1/market/ticker` | Market Data gRPC | 6.1 |
| GET | `/v1/market/ticker/all` | Redis（MD 写入） | 6.5 |
| GET | `/v1/market/symbols` | 静态配置或 MD | 6.1 |
| GET | `/v1/klines` | Kline gRPC | 6.3 |
| GET | `/v1/index-price` | Index Price gRPC | 6.4 |
| WS | `/v1/ws` | Push / 内嵌 hub | 6.2+ |

`configs/gateway.json` 增加：`marketdata_grpc_addr`、`kline_grpc_addr`、`indexprice_grpc_addr`、`redis_addr`（ticker/all 用）、`push_ws_url`（若 WS 独立）。

---

## 8. 测试策略

| 层级 | 范围 | 工具 |
|------|------|------|
| 单元 | 聚合、diff、24h 滚动窗口、指数过滤 | `go test ./internal/marketdata/...` |
| 集成 | Kafka → MD → Redis → WS | testcontainers 或 compose + 脚本 |
| 端到端 | 下单 → 成交 → WS 收到 ticker/depth | `curl` + `wscat` |
| 回归 | Order 市价买冻结 | `internal/order/repository/freeze_test.go` + mock MD |

**每个包先写 `*_test.go`**（与路线图 §7 一致）；禁止仅靠手工 `main` 验证。

---

## 9. 可观测性（第 6 步最小集）

| 指标 | 服务 |
|------|------|
| `kafka_consumer_lag` | MD、Kline |
| `marketdata_aggregate_duration_ms` | MD |
| `redis_publish_errors_total` | MD、Kline、Index |
| `ws_connections_active` | Push |
| `ws_broadcast_dropped_total` | Push（慢客户端丢弃） |
| `index_price_sources_up` | Index |

日志：结构化，带 `symbol`、`shard`（若有）、`request_id`（Gateway）。

---

## 10. 建议周计划（3～4 周）

| 周 | 交付 |
|----|------|
| **W1** | `pkg/redis`、proto MD、MD 消费 trade + Ticker、gRPC GetTicker/GetReferencePrice；Gateway `/v1/market/ticker` |
| **W2** | MD 深度镜像 + depth Redis/Pub/Sub；Push hub + WS `depth`/`ticker`；Gateway `/v1/market/depth`、`/v1/ws` |
| **W3** | Kline 1m/1h/1d + REST/WS；Index Price + REST/WS；Order 市价买方案 C |
| **W4** | `ticker@all` REST+WS、做市商鉴权、304/protobuf 可选、端到端压测与文档修订 |

---

## 11. 端到端联调顺序

1. `docker compose -f deploy/docker-compose.yml up -d`
2. `matching` → `order` → 充值 → 下一笔限价单确认 trade.events
3. 启动 `marketdata` → 验证 gRPC/Redis
4. 启动 Gateway WS → 订阅 `ticker:BTC-USDT` → 再成交一笔 → 帧更新
5. 启动 `kline` → `GET /v1/klines` + WS kline
6. 启动 `indexprice` → REST/WS 指数
7. 启用 `ticker@all:USDT` → REST 快照与 WS `snapshot_id` 一致
8. Order 市价买单无 `price` → 冻结成功

---

## 12. 完成检查清单（打印自用）

```
[ ] trade.events / match.events 已被 Market Data 稳定消费，offset 正常 commit
[ ] GET /v1/market/depth 与 GET /v1/market/ticker 符合 rest-api §4
[ ] WS 订阅 depth:{symbol}、ticker:{symbol} 可收 snapshot + 增量
[ ] WS 订阅 trade:{symbol} 可见公开市场成交（若本步实现）
[ ] GET /v1/klines + WS kline:{symbol}:1m 正常
[ ] GET /v1/index-price + WS 指数推送正常
[ ] GET /v1/market/ticker/all + WS ticker@all:USDT，snapshot_id 一致
[ ] 做市商 token 可订 ticker@all；普通 token 被拒绝
[ ] 市价买单（无 price）通过 GetReferencePrice 冻结（方案 C）
[ ] go test ./... 通过；关键路径有集成测
[ ] 更新 rest-api / order-api / development-roadmap 验收项
```

---

## 13. 文档与修订

| 文档 | 变更时机 |
|------|----------|
| [rest-api.md](./rest-api.md) | 实现与 WS 帧字段不一致时 |
| [order-api.md](./order-api.md) | 市价买 price 可选 |
| [design/market-buy-freeze.md](./design/market-buy-freeze.md) | 方案 C 完成后勾选 §8 |
| [development-roadmap.md](./development-roadmap.md) | 子任务完成打勾 |
| [architecture-spec.md](./architecture-spec.md) | 仅当架构性决策变更（如 Push 合并进 Gateway） |

---

## 14. 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-05-26 | 初稿：第 6 步 6.1～6.5 开发计划、契约、验收与周计划 |
