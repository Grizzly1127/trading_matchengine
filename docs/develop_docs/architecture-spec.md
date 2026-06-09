# 虚拟货币交易所撮合引擎 — 成长型生产架构设计

**版本**: 1.2  
**日期**: 2026-05-26  
**状态**: 草稿  
**目标规模**: 成长型生产 — 多交易对分片、水平扩展、高可用、可审计；不追求机构级极低延迟

---

## 1. 推荐总体架构

### 1.1 架构风格

**模块化单集群微服务**（Modular Microservices on Single Cluster）

- 各核心服务独立部署、独立扩缩容
- 通过 Kafka 事件总线解耦
- 撮合引擎按**撮合分片组**部署：热门交易对可独占分片，冷门交易对可合并到共享分片；每个交易对内部仍保持串行有序撮合
- 不引入跨数据中心分布式事务，依靠事件溯源保证最终一致

### 1.2 总体架构图（文字版）

```
┌─────────────────────────────────────────────────────────────────┐
│                          客户端层                                 │
│          REST / WebSocket (行情推送、订单管理)                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                      API Gateway (网关服务)                        │
│  认证/鉴权 · 限流 · 路由 · WebSocket连接管理 · 请求日志              │
└──────┬──────────────────────────────────┬────────────────────────┘
       │ REST/gRPC                         │ WebSocket 推送回调
┌──────▼────────────────────────┐  ┌──────▼─────────────────┐
│       Order Service           │  │  Push Service           │
│  订单创建/撤单/查询/历史        │  │  行情/深度/成交订阅推送   │
│  余额预锁 · 幂等检查           │  │  Redis Pub/Sub → WS     │
└──────┬────────────────────────┘  └──────▲─────────────────┘
       │ Kafka: order.commands             │ Redis channel
┌──────▼────────────────────────────────────────────────────┐
│                   Kafka 事件总线（核心骨干）                  │
│  Topics: order.commands · match.events · trade.events     │
│          kline.raw · index.price · system.audit           │
└──────┬──────────┬───────────────┬──────────────┬──────────┘
       │          │               │              │
┌──────▼──────┐ ┌─▼────────────┐ │         ┌────▼──────────┐
│  Matching   │ │ Market Data  │ │         │  Kline        │
│  Engine     │ │ Service      │ │         │  Service      │
│  (sharded)  │ │ 深度·ticker  │ │         │  K线聚合生成   │
│  WAL+快照   │ │ 聚合推送      │ │         │  1m/5m/1h/1d  │
└──────┬──────┘ └─▬────────────┘ │         └────▬──────────┘
       │ trade.events             │ index.price
       │                    ┌─────▼──────────┐
       │                    │  Index Price   │
       │                    │  Service       │
       │                    │  多源价格聚合   │
       │                    └────────────────┘
       │
┌──────▼──────────────────────────────────────────────────────┐
│                    持久化层                                    │
│  PostgreSQL (订单/成交/账户审计)  ·  Redis (缓存/Pub/Sub)     │
│  BadgerDB/本地 (撮合引擎 orderbook 快照)                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 核心服务划分和职责

### 2.1 API Gateway（网关服务）


| 属性        | 说明                                              |
| --------- | ----------------------------------------------- |
| **职责**    | 协议适配：REST/WS → 内网 gRPC；统一错误体与请求追踪；**不**承载充值/风控等业务编排 |
| **拥有的实体** | 无业务状态（无状态）；限流/鉴权配置随部署形态而定              |
| **对外接口**  | REST / WS 详见 [rest-api.md](./rest-api.md)       |
| **上游依赖**  | Order Service (gRPC)、Market Data Service (gRPC) |
| **技术**    | Go，无状态，水平扩展                               |

#### 2.1.1 部署形态：公网 Gateway vs 内网 Gateway + Web/BFF

成长型生产推荐 **两层 HTTP**，避免把调账、登录等全部堆在 Gateway：

```text
┌─────────────────────────────────────────────────────────────┐
│  终端用户（浏览器 / App / 开放 API 客户）                        │
└────────────────────────────┬────────────────────────────────┘
                             │ HTTPS（公网）
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  Web / BFF（对用户的产品 API）                                 │
│  · 登录 / Session / JWT 签发                                  │
│  · 页面与业务聚合、支付/链上充值流程、风控、审计                  │
│  · 从登录态解析 user_id，再调内网 Gateway                       │
└────────────────────────────┬────────────────────────────────┘
                             │ HTTP（VPC 内网 / mTLS，不对公网）
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  API Gateway（内网集成层，本文 rest-api 描述的对象）             │
│  · REST → Order / Market Data … gRPC                         │
│  · 服务间鉴权（非终端用户 Bearer）                              │
│  · 可选：POST /v1/balances（UpdateBalance）仅 Web/清算可调用   │
└────────────────────────────┬────────────────────────────────┘
                             │ gRPC
                             ▼
                    Order / Matching / …
```

| 能力 | Web / BFF（公网） | API Gateway（内网） | Order gRPC |
|------|------------------|---------------------|------------|
| 用户登录、注册 | ✅ | ❌ | ❌ |
| 下单 / 撤单 / 查单 | 转发或聚合 | ✅ REST → gRPC | ✅ |
| 查余额 | 对用户暴露 `GET` | ✅ `GET /v1/balances` | ✅ |
| 充值到账加余额 | 支付回调后触发 | 可选 `POST /v1/balances` | ✅ `UpdateBalance` |
| 浏览器直连调账 | ❌ **禁止** | — | — |

**原则**

1. **公网边界在 Web**，不在 Gateway；Gateway 监听地址不对 Internet 开放（安全组 / Ingress 内网）。
2. **终端用户永不直连 Gateway**；Gateway 的 `Authorization` 面向 **Web 服务账号 / 内网 mTLS**，与用户 Session 分离。
3. **`UpdateBalance` 是账务写操作**：生产由 Web（或清算服务）在充值/调账流程完成后调用；Phase 1 可在内网 Gateway 暴露以便联调，见 [rest-api.md §3.6.3](./rest-api.md#363-调账--充值内网--联调)。
4. Gateway 保持 **薄**：不做支付验签、不做 KYC；这些在 Web 完成后再调内网。

**Phase 1（当前仓库）**：常把 Gateway 与 Web 同机联调（`localhost:8080` + 硬编码 token），易误解为「用户 API」；上线时应按上表拆分网络与鉴权。

### 2.2 Order Service（订单服务）


| 属性        | 说明                                                                            |
| --------- | ----------------------------------------------------------------------------- |
| **职责**    | 订单全生命周期管理：创建/撤单/查询/历史；余额预锁定；幂等校验；Transactional Outbox 投递；超时补偿                 |
| **拥有的实体** | Order、OrderHistory、AccountBalance（冻结部分）、OrderOutbox                           |
| **对外接口**  | gRPC：`PlaceOrder`、`CancelOrder`、`GetOrder`、`ListOrders`                       |
| **发布事件**  | `order.commands` topic（经 Outbox Relay）：`NewOrderCommand`、`CancelOrderCommand` |
| **消费事件**  | `match.events` topic：更新订单状态（部成/全成/已撤）                                         |
| **存储**    | PostgreSQL（orders、order_outbox、account_balances 表）；一致性模型见 §4                  |


### 2.2.1 订单标识（order_id / client_order_id）


| 字段                | 类型         | 生成方           | 用途                          |
| ----------------- | ---------- | ------------- | --------------------------- |
| `client_order_id` | **string** | 客户端           | 幂等键；用户维度唯一；最长 64 字符         |
| `order_id`        | **uint64** | Order Service | 系统订单主键；DB/Kafka/撮合/WAL 统一使用 |


`**order_id` 发号（Phase 1 推荐）**

- PostgreSQL：`orders.id BIGINT` 使用 `GENERATED BY DEFAULT AS IDENTITY` 或应用层 **Snowflake**（64 位，单调大致有序）。
- 禁止把 `client_order_id` 直接当作 `order_id`。
- 多实例 Order Service 使用 Snowflake 时，以 `instance_id` 区分 worker，避免撞号。

**Protobuf**（`common/types.proto`）：`order_id` → `uint64`；`client_order_id` → `string`。

**REST / JSON**（见 [rest-api.md](./rest-api.md)）：`order_id` 语义为 uint64，**序列化为十进制字符串**（如 `"1000000001"`），避免 JavaScript `Number` 超过 2^53 丢精度；路径参数同为数字字符串 `/v1/orders/1000000001`。

**撮合引擎**：`order_map` 键为 `uint64`；`NewOrderCommand` / `CancelOrderCommand` 携带 `uint64 order_id`。

### 2.3 Matching Engine（撮合引擎）


| 属性         | 说明                                                                                                  |
| ---------- | --------------------------------------------------------------------------------------------------- |
| **职责**     | 在撮合分片内维护多个交易对的价格-时间优先撮合；产出成交事件；保证可恢复重启                                                              |
| **分片策略**   | 以 `matching shard` 为部署单元；热门交易对可独占 shard，冷门交易对合并到共享 shard；每个 symbol 在 shard 内由独立 `SymbolEngine` 串行处理 |
| **拥有的实体**  | Shard 消费位点、SymbolEngine、Orderbook（内存）、WAL事件日志（本地）、Snapshot（定期）                                      |
| **消费**     | `order.commands` Kafka topic（按 `symbol -> shard -> partition` 路由）                                   |
| **发布事件**   | `match.events`（订单状态变更）、`trade.events`（成交记录）                                                         |
| **核心数据结构** | 买卖各一棵价格树（`map[price]*PriceLevel`），PriceLevel 含 FIFO 队列                                              |
| **恢复机制**   | 见第 5 节                                                                                              |


### 2.3.1 撮合分片策略

交易对存在明显冷热差异时，不应默认采用“1 交易对 = 1 进程”。推荐使用**混合分片模型**：


| 类型     | 部署方式                                            | 适用场景                              |
| ------ | ----------------------------------------------- | --------------------------------- |
| 热门交易对  | 1 个 symbol 独占 1 个 shard / Kafka partition / Pod | BTC-USDT、ETH-USDT 等高 TPS、低延迟要求交易对 |
| 次热门交易对 | 少量 symbol 共享 1 个 shard                          | 有稳定流量，但单独部署资源利用率不高                |
| 冷门交易对  | 多个 symbol 合并到共享 shard                           | 低频交易对，重点提升资源利用率                   |


设计约束：

- 每个 `SymbolEngine` 只负责一个交易对，内部单线程处理命令，保证价格时间优先和确定性。
- 一个 `Matching Engine` 进程可以承载多个 `SymbolEngine`，进程内通过 `symbol` 路由到对应引擎。
- `Shard Manager` 维护 `symbol -> shard_id -> kafka_partition -> node` 映射，Order Service 发布命令前必须读取该映射。
- 热门交易对可以从共享 shard 迁移到独占 shard；迁移必须在停牌/只撤单/短暂停写窗口内完成，避免同一 symbol 同时被两个 shard 处理。
- shard 分配需要基于 `TPS`、活跃挂单数、撮合延迟、Kafka lag、CPU/内存等指标动态评估，但第一版可采用静态配置。

### 2.4 Market Data Service（行情服务）


| 属性        | 说明                                                                  |
| --------- | ------------------------------------------------------------------- |
| **职责**    | 消费 trade/match 事件，聚合实时 Orderbook 深度、Ticker（24h量价）；向 Push Service 推送 |
| **拥有的实体** | 深度快照、Ticker 聚合状态（内存+Redis）                                          |
| **消费**    | `match.events`、`trade.events`                                       |
| **发布**    | Redis Pub/Sub channel：`depth:{symbol}`、`ticker:{symbol}`            |
| **接口**    | gRPC `GetOrderBook`、`GetTicker`（供 Gateway REST 查询）                  |


### 2.5 Kline Service（K线服务）


| 属性        | 说明                                                          |
| --------- | ----------------------------------------------------------- |
| **职责**    | 消费 `trade.events`，按时间窗口聚合 OHLCV，生成 1m/5m/15m/1h/4h/1d K线    |
| **拥有的实体** | Kline 记录（PostgreSQL）、当前未闭合 Bar（Redis）                       |
| **消费**    | `trade.events`                                              |
| **发布**    | Redis Pub/Sub `kline:{symbol}:{interval}`、Kafka `kline.raw` |
| **接口**    | gRPC `GetKlines(symbol, interval, start, end, limit)`       |


### 2.6 Index Price Service（指数价格服务）


| 属性        | 说明                                                   |
| --------- | ---------------------------------------------------- |
| **职责**    | 定期拉取多个外部交易所价格（HTTP/WS），加权聚合为指数价格；防操纵过滤               |
| **拥有的实体** | 外部价格源配置、指数价格历史（PostgreSQL）                           |
| **数据源**   | Binance、OKX、Bybit 等公开行情 API                          |
| **发布**    | Kafka `index.price` topic、Redis Key `index:{symbol}` |
| **接口**    | gRPC `GetIndexPrice(symbol)`                         |


### 2.7 Push Service（推送服务）


| 属性      | 说明                                                    |
| ------- | ----------------------------------------------------- |
| **职责**  | 管理客户端 WebSocket 长连接；订阅 Redis Pub/Sub；向对应客户端推送行情/订单/成交 |
| **无状态** | 连接状态存 Redis，支持水平扩展                                    |

> 实现说明：WebSocket 由独立 `cmd/push` 进程暴露 `/v1/ws`（默认 `:8081`）；Gateway（`:8080`）仅提供 REST。`internal/push/*` 为 Push 服务实现模块。


---

## 3. 关键数据流

### 3.1 下单流程

```
Client
  │ POST /v1/orders
  ▼
API Gateway (JWT验证, 限流)
  │ gRPC PlaceOrder
  ▼
Order Service
  ├─ 幂等检查 (client_order_id → Redis SET NX，最终以 DB 唯一索引为准)
  ├─ 单事务 (PostgreSQL):
  │    ├─ 余额预锁定 (account_balances.frozen += X)
  │    ├─ INSERT orders (status=PENDING)
  │    └─ INSERT order_outbox (NewOrderCommand payload)
  └─ Outbox Relay (异步) → Kafka: order.commands[symbol]
         │
         ▼
Matching Engine (消费 order.commands, 按 symbol partition)
  ├─ 追加 WAL: AppendLog(NewOrderCommand)
  ├─ 执行撮合: TryMatch()
  │    ├─ 无成交 → 挂单到 Orderbook
  │    └─ 有成交 → 产出 TradeEvent[]
  ├─ 写入本地 Event Outbox（match/trade 事件 payload）并 fsync
  ├─ Commit order.commands Kafka offset（仅 outbox durable 之后）
  └─ Event Relay（异步）→ Kafka: match.events / trade.events
         （设计详见 matching-event-outbox-design.md；实现前可仍为同步 Publish）
         │
         ▼
Order Service (消费 match.events)
  └─ 更新 orders 表状态, 释放/扣减冻结余额
```

### 3.2 撤单流程

```
Client → API Gateway → Order Service
  ├─ 检查订单存在且可撤 (status IN PENDING/ACCEPTED/PARTIAL)
  ├─ 单事务: UPDATE orders status=CANCELING + INSERT order_outbox (CancelOrderCommand)
  └─ Outbox Relay → Kafka: order.commands[symbol] → CancelOrderCommand{order_id}
         │
         ▼
Matching Engine
  ├─ 追加 WAL: AppendLog(CancelOrderCommand)
  ├─ 从 Orderbook 移除订单
  └─ 发布 match.events → OrderCanceled{order_id}
         │
         ▼
Order Service (消费 match.events)
  └─ 更新 status=CANCELED, 释放冻结余额
```

### 3.3 成交事件流

```
Matching Engine
  └─ 发布 trade.events → TradeEvent
         │
         ├──▶ Market Data Service → 更新 Ticker(last_price, 24h_vol) → Redis Pub/Sub depth/ticker
         ├──▶ Kline Service → 聚合 OHLCV → 更新未闭合 Bar → 写库/推送
         └──▶ Order Service → 确认成交记录 → 更新订单状态+余额
```

### 3.4 行情数据流

```
Matching Engine → match.events (Orderbook增量变更)
         │
         ▼
Market Data Service
  ├─ 维护内存 Orderbook 镜像 (按 symbol)
  ├─ 每 100ms 计算深度快照 (top 20 bid/ask)
  └─ 发布 Redis Pub/Sub: depth:{symbol} (增量diff + 快照)
         │
         ▼
Push Service (订阅 Redis Pub/Sub)
  └─ 按客户端订阅推送 WS frame
```

### 3.5 K线数据流

```
trade.events (Kafka)
         │
         ▼
Kline Service
  ├─ 按 symbol + interval 分组
  ├─ 当前未闭合 Bar 存 Redis Hash: kline:open:{symbol}:{interval}
  │    → open/high/low/close/vol 实时更新
  ├─ Bar 闭合时 → 写入 PostgreSQL klines 表
  └─ 发布 Redis Pub/Sub kline:{symbol}:{interval} → 推送最新 Bar
```

### 3.6 指数价格数据流

```
Index Price Service
  ├─ 定时 (每秒) 拉取多个外部交易所 REST/WS 价格
  ├─ 过滤异常值 (偏差超 3% 剔除)
  ├─ 加权中位数聚合
  ├─ 写入 Redis SET index:{symbol} = {price, ts, sources[]}
  ├─ 写入 PostgreSQL index_prices 表 (审计)
  └─ 发布 Kafka index.price → 供下游（合约标记价等）消费
```

---

## 4. 一致性模型与补偿

> **设计立场**：Order Service、Kafka、Matching Engine 之间**不**使用跨服务分布式事务（2PC/XA），依靠**局部事务 + Transactional Outbox + 幂等消费 + 超时补偿 + 定期对账** 保证最终一致。这与 §1.1「成长型生产、不追求机构级极低延迟」的定位一致。

### 4.1 一致性分层


| 层级  | 范围                              | 保证类型          | 实现手段                                 |
| --- | ------------------------------- | ------------- | ------------------------------------ |
| L1  | Order Service 单库内               | **强一致（ACID）** | PostgreSQL 单事务：冻结余额 + 写订单 + 写 Outbox |
| L2  | Order Service → Kafka           | **至少一次投递**    | Transactional Outbox + 后台 Relay      |
| L3  | Kafka → Matching Engine         | **有序 + 至少一次** | 按 symbol 固定 partition；消费幂等           |
| L4  | Matching Engine 进程内             | **命令级原子**     | WAL fsync 后再改内存；单 SymbolEngine 串行    |
| L4b | Matching 进程内事件出站              | **至少落盘一次**    | 本地 Event Outbox fsync；详见 [matching-event-outbox-design.md](./matching-event-outbox-design.md) |
| L4c | `order.commands` offset commit  | **不超过已落盘事件**  | Event Outbox durable 后才 commit；禁止先 commit 后写 outbox |
| L5  | Matching Engine → Order Service | **最终一致**      | Event Outbox Relay + `match.events` / `trade.events` 幂等写库 |
| L6  | 全链路对用户语义                        | **最终一致**      | 订单状态机 + 中间态超时 + 对账（§5.6）             |


**不保证**：API 返回成功 ⟺ 订单已进 Orderbook 且余额已最终扣减（同步原子）。客户端应依据 `status` 字段判断生命周期。

### 4.2 订单状态机

```
                    PlaceOrder (DB+Outbox commit)
                              │
                              ▼
                         ┌─────────┐
              ┌─────────│ PENDING │─────────┐
              │         └────┬────┘         │
              │ 超时/拒单     │ OrderAccepted│
              │              ▼              │
              │         ┌──────────┐        │
              │         │ ACCEPTED │        │
              │         └────┬─────┘        │
              │              │ 部分成交     │
              │              ▼              │
              │         ┌──────────┐   CancelOrder
              │         │ PARTIAL  │◄──────────────┐
              │         └────┬─────┘               │
              │    全成/撤单完成│                  │
              │              ▼                    │
              │    ┌─────────────────────┐        │
              └───►│ FILLED / CANCELED   │◄───────┘
                   │ REJECTED (风控/余额) │
                   └─────────────────────┘

撤单中间态：PENDING/PARTIAL/ACCEPTED → CANCELING → CANCELED
```


| 状态          | 含义                   | 余额            |
| ----------- | -------------------- | ------------- |
| `PENDING`   | 已落库、Outbox 待投递或撮合未确认 | 已冻结           |
| `ACCEPTED`  | 撮合已接单（可能在 Orderbook） | 已冻结           |
| `PARTIAL`   | 部分成交                 | 按成交比例扣减/仍冻结剩余 |
| `CANCELING` | 撤单命令已发出，等待撮合确认       | 仍冻结剩余部分       |
| `FILLED`    | 全部成交                 | 冻结转扣减         |
| `CANCELED`  | 已撤销                  | 释放剩余冻结        |
| `REJECTED`  | 风控/余额不足/超时未进撮合       | 释放全部冻结        |


**并发规则**：状态迁移必须使用 `UPDATE ... WHERE id=? AND status=? AND version=?`（乐观锁）；`CANCELING` 期间拒绝重复撤单；`FILLED`/`CANCELED`/`REJECTED` 为终态。

### 4.3 Transactional Outbox（DB → Kafka 可靠投递）

避免「DB 已提交但 Kafka 未发出」导致撮合永远收不到命令。

**表结构**（Order Service 库内）：

```sql
CREATE TABLE order_outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  BIGINT NOT NULL,         -- order_id (uint64)
    event_type    VARCHAR(32) NOT NULL,   -- NewOrderCommand | CancelOrderCommand
    payload       BYTEA NOT NULL,         -- protobuf 序列化
    topic         VARCHAR(64) NOT NULL,   -- order.commands
    partition_key VARCHAR(32) NOT NULL,   -- symbol（路由到 shard partition）
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,            -- NULL = 待投递
    retry_count   INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_outbox_unpublished ON order_outbox (created_at)
    WHERE published_at IS NULL;
```

**下单/撤单写路径**（同一 PostgreSQL 事务内）：

```
BEGIN;
  1. 幂等：INSERT client_order_idempotency ... ON CONFLICT DO NOTHING（或先查）
  2. 冻结余额：UPDATE account_balances ...
  3. INSERT orders (status=PENDING 或 CANCELING)
  4. INSERT order_outbox (published_at=NULL)
COMMIT;
-- 事务外：Outbox Relay 异步投递，不在请求线程阻塞等待 Kafka
```

**Outbox Relay**（独立 goroutine / 可选独立 worker）：

- 轮询 `published_at IS NULL`，按 `created_at` 升序批量读取
- 投递 Kafka `order.commands`，`acks=all` 成功后将该行 `published_at=now()`
- 失败指数退避重试；超过 `max_retry` 告警并进入死信人工处理
- 投递前检查 `orders.status` 仍为可发送态（`PENDING`/`CANCELING`），终态订单跳过并标记 Outbox 已废弃

**幂等**：Kafka 消息携带 `uint64 order_id` + `uint64 command_id`（可用 `order_outbox.id`）；Matching Engine 按 `order_id` 去重。

### 4.4 Order Service 库内事务边界


| 操作                 | 同一事务内包含                                                  | 说明                                          |
| ------------------ | -------------------------------------------------------- | ------------------------------------------- |
| `PlaceOrder`       | 幂等记录 + 冻结 + `orders` INSERT + `order_outbox` INSERT      | 任一步失败整体 ROLLBACK                            |
| `CancelOrder`      | 状态 CAS 更新为 `CANCELING` + `order_outbox` INSERT           | 仅当 `status IN (PENDING, ACCEPTED, PARTIAL)` |
| 消费 `match.events`  | `orders` 更新 + `trades` INSERT + `account_balances` 扣减/解冻 | `trade_id` 唯一约束幂等                           |
| 消费 `OrderRejected` | `orders` → `REJECTED` + 释放冻结                             | 与撮合拒单事件对齐                                   |


Redis `SET NX` 仅作**快速幂等拦截**；最终以 DB `client_order_id` 唯一索引为准，防止 Redis 过期后重复下单。

### 4.5 中间态超时与补偿


| 场景           | 触发条件                                                                | 补偿动作                                                                  |
| ------------ | ------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Outbox 长期未发出 | `PENDING` 且 Outbox `published_at IS NULL` 超过 **30s**                | Relay 加急重试；超 **5min** 告警                                              |
| 撮合未确认接单      | `PENDING` 且 Outbox 已发出，超过 **60s** 无 `OrderAccepted`/`OrderRejected` | 定时任务查询 Kafka offset / 向 Matching Engine 对账；仍无响应则 `REJECTED` + 解冻 + 告警 |
| 撤单悬挂         | `CANCELING` 超过 **30s** 无 `OrderCanceled`                            | 重发 `CancelOrderCommand`（幂等）；超 **5min** 对账 + 人工介入                      |
| 成交回写滞后       | 撮合已发 `trade.events`，Order Service lag 高                             | 扩消费者；**不**在撮合侧重试成交                                                    |
| 重复 Kafka 投递  | At-Least-Once                                                       | Matching：`order_id` 去重；Order：`trade_id` 唯一索引                          |


**补偿任务**（Order Service 内置 scheduler，建议每分钟 + 关键指标触发）：

1. 扫描超时 `PENDING` / `CANCELING` 订单
2. 对比 `order_outbox` 与 Kafka（可选：记录已发 `command_id` 到 Redis Set 辅助）
3. 调用 §5.6 对账接口与 Matching Engine Orderbook diff（仅异常单）

### 4.6 用户可见语义（API 契约）


| API 行为                     | 保证                                        |
| -------------------------- | ----------------------------------------- |
| `PlaceOrder` 返回 `order_id` | 订单**已持久化**且 Outbox 已写入（同事务）；**不保证**已撮合    |
| 重复 `client_order_id`       | 返回同一 `order_id`（幂等）                       |
| `CancelOrder` 返回成功         | 已进入 `CANCELING` 且 Outbox 已写入；**不保证**盘口已移除 |
| 查询 `GetOrder`              | 以 DB `status` 为准；成交明细以 `trades` 表为准       |


WebSocket 订单推送应在 Order Service 消费 `match.events` **之后**发出，避免客户端先于 DB 看到终态。

### 4.7 故障矩阵（快速查阅）


| 故障             | 库表状态                | 撮合状态  | 恢复           |
| -------------- | ------------------- | ----- | ------------ |
| 事务提交前崩溃        | 无单                  | 无     | 客户端重试（幂等）    |
| 事务成功，Relay 未发  | PENDING + Outbox 待发 | 无     | Relay 重试     |
| Kafka 已发，撮合未消费 | PENDING             | 无     | 等待消费；超时拒单    |
| 撮合已成交，事件未回写    | PENDING/PARTIAL（滞后） | 已成交   | 消费 lag 追上；对账 |
| 重复消费命令         | 正确（幂等）              | 无重复撮合 | —            |


---

## 5. 撮合引擎恢复重启机制

> 核心原则：**每个 symbol 在任意时刻只能由一个 SymbolEngine 处理**。Matching Engine 进程可以承载多个 symbol，但必须保证重启后每个 Orderbook 完全还原、不重复撮合、不丢失命令。

### 5.1 事件日志（WAL - Write-Ahead Log）

- **位置**: 撮合引擎本地磁盘，文件路径 `data/wal/{shard_id}/`；可额外维护按 symbol 的索引，便于排查和局部恢复
- **格式**: 顺序追加二进制文件（protobuf 序列化），每条记录：
  ```
  [4 bytes len][seq_id uint64][timestamp int64][event_type byte][payload bytes][crc32 uint32]
  ```
- **写入时机**: 执行任何状态变更**之前**先写 WAL（写前日志），**durable `fsync` 后**再修改内存状态
- **组提交（可选）**: `sync_every_records` / `sync_interval_ms` 控制多条记录共享一次 `fdatasync`；`CommitBatch` 在 `Sync()` 成功后按序 apply。未 `Sync()` 前禁止改 orderbook、禁止 commit Kafka offset；崩溃后靠 WAL 已 durable 前缀 + Kafka 重投幂等恢复（见 `configs/matching.json` → `wal_group_commit`）
- **WAL 滚动**: 每 100MB 或每 10 分钟滚动新文件；已确认快照覆盖的旧 WAL 可安全删除

### 5.2 序列号（Global Sequence ID）

- 每个 shard 维护**单调递增 shard_seq_id**，直接使用对应 Kafka partition offset
- 每个 symbol 维护自己的 `last_applied_offset` 和业务序列号，用于对账、快照和排查；同一 symbol 在共享 partition 中的 offset 可以不连续
- 撮合引擎在 WAL 中记录每条命令的 `shard_id`、`symbol`、`kafka_partition`、`kafka_offset`，重启后以 shard 维度 seek 到 `recovered_offset + 1`

### 5.3 快照（Snapshot）

- **触发条件**: 每处理 10,000 条命令 **或** 每 5 分钟，取较早者
- **快照内容**:
  ```
  Snapshot {
    shard_id       string
    symbol         string
    seq_id         uint64       // 该 symbol 已应用到的 Kafka offset
    timestamp      int64
    bids           []PriceLevel // 价格+挂单队列
    asks           []PriceLevel
    order_map      map[uint64]Order  // 活跃订单索引，键为 order_id
    checksum       uint64       // FNV-64 of bids+asks
  }
  ```
- **存储位置**: `data/snapshots/{shard_id}/{symbol}/snapshot_{seq_id}.pb`，保留最近 3 份
- **Shard Manifest**: 每次生成快照后写入 `manifest.pb`，记录 shard 内各 symbol 的快照位点以及 shard 可恢复的统一 `recovered_offset`
- **写入方式**: 先写 `.tmp` 文件，fsync，再原子重命名

### 5.4 幂等性保证

- Matching Engine **只消费 Kafka**，Kafka 消费组确保 At-Least-Once
- 每条 `NewOrderCommand` 含 `uint64 order_id`，撮合前检查是否已在 active/closed 集合
- `CancelOrderCommand` 对已不存在的订单直接忽略（幂等）
- 成交产出的 `TradeEvent` 含 `trade_id = sha256(str(order_id_maker) + str(order_id_taker) + seq_id)`（或 uint64 派生），下游消费幂等写库

### 5.5 回放（Replay）

重启恢复流程（顺序执行）：

```
Step 1: 加载 shard manifest 和各 symbol 最新快照
  └─ 读取 data/snapshots/{shard_id}/{symbol}/snapshot_*.pb
  └─ 恢复 shard 内每个 SymbolEngine 的 Orderbook + order_map
  └─ 记录 shard_recovered_offset = manifest.recovered_offset

Step 2: 回放 shard WAL（manifest 后的增量）
  └─ 读取 WAL 中 kafka_offset > shard_recovered_offset 的所有条目
  └─ 按 symbol 分发给对应 SymbolEngine 重新执行命令
  └─ **不**从命令回放推导并发布 Kafka 事件，仅恢复内存状态
  └─ WAL 回放完成后记录 wal_recovered_offset

Step 2b: Event Outbox Relay 追平（启用异步发布时）
  └─ 从 `last_published_outbox_seq` 续投未发布记录至 match.events / trade.events
  └─ 与 Step 2 命令回放分工：命令恢复 orderbook，Outbox 恢复事件投递

Step 3: 从 Kafka 续消费
  └─ seek Kafka consumer offset 到 wal_recovered_offset + 1
  └─ 恢复正常撮合循环
```

### 5.6 恢复校验（Recovery Verification）

- **快照 Checksum 校验**: 加载快照后重算 `checksum`，与文件中记录比对
- **WAL CRC 校验**: 回放每条 WAL 记录时验证 `crc32`，检测磁盘损坏
- **Orderbook 合法性断言**:
  ```go
  // 校验最优买价 < 最优卖价（无价格倒挂）
  assert(bestBid < bestAsk || orderbook.isEmpty())
  // 校验活跃订单数量与价格树节点总和一致
  assert(len(orderMap) == totalOrdersInPriceLevels)
  ```
- **对账校验（启动后 30 秒内）**: 按 shard 内 symbol 逐个向 Order Service 查询 PENDING/PARTIAL 订单，与内存 Orderbook 中的 active orders 做 diff；若任一 symbol 有差异，则该 symbol 进入只读/拒单状态并报警，避免影响同 shard 的其他正常交易对

---

## 6. 存储/消息队列/缓存建议

### 6.1 PostgreSQL（主数据库）


| 表                          | 所属服务                | 说明                                          |
| -------------------------- | ------------------- | ------------------------------------------- |
| `orders`                   | Order Service       | 订单全生命周期；主键 `id BIGINT`（即 `order_id`，uint64） |
| `order_outbox`             | Order Service       | Transactional Outbox，保证命令可靠投递至 Kafka（§4.3）  |
| `client_order_idempotency` | Order Service       | `client_order_id` 唯一索引，下单幂等                 |
| `trades`                   | Order Service       | 成交记录（幂等写）                                   |
| `account_balances`         | Order Service       | 账户余额+冻结                                     |
| `klines`                   | Kline Service       | OHLCV，按(symbol, interval, open_time)联合主键    |
| `index_prices`             | Index Price Service | 审计用，保留 30 天                                 |


**分区策略**: `orders`、`trades` 按月 Range Partition；`klines` 按(symbol+interval)分区

**版本**: PostgreSQL 16，连接池用 `pgbouncer`（transaction 模式）

### 6.2 Kafka（事件总线）

详细 Topic 契约、消息体与消费者矩阵见 **[kafka-data.md](./kafka-data.md)**。

| Topic            | Partitions   | 保留  | 说明                                        |
| ---------------- | ------------ | --- | ----------------------------------------- |
| `order.commands` | N（按 shard 数） | 7天  | 命令流，按 `symbol -> shard` 映射路由到固定 partition |
| `match.events`   | N            | 7天  | 订单状态变更事件                                  |
| `trade.events`   | N            | 30天 | 成交事件，多下游消费                                |
| `kline.raw`      | 16           | 3天  | K线闭合通知                                    |
| `index.price`    | 8            | 1天  | 指数价格广播                                    |
| `system.audit`   | 4            | 90天 | 全量操作审计                                    |


**版本**: Kafka 3.7（KRaft 模式，无 ZooKeeper）

**关键配置**:

- `acks=all`，`min.insync.replicas=2`（生产端强持久化）
- 撮合引擎消费组：`enable.auto.commit=false`，手动提交 offset（shard WAL fsync 后才 commit）

### 6.3 Redis（缓存/实时状态）

Key / Pub/Sub 契约、JSON 载荷与读写方矩阵见 **[redis-data.md](./redis-data.md)**。

| Key 模式                               | 用途             | TTL         |
| ------------------------------------ | -------------- | ----------- |
| `idempotent:order:{client_order_id}` | 下单幂等锁          | 24h         |
| `index:{symbol}`                     | 最新指数价格         | 10s         |
| `kline:open:{symbol}:{interval}`     | 未闭合 Bar        | interval 时长 |
| `depth:{symbol}:snapshot`            | Orderbook 深度快照 | 5s          |
| Pub/Sub channel                      | 行情实时推送         | 无TTL        |


**版本**: Redis 7.2，集群模式（3主3从），开启 AOF+RDB 双持久化

### 6.4 本地存储（撮合引擎专属）

- **WAL 文件**: 本地 SSD，`data/wal/{shard_id}/`
- **Snapshot 文件**: 本地 SSD，`data/snapshots/{shard_id}/{symbol}/`
- **Shard Manifest**: 本地 SSD，`data/snapshots/{shard_id}/manifest.pb`
- **Kubernetes**: 使用 `StatefulSet` + `PersistentVolumeClaim`（local SSD StorageClass），1 Pod 对应 1 个或多个 shard；热门 symbol 可独占 Pod
- **备份**: 定期将 Snapshot 上传 S3/OSS，灾难恢复用

---

## 7. Go 项目模块结构

```
trading_matchengine/
├── cmd/
│   ├── gateway/          # API Gateway 启动入口
│   ├── order/            # Order Service 启动入口
│   ├── matching/         # Matching Engine 启动入口
│   ├── marketdata/       # Market Data Service 启动入口
│   ├── kline/            # Kline Service 启动入口
│   └── indexprice/       # Index Price Service 启动入口
│
├── internal/
│   ├── gateway/
│   │   ├── handler/      # HTTP/WS 路由处理器
│   │   ├── middleware/   # JWT、限流、日志中间件
│   │   └── client/       # 下游服务 gRPC client 封装
│   │
│   ├── order/
│   │   ├── handler/      # gRPC server 实现
│   │   ├── service/      # 业务逻辑（下单/撤单/查询）
│   │   ├── repository/   # PostgreSQL CRUD
│   │   ├── outbox/       # Outbox 写入 + Relay 投递
│   │   ├── reconciler/   # 超时扫描、补偿、对账触发
│   │   └── consumer/     # Kafka match.events 消费者
│   │
│   ├── matching/
│   │   ├── engine/       # 撮合核心：orderbook.go, matcher.go
│   │   ├── recovery/     # WAL 写入/读取, Snapshot 生成/加载, 恢复流程
│   │   ├── consumer/     # Kafka order.commands 消费者
│   │   └── publisher/    # Kafka match.events/trade.events 发布者
│   │
│   ├── marketdata/
│   │   ├── aggregator/   # Orderbook 镜像维护, Ticker 聚合
│   │   ├── handler/      # gRPC server
│   │   └── publisher/    # Redis Pub/Sub 发布
│   │
│   ├── kline/
│   │   ├── aggregator/   # 时间窗口聚合逻辑
│   │   ├── repository/   # klines 表读写
│   │   └── publisher/    # Redis Pub/Sub 发布
│   │
│   ├── indexprice/
│   │   ├── collector/    # 多交易所价格拉取（HTTP/WS）
│   │   ├── aggregator/   # 加权中位数, 异常值过滤
│   │   └── publisher/    # Redis + Kafka 发布
│   │
│   └── push/
│       ├── hub/          # WebSocket 连接管理, 订阅路由
│       └── subscriber/   # Redis Pub/Sub 订阅转发
│
├── pkg/
│   ├── kafka/            # Kafka producer/consumer 封装（sarama 或 kafka-go）
│   ├── redis/            # Redis client 封装
│   ├── postgres/         # DB 连接池, 事务封装
│   ├── wal/              # WAL 读写、CRC 校验（撮合引擎专用）
│   ├── snapshot/         # Snapshot 序列化/反序列化
│   ├── grpc/             # gRPC server/client 通用封装（拦截器、TLS）
│   ├── auth/             # JWT 解析/验证
│   ├── metrics/          # Prometheus metrics 注册
│   └── logger/           # 结构化日志 (zerolog/zap)
│
├── proto/                # Protobuf 定义
│   ├── order/
│   │   └── order.proto
│   ├── matching/
│   │   └── matching.proto
│   ├── marketdata/
│   │   └── marketdata.proto
│   └── common/
│       └── types.proto   # Price, Quantity, Symbol 等共享类型
│
├── migrations/           # SQL 迁移文件 (golang-migrate)
│   ├── 001_create_orders.sql
│   ├── 002_create_order_outbox.sql
│   ├── 003_create_trades.sql
│   └── ...
│
├── deploy/
│   ├── docker/
│   │   └── Dockerfile.*  # 各服务 Dockerfile
│   └── k8s/
│       ├── matching/     # StatefulSet + PVC
│       ├── order/        # Deployment
│       ├── gateway/      # Deployment + HPA
│       └── infra/        # Kafka, Redis, PostgreSQL Helm values
│
├── scripts/
│   ├── gen-proto.sh      # Protobuf 代码生成
│   └── migrate.sh        # 数据库迁移
│
├── go.mod
├── go.sum
├── Makefile
└── docs/
    └── architecture-spec.md   # 本文档
```

### 7.1 模块间依赖原则

```
gateway  →  order(gRPC), marketdata(gRPC)
order    →  kafka(pub), postgres
matching →  kafka(sub/pub), wal, snapshot
marketdata → kafka(sub), redis(pub), grpc(server)
kline    →  kafka(sub), postgres, redis(pub)
indexprice → external HTTP/WS, redis, kafka(pub), postgres
push     →  redis(sub), websocket
```

**禁止**: 任何服务直接访问**另一个服务**的数据库；所有跨服务数据交换通过 Kafka 或 gRPC。

---

## 8. 分阶段落地路线

> **Go 自学实现顺序**（先 matcher 后 WAL、分步验收）见 [development-roadmap.md](./development-roadmap.md)。

### Phase 1：单交易对 MVP（第 1-4 周）

**目标**: 核心撮合链路跑通，可人工测试

- 搭建基础设施：Kafka（单节点开发版）、PostgreSQL、Redis
- 实现 `pkg/wal` 和 `pkg/snapshot`（先实现，撮合引擎依赖它）
- 实现 Matching Engine 核心：orderbook + matcher + WAL写入 + Snapshot
- 实现 Matching Engine 恢复流程（重启后自动恢复并校验）
- 实现 Order Service：下单/撤单 gRPC + Transactional Outbox + Relay + match.events 消费
- 实现订单状态机（PENDING/ACCEPTED/PARTIAL/CANCELING 等）与超时补偿 scheduler
- 实现最简 API Gateway：REST 下单/撤单/查询（暂不做 WebSocket）
- 联调：下单 → 撮合 → 成交回写 全链路

**验收标准**: 重启撮合引擎后，挂单不丢失，已成交订单不重复撮合

---

### Phase 2：行情与K线（第 5-7 周）

**目标**: 完整行情推送能力，支持前端接入

- 实现 Market Data Service（深度聚合 + gRPC接口）
- 实现 Kline Service（1m/1h/1d 聚合 + PostgreSQL存储）
- 实现 Push Service（WebSocket + Redis Pub/Sub）
- API Gateway 增加 WebSocket 推送接口
- 实现 Index Price Service（接入 2-3 个外部交易所）

**验收标准**: 客户端订阅 WS 后，实时收到深度/成交/K线/指数价格

---

### Phase 3：多交易对 + 高可用（第 8-12 周）

**目标**: 生产就绪，支持多交易对，可 K8s 部署

- 引入 `Shard Manager`，维护 `symbol -> shard_id -> kafka_partition -> node` 映射
- 撮合引擎按 shard 分片，支持一个进程承载多个 `SymbolEngine`
- Kafka 按 shard 分配 partition；热门 symbol 可独占 partition，冷门 symbol 共享 partition
- Matching Engine 改为 K8s StatefulSet（1 Pod = 1 个或多个 shard，热门交易对可独占 Pod）
- 所有服务接入 Prometheus + Grafana（关键指标：撮合延迟、WAL写入延迟、Kafka消费lag）
- 添加恢复对账告警：Orderbook 与数据库不一致时触发 PagerDuty（§5.6、§4.5）
- PostgreSQL 主从 + pgbouncer；Redis Cluster；Kafka 3副本

**验收标准**: 模拟 Pod 重启，撮合引擎 < 30 秒自动恢复，无订单丢失

---

### Phase 4：安全、审计与运维（第 13-16 周）

**目标**: 满足合规与运维要求

- API Key 管理（HMAC-SHA256 签名验证）
- 全量操作写入 `system.audit` Kafka topic + 归档存储
- 限流熔断（令牌桶，按用户/IP 两级）
- 撮合引擎 Snapshot 定期备份至 S3/OSS
- 压测：单交易对 TPS ≥ 5,000，撮合延迟 P99 ≤ 10ms（方案见 [docs/benchmark.md](../benchmark.md)；未达标优化见 [l2-optimization-roadmap.md](./l2-optimization-roadmap.md)）
- 灾难恢复演练：从 S3 快照冷启动恢复

---

## 9. 架构约束（所有服务必须遵守）

1. **服务间通信**: 同步调用用 gRPC + protobuf；异步解耦用 Kafka；禁止 REST 服务间调用
2. **数据库隔离**: 每个服务只访问自己的数据库表，禁止跨服务 JOIN
3. **命令可靠投递**: Order Service 向 `order.commands` 的发送**必须**经 Transactional Outbox（§4.3）；禁止在 DB 事务提交前直接 `publish` Kafka，禁止仅靠进程内内存队列替代 Outbox
4. **幂等写入**: 所有 Kafka 消费者处理逻辑必须幂等（使用 `ON CONFLICT DO NOTHING` 或唯一索引）
5. **错误处理**: panic 只用于不可恢复的程序错误；业务错误通过 `error` 返回并结构化日志记录
6. **日志规范**: 每条日志必须含 `service`、`symbol`、`request_id`、`seq_id` 字段（zerolog）
7. **指标规范**: 每个 Kafka 消费者暴露 `processing_latency_ms` 和 `lag` 两个 Prometheus 指标；Order Service 额外暴露 `outbox_pending_count`、`order_stuck_pending_seconds`
8. **配置管理**: 所有配置通过环境变量注入，使用 `viper` 统一管理，禁止硬编码
9. **Go 版本**: Go 1.22+，开启 `-race` detector 跑 CI 测试
10. **测试要求**: 撮合引擎核心逻辑（orderbook、matcher、recovery）单元测试覆盖率 ≥ 80%；Order Service 的 Outbox Relay 与状态机迁移需有集成测试
11. **WAL 原则**: 撮合引擎任何状态变更必须先写 WAL 再改内存；绝对禁止先改内存后写 WAL

---

## 10. 假设与风险


| #   | 假设/风险                                                      | 缓解策略                                                                               |
| --- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| A1  | 每个交易对由单一 `SymbolEngine` 串行处理；一个 Matching Engine 进程可承载多个交易对 | 保证单 symbol 撮合确定性，同时提升冷门交易对资源利用率；若单 symbol TPS > 50k，可迁移到独占 shard 或引入更高性能的单线程事件循环   |
| A2  | 撮合引擎本地 SSD 不会永久故障                                          | 定期 Snapshot 备份至对象存储；PVC 使用云盘保证可用性                                                  |
| A3  | Kafka 作为命令源，撮合引擎不会收到同一 symbol 的乱序命令                        | 利用 `symbol -> shard -> partition` 固定映射保证同一 symbol 始终进入同一有序分区；迁移 symbol 时必须通过受控流程完成 |
| A4  | 未设计账户服务（Account Service）独立模块                               | Phase 1 余额逻辑在 Order Service 中，后续可拆分                                                |
| R1  | 外部指数价格源不可用                                                 | 保留最后有效值 + 告警；超过 60 秒无更新触发熔断                                                        |
| R2  | WAL 磁盘写满导致撮合引擎停止接单                                         | 监控磁盘使用率告警 80%；自动删除已快照覆盖的旧 WAL                                                      |
| R3  | 恢复对账发现差异但无法自动处理                                            | 进入"只读模式"拒绝新命令，发出告警等待人工介入                                                           |
| R4  | Outbox Relay 停滞导致大量 PENDING 订单未进撮合                         | 监控 `outbox_pending_count`；自动扩容 Relay；超时任务将长期未确认订单置 `REJECTED` 并解冻                  |
| R5  | `CANCELING` 窗口内再次发生成交                                      | 撮合按命令顺序处理；撤单命令到达前已成交部分保留，仅撤销剩余数量；客户端以最终 `status` 为准                                |


