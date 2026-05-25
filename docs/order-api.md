# Order Service API Documentation

**Version**: 1.2  
**Date**: 2026-05-25  
**Status**: 与当前代码一致（第 4 步）  
**关联**: [development-roadmap.md](./development-roadmap.md) · [matching-api.md](./matching-api.md) · [architecture-spec.md](./architecture-spec.md)

Order Service（`cmd/order`）对外提供 **gRPC**；REST 由第 5 步 API Gateway 封装。异步契约见 [matching-api.md §4](./matching-api.md#4-入站消息ordercommands)。

---

## 1. 服务边界

```text
  Client / grpcurl          Order Service                    Matching Engine
        │                        │                                │
        │  PlaceOrder/Cancel      │  Outbox Relay → order.commands │
        └───────────────────────►│───────────────────────────────►│
                                 │  PostgreSQL                     │
                                 │  ← match.events / trade.events │
                                 │  reconciler（超时补偿）          │
```

**职责**

- 下单 / 撤单 / 查单 / 列表；`client_order_id` 幂等
- 同事务：冻结余额 + `orders` + `order_outbox`（Transactional Outbox）
- 后台 Relay 投递 Kafka；消费撮合回写状态与成交结算
- 超时补偿：超时 `PENDING` 拒单 + Cancel Outbox；`CANCELING` 重发撤单
- 资产查询与联调充值（`BalanceService`）

**不负责**

- 撮合执行（Matching Engine）
- 行情与市价买冻结方案 C（见 [design/market-buy-freeze.md](./design/market-buy-freeze.md)）

---

## 2. 进程与配置

### 2.1 启动

```bash
make build
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh
./scripts/matching.sh start --build
./bin/order -config configs/order.json
```

| 参数 | 说明 |
|------|------|
| `-config <path>` | JSON 配置文件，默认 `configs/order.json` |

### 2.2 配置文件 `configs/order.json`

| 字段 | 说明 |
|------|------|
| `grpc_listen` | gRPC 监听，默认 `:50051` |
| `database_url` | PostgreSQL DSN（开发环境注意端口，避免与本机 5432 冲突） |
| `migrate_on_start` | 启动时执行内嵌 migration（`internal/order/repository/migrations`） |
| `kafka.command_topic` | 默认 `order.commands` |
| `kafka.match_topic` / `trade_topic` | 消费回写 |
| `kafka.consumer_enabled` | 是否启动 match/trade 消费者 |
| `kafka.partition` | 开发环境固定 `0` |
| `reconciler.*` | 超时补偿，见 §5 |

### 2.3 优雅退出

- 信号：`SIGINT`、`SIGTERM`
- 停止 gRPC、Outbox Relay、reconciler、Kafka consumer/writer、DB 连接池

---

## 3. gRPC：`OrderService`

Proto：`proto/order/v1/order.proto`

### 3.1 `PlaceOrder`

同事务：幂等 → 冻结 → `orders(PENDING)` → `order_outbox`。**不**在请求线程直接发 Kafka。

- 幂等命中：返回已有订单，`idempotent_hit=true`，不重复写 Outbox
- 成功：保证 DB 已提交；撮合命令由 Relay 异步投递
- 余额不足：`FailedPrecondition`

### 3.2 `CancelOrder`

可撤单状态：`PENDING` / `ACCEPTED` / `PARTIAL` → `CANCELING` + 撤单 Outbox。

### 3.3 `GetOrder` / `ListOrders`

- `GetOrder`：`user_id` + `order_id`，响应 `OrderInfo`
- `ListOrders`：仅 `user_id` 必填；`symbol` / `side` / `type` / `status` / 时间范围可选；默认 `page=1`、`page_size=20`（最大 100）

### 3.4 余额冻结（下单）

| 类型 | 方向 | 规则 |
|------|------|------|
| LIMIT | BUY/SELL | `price×qty` / `qty` |
| MARKET | SELL | `qty`（base） |
| MARKET | BUY | 暂需 `price` 作保护价；方案 C 见 design 文档 |

### 3.5 grpcurl 示例

需指定 proto（未开 gRPC reflection）：

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
PROTO_ARGS="-import-path proto -proto proto/common/v1/types.proto -proto proto/order/v1/order.proto"

grpcurl -plaintext $PROTO_ARGS localhost:50051 list

grpcurl -plaintext $PROTO_ARGS -d '{
  "user_id": 1,
  "client_order_id": "demo-001",
  "symbol": "BTC-USDT",
  "side": "SIDE_BUY",
  "type": "ORDER_TYPE_LIMIT",
  "price": { "value": "100" },
  "quantity": { "value": "1" }
}' localhost:50051 order.v1.OrderService/PlaceOrder

grpcurl -plaintext $PROTO_ARGS -d '{"user_id":1,"order_id":1}' \
  localhost:50051 order.v1.OrderService/GetOrder

grpcurl -plaintext $PROTO_ARGS -d '{"user_id":1}' \
  localhost:50051 order.v1.OrderService/ListOrders
```

联调前先充值（`BalanceService`）：

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/common/v1/types.proto \
  -proto proto/order/v1/balance.proto \
  -d '{"user_id":1,"asset":"USDT","business":"deposit","business_id":1001,"change":{"value":"10000"}}' \
  localhost:50051 order.v1.BalanceService/UpdateBalance
```

---

## 4. gRPC：`BalanceService`

Proto：`proto/order/v1/balance.proto`

| RPC | 说明 |
|-----|------|
| `GetBalance` | `user_id` + `asset` |
| `ListBalances` | `user_id` 下全部资产 |
| `UpdateBalance` | 调账/充值；幂等键 `business` + `business_id` |

---

## 5. 下游 Kafka 与后台任务

### 5.1 Outbox → `order.commands`

| 项 | 值 |
|----|-----|
| 投递方 | `internal/order/outbox/relay.go`（独立 goroutine） |
| Value | `OrderCommandEnvelope`（`command_id` = `order_outbox.id`） |
| Key | `symbol` |

### 5.2 入站事件

| Topic | 作用 |
|-------|------|
| `match.events` | 更新 `orders.status`、释放冻结等 |
| `trade.events` | 写 `trades`、结算 `account_balances` |

### 5.3 Reconciler（§4.5）

| 场景 | 动作 |
|------|------|
| `PENDING` + 已发 NewOrder + 超时 | `REJECTED` + 解冻 + **Cancel Outbox** |
| `CANCELING` + 无待发撤单 Outbox + 超时 | 补写 Cancel Outbox |
| 长期未发布 Outbox | WARN 日志（仍由 Relay 重试） |

配置段：`reconciler` in `configs/order.json`。

---

## 6. 数据库

内嵌迁移 `001`～`007`（与根目录 `migrations/` 一致）。

| 表 | 说明 |
|----|------|
| `orders` | 订单主表 + 乐观锁 `version` |
| `order_outbox` | Transactional Outbox |
| `client_order_idempotency` | 下单幂等 |
| `account_balances` | 余额 + 冻结 |
| `trades` | 成交幂等（`trade_id`） |
| `processed_match_events` | match 消费幂等 |
| `balance_adjust_idempotency` | 调账幂等 |

索引：`idx_orders_user_status`、`idx_orders_user_id_desc`（ListOrders）等。

---

## 7. 端到端联调清单

1. 基础设施：compose + `kafka-create-topics.sh`
2. `./scripts/matching.sh start --build`
3. `./bin/order -config configs/order.json`
4. `UpdateBalance` 充值 → `PlaceOrder` → 查 Matching / `GetOrder` 状态
5. 撤单：`CancelOrder` → 终态 `CANCELED`
6. （可选）停 Order 再启，确认未发布 Outbox 仍会投递

---

## 8. 尚未实现

| 项 | 阶段 |
|----|------|
| API Gateway REST | 第 5 步 |
| 市价买行情冻结（方案 C） | 第 6 步 + Market Data |
| Matching 对账 gRPC | Phase 2+ |
| testcontainers 集成测试 | 第 4 步收尾可选 |

---

## 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-05-24 | 初稿（4.1 直连 Kafka） |
| 1.1 | 2026-05-24 | 余额冻结与市价买方案 C 文档 |
| 1.2 | 2026-05-25 | 对齐第 4 步完整实现：Outbox、消费者、ListOrders、Balance、reconciler |
