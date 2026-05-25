# Order Service API Documentation

**Version**: 1.0  
**Date**: 2026-05-24  
**Status**: 与当前代码一致（第 4 步 4.1）  
**关联**: [development-roadmap.md](./development-roadmap.md) · [matching-api.md](./matching-api.md) · [architecture-spec.md](./architecture-spec.md)

Order Service（`cmd/order`）对外提供 **gRPC** 接口；REST 由第 5 步 API Gateway 封装。与 Matching Engine 的异步契约见 [matching-api.md §4](./matching-api.md#4-入站消息ordercommands)。

---

## 1. 服务边界

```text
  Client / grpcurl          Order Service              Matching Engine
        │                        │                           │
        │  gRPC PlaceOrder         │  order.commands (Kafka)   │
        └───────────────────────►│──────────────────────────►│
                                 │  PostgreSQL (orders)      │
                                 │  （4.1 无 Outbox）         │
```

**职责（4.1）**

- 接收下单请求，校验参数
- `client_order_id` 幂等（DB 唯一索引）
- 写入 `orders`（`status=PENDING`）
- **直接**发布 `OrderCommandEnvelope` 到 Kafka（尚未 Transactional Outbox）

**不负责**

- 撮合执行（Matching Engine）
- 消费 `match.events` / `trade.events` 回写（4.1 后续子步骤）
- 余额冻结（见 §3.4）

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

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `grpc_listen` | string | `:50051` | gRPC 监听地址 |
| `database_url` | string | — | PostgreSQL DSN |
| `migrate_on_start` | bool | `true` | 启动时执行内嵌 migration |
| `default_symbol` | string | `BTC-USDT` | 文档/联调默认交易对 |
| `kafka.brokers` | string[] | — | 如 `["localhost:9092"]` |
| `kafka.command_topic` | string | `order.commands` | 命令 topic |
| `kafka.partition` | int | `0` | 开发环境固定分区（与 matching 一致） |
| `log.*` | object | — | 同 Matching，见 matching-api |

### 2.3 优雅退出

- 信号：`SIGINT`、`SIGTERM`
- 行为：停止 gRPC、关闭 DB 连接池与 Kafka writer

---

## 3. gRPC 接口

Proto：`proto/order/v1/order.proto`  
Package：`order.v1`  
Go import：`github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1`

### 3.1 `PlaceOrder`

**RPC:** `order.v1.OrderService/PlaceOrder`

**请求 `PlaceOrderRequest`**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `user_id` | uint64 | 是 | 用户 ID（4.1 联调可传固定值，如 `1`） |
| `client_order_id` | string | 是 | 幂等键，最长 64 |
| `symbol` | string | 是 | 交易对，如 `BTC-USDT` |
| `side` | `common.v1.Side` | 是 | `SIDE_BUY` / `SIDE_SELL` |
| `type` | `common.v1.OrderType` | 是 | `ORDER_TYPE_LIMIT` / `ORDER_TYPE_MARKET` |
| `price` | `common.v1.Decimal` | LIMIT 必填；MARKET 卖不需要 | 字符串小数，如 `{ "value": "65000.50" }`；**市价买单**见 [§3.4](#34-余额冻结) |
| `quantity` | `common.v1.Decimal` | 是 | 字符串小数，如 `{ "value": "0.01" }` |

**响应 `PlaceOrderResponse`**

| 字段 | 类型 | 说明 |
|------|------|------|
| `order_id` | uint64 | 系统订单号（PostgreSQL 发号） |
| `client_order_id` | string | 回显 |
| `symbol` | string | 交易对 |
| `status` | string | 初始为 `PENDING` |
| `created_at` | Timestamp | 创建时间 |
| `idempotent_hit` | bool | `true` 表示命中幂等，返回已有订单 |

**语义**

- 成功：订单已落库；Kafka 命令已发出（4.1 **不保证** Outbox 级可靠投递）
- 幂等：相同 `user_id` + `client_order_id` 返回同一 `order_id`，**不**重复发 Kafka
- **不保证**已撮合或已成交；需后续 `GetOrder` 或消费回写（4.1 尚未实现查询 RPC）

**gRPC 状态码**

| code | 场景 |
|------|------|
| `OK` | 成功或幂等命中 |
| `InvalidArgument` | 参数校验失败 |
| `Internal` | DB / Kafka 错误 |

### 3.2 grpcurl 示例

列出服务（需安装 [grpcurl](https://github.com/fullstorydev/grpcurl)）：

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe order.v1.OrderService
```

限价买单：

```bash
grpcurl -plaintext -d '{
  "user_id": 1,
  "client_order_id": "demo-001",
  "symbol": "BTC-USDT",
  "side": "SIDE_BUY",
  "type": "ORDER_TYPE_LIMIT",
  "price": { "value": "100" },
  "quantity": { "value": "1" }
}' localhost:50051 order.v1.OrderService/PlaceOrder
```

预期响应（字段随实际变化）：

```json
{
  "orderId": "1",
  "clientOrderId": "demo-001",
  "symbol": "BTC-USDT",
  "status": "PENDING",
  "createdAt": "2026-05-24T12:00:00Z",
  "idempotentHit": false
}
```

重复相同 `client_order_id`：

```json
{
  "orderId": "1",
  "clientOrderId": "demo-001",
  "status": "PENDING",
  "idempotentHit": true
}
```

### 3.4 余额冻结

`PlaceOrder` 在同事务内调用 `ComputeFreeze` + `lockFunds`（`account_balances.frozen`）。

| 类型 | 方向 | 冻结规则（当前） |
|------|------|------------------|
| LIMIT | BUY | quote = `price × quantity` |
| LIMIT | SELL | base = `quantity` |
| MARKET | SELL | base = `quantity` |
| MARKET | BUY | **必须**传 `price` 作临时保护价；否则 `InvalidArgument` |

**市价买单（目标方案，未实现）**：按 Market Data 返回的 **Best Ask / Mark Price** 估算，加滑点缓冲后冻结 quote；用户无需填 `price`。详见 [design/market-buy-freeze.md](./design/market-buy-freeze.md)（**方案 C**），在 **第 6 步 Market Data Service** 就绪后实现。

---

## 4. 下游 Kafka：`order.commands`

PlaceOrder 成功后（非幂等命中），Order Service 发布：

| 项 | 值 |
|----|-----|
| Topic | `order.commands`（可配置） |
| Key | `symbol` 字符串 |
| Value | `proto.Marshal(OrderCommandEnvelope)` |
| 内层命令 | `NewOrderCommand{ command_id: order_id, order: ... }` |

**必须与 Matching 一致**，详见 [matching-api.md §4](./matching-api.md#4-入站消息ordercommands)。

**注意（4.1 局限）**：DB 提交与 Kafka 发送**不在同一事务**；Kafka 失败会导致 DB 有单但撮合未收到命令。第 4 步 4.2 将改为 Transactional Outbox。

---

## 5. 数据库（4.1）

启动时若 `migrate_on_start=true`，执行 `migrations/001_create_orders.up.sql`（内嵌于 `internal/order/repository`）。

| 表 | 说明 |
|----|------|
| `orders` | 订单主表，`status` 初始 `PENDING` |
| `client_order_idempotency` | `(user_id, client_order_id)` 主键，幂等 |

---

## 6. 端到端联调（4.1）

```bash
# 1. 基础设施
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh

# 2. Matching（Kafka 模式）
./scripts/matching.sh start --build

# 3. Order Service
make build
./bin/order -config configs/order.json

# 4. 下单
grpcurl -plaintext -d '{
  "user_id": 1,
  "client_order_id": "e2e-001",
  "symbol": "BTC-USDT",
  "side": "SIDE_SELL",
  "type": "ORDER_TYPE_LIMIT",
  "price": { "value": "100" },
  "quantity": { "value": "1" }
}' localhost:50051 order.v1.OrderService/PlaceOrder

# 5. 验证 Matching 日志 / WAL 中有该 order_id
```

---

## 7. 后续 RPC（尚未实现）

| RPC | 计划步骤 |
|-----|----------|
| `CancelOrder` | 4.1 后续 / 4.2 |
| `GetOrder` | 4.1 子步骤 6 |
| `ListOrders` | 可选，第 5 步 Gateway |

---

## 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-05-24 | 初稿：PlaceOrder gRPC + 直连 Kafka（4.1） |
| 1.1 | 2026-05-24 | §3.4 余额冻结；市价买方案 C 见 design/market-buy-freeze.md |
