# Matching Engine 接口说明

**版本**: 1.0  
**日期**: 2026-05-22  
**状态**: 与当前代码一致（第 3 步）  
**关联**: [kafka-data.md](./kafka-data.md) · [architecture-spec.md](./architecture-spec.md)

Matching Engine（`cmd/matching`）是**撮合执行服务**，对外不提供 HTTP/gRPC。与上下游的契约主要是：

| 接口类型 | 用途 | 状态 |
|----------|------|------|
| **Kafka 消费** | 接收撮合命令 | ✅ 已实现 |
| **Kafka 发布** | 输出订单状态与成交 | ✅ 已实现 |
| **JSONL（stdin/stdout）** | 本地调试 | ✅ 已实现 |
| **gRPC/REST 查询** | 查用户订单、深度等 | ❌ 不在本服务（见 Order / Market Data） |

---

## 1. 服务边界

```text
  Order Service (未来)                Matching Engine              下游 (未来)
        │                                    │                          │
        │  order.commands (protobuf)         │  match.events            │ Order Service
        └──────────────────────────────────►│  trade.events            ├► Market Data
                                             │                          └► Kline ...
```

**职责**

- 按价格-时间优先撮合限价/市价单
- 先写 WAL（`fsync`）再改内存
- 发布撮合结果事件

**不负责**

- 用户鉴权、余额、订单持久化查询（Order Service + PostgreSQL）
- 对外 REST/WebSocket（API Gateway）

---

## 2. 进程与配置

### 2.1 启动

```bash
# 编译
make build

# Kafka 模式（生产联调）
./bin/matching -config configs/matching.kafka.json

# 或使用脚本
./scripts/matching.sh start --build

# JSONL 本地模式
./bin/matching -config configs/matching.json
# 或
MATCHING_CONFIG=configs/matching.json ./scripts/matching.sh start
```

| 参数 | 说明 |
|------|------|
| `-config <path>` | JSON 配置文件路径，默认 `configs/matching.json` |

### 2.2 配置文件

#### 通用字段

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `data_dir` | string | `data` | WAL、快照根目录 |
| `shard_id` | string | `shard-0` | 分片 ID，对应 `data/wal/{shard_id}/` |
| `snapshot_every` | uint | `10000` | 每 N 条 WAL 命令触发快照 |
| `snapshot_on_exit` | bool | `true` | SIGTERM 退出前写快照 |
| `commands_file` | string | `""` | JSONL 模式：命令文件路径；空则读 stdin |
| `default_symbol` | string | `BTC-USDT` | JSONL 未指定 `symbol` 时使用 |
| `log.*` | object | — | 见下表 |

#### `log` 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `level` | string | `debug` / `info` / `warn` / `error`，默认 `info` |
| `dev` | bool | 控制台彩色输出 |
| `file` | string | 日志文件路径，如 `logs/matching.log` |
| `async` | bool | 异步写文件（配置 `file` 时默认真） |
| `buffer_size` | int | 异步缓冲区，默认 `512` |
| `max_size_mb` / `max_age_days` / `max_backups` | int | 日志轮转 |
| `compress` / `local_time` / `rotate_daily` | bool | 轮转选项 |

#### `kafka` 字段（`enabled: true` 时必填）

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `enabled` | bool | `false` | 为 `true` 时走 Kafka，否则 JSONL |
| `brokers` | string[] | — | 如 `["localhost:9092"]` |
| `group_id` | string | `matching-{shard_id}` | 消费组 ID |
| `command_topic` | string | `order.commands` | 命令 topic |
| `match_topic` | string | `match.events` | 订单状态 topic |
| `trade_topic` | string | `trade.events` | 成交 topic |
| `partition` | int | `0` | 本进程消费的固定分区 |

**示例文件**

- JSONL：`configs/matching.json`
- Kafka：`configs/matching.kafka.json`

### 2.3 优雅退出

- 信号：`SIGINT`、`SIGTERM`
- 行为：停止消费循环；若 `snapshot_on_exit=true` 写快照；关闭 WAL
- Kafka：仅在**整条命令处理成功**（含 WAL、发布）后提交 offset

---

## 3. Kafka 接口

### 3.1 Topic 一览

| Topic | 方向 | 分区（开发） | 消息编码 | 说明 |
|-------|------|--------------|----------|------|
| `order.commands` | **入站** | 0（可配置） | Protobuf | 撮合命令 |
| `match.events` | **出站** | — | Protobuf | 订单状态变更 |
| `trade.events` | **出站** | — | Protobuf | 成交记录 |

创建 topic（开发环境）：

```bash
./scripts/kafka-create-topics.sh
```

### 3.2 消费语义

| 项 | 行为 |
|----|------|
| 提交方式 | **手动 commit**（`Process` 成功后） |
| 与 WAL 顺序 | 先 `wal.Append` + `fsync`，再改盘口，再发事件，最后 commit |
| 重复投递 | `order_id` 幂等：重复单不写 WAL、不发事件，但仍 commit |
| 启动位点 | 扫描 WAL 中最大 `kafka_offset`，从 `offset+1` 续消费；无记录则从 latest |

**消息 Key**：建议为 `symbol` 字符串（当前 Producer/Consumer 不强制校验 Key）。

---

## 4. 入站消息：`order.commands`

### 4.1 顶层：`OrderCommandEnvelope`

Proto：`proto/matching/v1/envelope.proto`

```protobuf
message OrderCommandEnvelope {
    oneof command {
        NewOrderCommand new_order = 1;
        CancelOrderCommand cancel_order = 2;
    }
}
```

**序列化**：整条 Kafka `message.value` = `proto.Marshal(OrderCommandEnvelope)`，**非 JSON**。

**注意**：不要只发送 `NewOrderCommand` 裸消息；必须包在 `OrderCommandEnvelope` 内。

### 4.2 `NewOrderCommand`（下单）

Proto：`proto/matching/v1/commands.proto`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `command_id` | uint64 | 建议 | 命令幂等/trace ID；为 0 时用 `order.order_id` |
| `order` | `common.v1.Order` | 是 | 订单详情 |
| `kafka_partition` | uint32 | 否 | 由 Matching 消费时写入并落 WAL |
| `kafka_offset` | uint64 | 否 | 由 Matching 消费时写入并落 WAL |

#### `common.v1.Order`

Proto：`proto/common/v1/types.proto`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `order_id` | uint64 | 是 | 系统订单号（Order Service 发号） |
| `client_order_id` | string | 否 | 客户端幂等键 |
| `symbol` | string | 是 | 交易对，如 `BTC-USDT` |
| `create_time` / `update_time` | Timestamp | 否 | 订单时间 |
| `side` | enum | 是 | `SIDE_BUY` / `SIDE_SELL` |
| `type` | enum | 是 | `ORDER_TYPE_LIMIT` / `ORDER_TYPE_MARKET` |
| `price` | Decimal | 限价必填 | 字符串小数，如 `"100.5"` |
| `quantity` | Decimal | 是 | 委托数量 |
| `remaining` | Decimal | 否 | 剩余量；空或 0 视为等于 `quantity` |
| `flags` | uint64 | 否 | 扩展标志 |

**撮合规则（当前实现）**

- 限价：价格-时间优先；未成交部分挂入订单簿
- 市价：吃掉对手盘；剩余丢弃
- 重复 `order_id`：幂等忽略（不重复撮合）

### 4.3 `CancelOrderCommand`（撤单）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `command_id` | uint64 | 建议 | 为 0 时用 `order_id` |
| `symbol` | string | 是 | 交易对 |
| `order_id` | uint64 | 是 | 待撤订单 |
| `kafka_partition` / `kafka_offset` | — | 否 | 消费时由 Matching 填充 |

**行为**：从盘口移除；订单不存在时**幂等成功**（仍写 WAL、发 `ORDER_CANCELED`）。

### 4.4 入站示例（伪代码）

```go
env := &matchingv1.OrderCommandEnvelope{
    Command: &matchingv1.OrderCommandEnvelope_NewOrder{
        NewOrder: &matchingv1.NewOrderCommand{
            CommandId: 1001,
            Order: &commonv1.Order{
                OrderId:  42,
                Symbol:   "BTC-USDT",
                Side:     commonv1.Side_SIDE_BUY,
                Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
                Price:    &commonv1.Decimal{Value: "100"},
                Quantity: &commonv1.Decimal{Value: "1"},
            },
        },
    },
}
payload, _ := proto.Marshal(env)
// kafka.Produce("order.commands", key=[]byte("BTC-USDT"), value=payload)
```

---

## 5. 出站消息

### 5.1 `match.events` — `MatchEvent`

Proto：`proto/matching/v1/events.proto`

| 字段 | 类型 | 说明 |
|------|------|------|
| `command_id` | uint64 | 关联命令 |
| `symbol` | string | 交易对 |
| `order_id` | uint64 | 订单号 |
| `event_type` | enum | 见下表 |
| `order` | `common.v1.Order` | 可选快照 |
| `wal_seq` | uint64 | 本 shard WAL 序号 |

#### `MatchEventType`

| 值 | 名称 | 触发场景 |
|----|------|----------|
| 1 | `ORDER_ACCEPTED` | 新单进入撮合（非重复） |
| 2 | `ORDER_FILLED` | 订单完全成交或已离开盘口 |
| 3 | `ORDER_PARTIAL_FILLED` | 部分成交，盘口仍有剩余 |
| 4 | `ORDER_CANCELED` | 撤单命令处理完成 |

**单笔新单典型事件顺序**

1. `ORDER_ACCEPTED`（吃单方）
2. 若有成交：每个 maker 一条 `FILLED` / `PARTIAL_FILLED`；吃单方一条 `FILLED` / `PARTIAL_FILLED`
3. 每笔成交另有一条 `trade.events`（见下）

**重复 `order_id`**：不产生任何出站事件。

### 5.2 `trade.events` — `TradeEvent`

| 字段 | 类型 | 说明 |
|------|------|------|
| `trade` | `common.v1.Trade` | 成交详情 |
| `wal_seq` | uint64 | WAL 序号 |

#### `common.v1.Trade`

| 字段 | 类型 | 说明 |
|------|------|------|
| `trade_id` | uint64 | 确定性 ID，见 §5.3 |
| `symbol` | string | 交易对 |
| `create_time` | Timestamp | 成交时间 |
| `price` / `quantity` | Decimal | 成交价、量 |
| `maker_order_id` | uint64 | 挂单方 |
| `taker_order_id` | uint64 | 吃单方 |

### 5.3 `trade_id` 生成规则

与 WAL 命令序号绑定，回放可复现：

```text
trade_id = FNV-64a( big_endian(maker_order_id) ||
                      big_endian(taker_order_id) ||
                      big_endian(wal_command_seq) )
```

实现：`internal/matching/engine/trade_id.go` — `DeriveTradeID(commandSeq, makerOrderID, takerOrderID)`。

下游应用 `trade_id` 做幂等入库。

---

## 6. JSONL 调试接口（3.1）

`kafka.enabled=false` 时，进程从 **stdin**（或 `commands_file`）读入**一行一个 JSON**，向 **stdout** 输出一行 JSON 结果。  
**不**经过 Kafka，**不**发布 `match.events` / `trade.events`。

### 6.1 命令

#### `new_order`

请求：

```json
{
  "op": "new_order",
  "order_id": 1,
  "symbol": "BTC-USDT",
  "side": "buy",
  "type": "limit",
  "price": "100",
  "quantity": "1",
  "client_order_id": "optional",
  "command_id": 1
}
```

| 字段 | 说明 |
|------|------|
| `side` | `buy`/`b` 或 `sell`/`s` |
| `type` | 空/`limit`/`l` 或 `market`/`m` |
| `symbol` | 可省略，用配置 `default_symbol` |

成功响应：

```json
{
  "ok": true,
  "op": "new_order",
  "last_seq": 1,
  "trades": [
    {
      "trade_id": 123,
      "symbol": "BTC-USDT",
      "price": "100",
      "quantity": "1",
      "maker_order_id": 2,
      "taker_order_id": 1
    }
  ]
}
```

重复单：

```json
{ "ok": true, "op": "new_order", "duplicate": true, "last_seq": 1 }
```

#### `cancel_order` / `cancel`

```json
{ "op": "cancel_order", "symbol": "BTC-USDT", "order_id": 1 }
```

#### `snapshot`

```json
{ "op": "snapshot" }
```

立即对所有 symbol 写快照。

#### `status` / `book`

```json
{ "op": "status", "symbol": "BTC-USDT" }
```

响应含 `best_bid`、`best_ask`、`active_count`、`active_orders`。

#### `quit` / `exit`

```json
{ "op": "quit" }
```

### 6.2 错误响应

```json
{ "ok": false, "op": "new_order", "error": "order_id is required" }
```

---

## 7. 持久化与恢复（实现约束）

| 路径 | 内容 |
|------|------|
| `data/wal/{shard_id}/wal_*.log` | 命令 WAL（protobuf payload） |
| `data/snapshots/{shard_id}/{symbol}/snapshot_*.pb` | 盘口快照 |
| `data/snapshots/{shard_id}/manifest.pb` | 分片 manifest |

**重启**：加载快照 → 回放 WAL → Kafka 从 `max(kafka_offset)+1` 续消费。  
回放阶段**不**重发 `match.events` / `trade.events`。

---

## 8. 与 Order Service 的对接清单（第 4 步）

| 步骤 | Order Service | Matching |
|------|---------------|----------|
| 发命令 | Outbox → `OrderCommandEnvelope` → `order.commands` | 消费并 WAL |
| 收状态 | 消费 `match.events` 更新 `orders.status` | 发布 |
| 收成交 | 消费 `trade.events` 写 `trades` | 发布 |
| 查单 | `GetOrder` / `ListOrders` 读 DB | **不提供** |

`order_id` 必须由 Order Service 在落库时生成，Matching 只消费，不分配订单号。

---

## 9. 限制与后续扩展

| 项 | 当前状态 |
|----|----------|
| 交易对 | 未注册 symbol 时自动创建默认引擎（便于开发） |
| 订单类型 | 仅 `LIMIT`、`MARKET` |
| 多分片 / 多 partition | 单进程消费一个 partition |
| 查询类 API | 不做；请用 Order Service 或未来 gRPC 只读接口 |
| 新写命令 | 扩展 `commands.proto` + `envelope.oneof` + WAL 事件类型 |

---

## 10. Proto 源文件索引

| 文件 | 内容 |
|------|------|
| `proto/matching/v1/envelope.proto` | Kafka 命令信封 |
| `proto/matching/v1/commands.proto` | `NewOrderCommand` / `CancelOrderCommand` |
| `proto/matching/v1/events.proto` | `MatchEvent` / `TradeEvent` |
| `proto/common/v1/types.proto` | `Order` / `Trade` / `Decimal` |
| `proto/matching/v1/snapshot.proto` | 快照（内部恢复，非 Kafka 接口） |

生成 Go 代码：

```bash
./scripts/gen-proto.sh
# 输出目录 pkg/pb/
```

---

## 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-05-22 | 初稿：Kafka 契约、JSONL、配置、事件语义 |
