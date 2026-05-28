# API Gateway — REST 接口文档

**版本**: 1.5  
**日期**: 2026-05-26  
**状态**: 草稿（内网 Gateway 定位说明 + 余额 REST + 请求指定 user_id）  
**关联**: [architecture-spec.md](./architecture-spec.md) · [development-roadmap.md](./development-roadmap.md) · [gateway-development-plan.md](./gateway-development-plan.md)（第 5 步实现清单）

---

## 1. 概述

### 1.1 API Gateway 的角色

本文档描述的是 **API Gateway 的 REST 契约**（`cmd/gateway`），用于 **内网服务集成**：将 HTTP 转为 Order / 行情等 gRPC 调用，**不**直接访问 Kafka 或撮合引擎。

**推荐生产拓扑**（详见 [architecture-spec.md §2.1.1](./architecture-spec.md#211-部署形态公网-gateway-vs-内网-gateway--webbff)）：

```
终端用户 (Web / App)
        │  HTTPS（公网，用户文档由 Web/BFF 定义）
        ▼
   Web / BFF 服务         ← 登录、充值流程、聚合 API、风控
        │  HTTP（VPC 内网，不对 Internet 暴露）
        ▼
   API Gateway            ← 本文档描述的范围（内网）
        │  gRPC
        ├── Order Service      （下单、撤单、订单查询、余额读写）
        ├── Market Data Service（深度、Ticker）
        ├── Kline Service      （K 线历史）
        └── Index Price Service（指数价格）
```


| 说明              | 内容                                                      |
| --------------- | ------------------------------------------------------- |
| **谁调用 Gateway** | Web/BFF、运营后台、清算服务；**不是**浏览器直连（生产）                       |
| **Phase 1 联调**  | 常在本机 `localhost:8080` 用 Bearer 测通；等价于「内网调用方」，不代表对终端用户开放 |
| **WebSocket**   | Phase 2+：`GET /v1/ws` 由 Gateway 管理连接，数据来自 Push / Redis  |


### 1.2 与 Web / BFF 的职责边界


| 能力           | Web / BFF（公网）      | API Gateway（内网，本文）                |
| ------------ | ------------------ | --------------------------------- |
| 用户登录、Session | ✅                  | ❌                                 |
| 下单 / 撤单 / 查单 | 转发（带真实 `user_id`）  | ✅ `POST/DELETE/GET /v1/orders`    |
| 查余额          | 可对用户暴露             | ✅ `GET /v1/balances`              |
| 充值到账加余额      | 支付/链上确认后调用 Gateway | ✅ `POST /v1/balances`（**仅内网调用方**） |
| 终端用户直接调账     | ❌                  | ❌（勿将 Gateway 暴露公网）                |


Gateway 保持 **薄**：协议转换、统一信封、`X-Request-Id`、gRPC 错误映射；业务编排（支付、KYC、聚合首页）在 Web 完成。

### 1.3 设计原则


| 原则        | 说明                                                        |
| --------- | --------------------------------------------------------- |
| 最终一致      | `POST /orders` 成功表示订单**已落库**；**不保证**已进盘口或已成交，需轮询或 WS 订阅状态 |
| 幂等下单      | 同一 `client_order_id` 重复提交返回同一 `order_id`                  |
| 无跨服务 REST | 仅 Gateway 对外暴露 REST；服务间禁止 REST 互调                         |
| 精度        | 价格、数量使用 **字符串** 传递十进制，避免浮点误差                              |


### 1.4 订单标识


| 字段                | 类型     | 说明                       |
| ----------------- | ------ | ------------------------ |
| `client_order_id` | string | 客户端幂等 ID，用户自定义，最长 64     |
| `order_id`        | uint64 | 系统订单号，由 Order Service 分配 |


**JSON 约定**：`order_id` 在请求/响应 JSON 中写作**十进制字符串**（如 `"1000000001"`），语义为无符号 64 位整数；禁止浮点数。URL 路径同理：`/v1/orders/1000000001`。

**Protobuf / gRPC**：`order_id` 为 `uint64`；`client_order_id` 为 `string`。详见 [architecture-spec.md §2.2.1](./architecture-spec.md#221-订单标识order_id--client_order_id)。

### 1.5 基础信息


| 项            | 值                                         |
| ------------ | ----------------------------------------- |
| Base URL（生产） | `https://api.example.com`                 |
| Base URL（开发） | `http://localhost:8080`                   |
| API 前缀       | `/v1`                                     |
| 内容类型         | `application/json; charset=utf-8`         |
| 时间格式         | ISO 8601 UTC，如 `2026-05-20T08:00:00.000Z` |
| 交易对格式        | `BASE-QUOTE`，如 `BTC-USDT`                 |


---

## 2. 认证与通用约定

### 2.1 认证方式

#### Phase 1：Bearer Token + 请求指定用户

Gateway 校验内网调用方 Token；**操作用户**由请求显式传入（Web/BFF 在登录后填入真实 `user_id`）。业务请求头：

```http
Authorization: Bearer <access_token>
X-User-Id: <user_id>
```

`user_id` 为无符号整数，**大于 0**。除下表三种方式外，还可写在 JSON body（见各 POST 接口）。

#### Phase 4：API Key + HMAC 签名（程序化交易）

```http
X-API-KEY: <api_key>
X-TIMESTAMP: <unix_ms>
X-SIGNATURE: <hmac_sha256_hex>
```

签名字符串（示例）：

```
{timestamp}{method}{path}{raw_body}
```

### 2.2 通用请求头


| 头                 | 必填  | 说明                                                              |
| ----------------- | --- | --------------------------------------------------------------- |
| `Authorization`   | 是*  | Bearer Token（写操作与私有读）                                           |
| `X-User-Id`       | 是*  | 操作用户 ID（uint64 十进制字符串）；GET/DELETE 常用；POST 可与 body `user_id` 二选一 |
| `Content-Type`    | 写操作 | `application/json`                                              |
| `X-Request-Id`    | 否   | 客户端追踪 ID；未传时 Gateway 生成并原样返回                                    |
| `Accept-Language` | 否   | 错误文案语言，默认 `zh-CN`                                               |


`**user_id` 传递方式**（优先级：**JSON body** > `**X-User-Id*`* > **query `user_id`**）：


| 方式              | 适用                | 示例                         |
| --------------- | ----------------- | -------------------------- |
| JSON `user_id`  | `POST` 有 body 的接口 | `"user_id": 1`             |
| 请求头 `X-User-Id` | 全部需鉴权接口           | `X-User-Id: 1`             |
| Query `user_id` | `GET` / `DELETE`  | `GET /v1/orders?user_id=1` |


未传或 `user_id=0` 时返回 `400`，`message` 提示缺少 `user_id`。

 公开行情接口（深度、Ticker、K 线、指数价）可不鉴权，由部署策略决定。

### 2.3 统一响应结构

**成功（单资源）**

```json
{
  "code": 0,
  "message": "ok",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": { }
}
```

**成功（列表）**

```json
{
  "code": 0,
  "message": "ok",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "items": [],
    "cursor": "eyJpZCI6MTIzfQ==",
    "has_more": true
  }
}
```

**失败**

```json
{
  "code": 40001,
  "message": "insufficient balance",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": null
}
```

### 2.4 错误码


| HTTP | code  | 含义              |
| ---- | ----- | --------------- |
| 400  | 40000 | 参数校验失败          |
| 401  | 40100 | 未认证或 Token 失效   |
| 403  | 40300 | 无权限             |
| 404  | 40400 | 资源不存在           |
| 409  | 40900 | 冲突（如重复撤单、状态不允许） |
| 422  | 42201 | 余额不足            |
| 422  | 42202 | 订单不可撤销          |
| 429  | 42900 | 限流              |
| 500  | 50000 | 服务内部错误          |
| 503  | 50300 | 服务暂不可用（熔断/维护）   |


### 2.5 限流


| 维度   | 默认配额（可配置）             |
| ---- | --------------------- |
| 按用户  | 下单 100 次/秒；查询 200 次/秒 |
| 按 IP | 1000 次/分钟（全接口）        |


超限响应 `429`，头字段：

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1716192000
Retry-After: 1
```

---

## 3. 订单命令（Order Service）

订单状态机见架构文档 §4.2。客户端应以 `status` 为准，不要假设 HTTP 成功即已成交。

### 3.1 订单状态枚举


| status      | 说明            | 终态  |
| ----------- | ------------- | --- |
| `PENDING`   | 已接受，待撮合确认     | 否   |
| `ACCEPTED`  | 撮合已接单（可能挂在盘口） | 否   |
| `PARTIAL`   | 部分成交          | 否   |
| `CANCELING` | 撤单处理中         | 否   |
| `FILLED`    | 全部成交          | 是   |
| `CANCELED`  | 已撤销           | 是   |
| `REJECTED`  | 拒单（风控/余额/超时等） | 是   |


### 3.2 下单

创建限价单或市价单，写入 Order Service 并进入 Outbox，异步投递撮合。

```http
POST /v1/orders
```

**请求体**


| 字段                | 类型     | 必填       | 说明                                  |
| ----------------- | ------ | -------- | ----------------------------------- |
| `user_id`         | number | 是*       | 用户 ID；也可用 `X-User-Id` / `?user_id=` |
| `client_order_id` | string | 是        | 客户端幂等 ID，最长 64，用户维度唯一               |
| `symbol`          | string | 是        | 交易对，如 `BTC-USDT`                    |
| `side`            | string | 是        | `BUY` | `SELL`                      |
| `type`            | string | 是        | `LIMIT` | `MARKET`                  |
| `price`           | string | LIMIT 必填 | 限价价格                                |
| `quantity`        | string | 是        | 下单数量（base 数量）                       |
| `time_in_force`   | string | 否        | `GTC`（默认）| `IOC` | `FOK`            |


**请求示例**

```json
{
  "user_id": 1,
  "client_order_id": "my-app-20260520-001",
  "symbol": "BTC-USDT",
  "side": "BUY",
  "type": "LIMIT",
  "price": "65000.50",
  "quantity": "0.01",
  "time_in_force": "GTC"
}
```

**响应 `data`**


| 字段                | 类型              | 说明                            |
| ----------------- | --------------- | ----------------------------- |
| `order_id`        | string (uint64) | 系统订单号，十进制字符串，如 `"1000000001"` |
| `client_order_id` | string          | 回显                            |
| `symbol`          | string          | 交易对                           |
| `status`          | string          | 初始一般为 `PENDING`               |
| `created_at`      | string          | 创建时间                          |


**响应示例**

```json
{
  "code": 0,
  "message": "ok",
  "request_id": "a1b2c3d4-...",
  "data": {
    "order_id": "1000000001",
    "client_order_id": "my-app-20260520-001",
    "symbol": "BTC-USDT",
    "status": "PENDING",
    "created_at": "2026-05-20T08:00:00.123Z"
  }
}
```

**语义**

- `201`：新订单创建成功（已持久化 + Outbox）。
- `200`：幂等命中，返回已有订单（相同 `client_order_id`）。
- **不保证**此时已挂单或成交；请 `GET /v1/orders/{id}` 或订阅 WS `order` 频道。

**curl**

```bash
curl -s -X POST "https://api.example.com/v1/orders" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "client_order_id": "my-app-20260520-001",
    "symbol": "BTC-USDT",
    "side": "BUY",
    "type": "LIMIT",
    "price": "65000.50",
    "quantity": "0.01"
  }'
```

---

### 3.3 撤单

```http
DELETE /v1/orders/{order_id}
```

**路径参数**


| 参数         | 说明                        |
| ---------- | ------------------------- |
| `order_id` | 系统订单号（uint64，路径为十进制数字字符串） |


**可选查询参数**


| 参数        | 说明                   |
| --------- | -------------------- |
| `symbol`  | 建议传入，用于 Gateway 路由校验 |
| `user_id` | 是*                   |


**响应 `data`**


| 字段           | 类型              | 说明              |
| ------------ | --------------- | --------------- |
| `order_id`   | string (uint64) | 订单 ID           |
| `status`     | string          | 一般为 `CANCELING` |
| `updated_at` | string          | 更新时间            |


**语义**

- 成功表示已进入 `CANCELING` 且 Outbox 已写入撤单命令。
- **不保证**盘口已移除；终态为 `CANCELED` 时撤单完成。
- 仅 `PENDING` / `ACCEPTED` / `PARTIAL` 可撤；否则 `42202`。

**curl**

```bash
curl -s -X DELETE "https://api.example.com/v1/orders/1000000001?symbol=BTC-USDT&user_id=1" \
  -H "Authorization: Bearer $TOKEN"
```

---

### 3.4 查询单个订单

```http
GET /v1/orders/{order_id}
```

**响应 `data`（Order 对象）**


| 字段                | 类型              | 说明                 |
| ----------------- | --------------- | ------------------ |
| `order_id`        | string (uint64) | 系统订单号              |
| `client_order_id` | string          | 客户端幂等 ID           |
| `symbol`          | string          | 交易对                |
| `side`            | string          | `BUY` | `SELL`     |
| `type`            | string          | `LIMIT` | `MARKET` |
| `price`           | string          | 委托价                |
| `quantity`        | string          | 委托总量               |
| `filled_quantity` | string          | 已成交数量              |
| `avg_price`       | string          | 成交均价，未成交为空字符串      |
| `status`          | string          | 见 §3.1             |
| `time_in_force`   | string          | GTC/IOC/FOK        |
| `version`         | integer         | 乐观锁版本              |
| `created_at`      | string          | 创建时间               |
| `updated_at`      | string          | 最后更新时间             |


---

### 3.5 查询订单列表

```http
GET /v1/orders
```

**查询参数**


| 参数           | 类型      | 必填  | 说明                          |
| ------------ | ------- | --- | --------------------------- |
| `user_id`    | integer | 是*  | 用户 ID（也可用 `X-User-Id`）      |
| `symbol`     | string  | 否   | 过滤交易对                       |
| `status`     | string  | 否   | 多状态逗号分隔，如 `PENDING,PARTIAL` |
| `side`       | string  | 否   | `BUY` | `SELL`              |
| `start_time` | string  | 否   | ISO 8601，创建时间下限             |
| `end_time`   | string  | 否   | ISO 8601，创建时间上限             |
| `limit`      | integer | 否   | 默认 50，最大 200                |
| `cursor`     | string  | 否   | 翻页游标                        |


**响应 `data.items`**：Order 对象数组。

---

### 3.6 账户余额（Order Service / BalanceService）

Gateway 已实现；gRPC 定义见 [order-api.md §4](./order-api.md#4-grpcbalanceservice)。**需鉴权**（`Authorization: Bearer`）。

余额字段均为**十进制字符串**。`available = balance - frozen`（由服务端计算回显）。

#### 3.6.1 查询全部余额

```http
GET /v1/balances
```

**响应 `data`**

```json
{
  "items": [
    {
      "asset": "USDT",
      "balance": "10000.000000000000000000",
      "frozen": "100.000000000000000000",
      "available": "9900.000000000000000000"
    }
  ]
}
```


| 字段                  | 类型     | 说明                     |
| ------------------- | ------ | ---------------------- |
| `items`             | array  | 各资产余额；无记录时为空数组         |
| `items[].asset`     | string | 资产符号，大写，如 `USDT`、`BTC` |
| `items[].balance`   | string | 总余额                    |
| `items[].frozen`    | string | 冻结金额                   |
| `items[].available` | string | 可用余额                   |


**curl**

```bash
curl -s "http://localhost:8080/v1/balances?user_id=1" \
  -H "Authorization: Bearer $TOKEN"
```

---

#### 3.6.2 查询单资产余额

```http
GET /v1/balances/{asset}
```

**路径参数**


| 参数      | 说明                             |
| ------- | ------------------------------ |
| `asset` | 资产符号，如 `USDT`（大小写不敏感，服务端归一为大写） |


**响应 `data`**：与 §3.6.1 中单条 `items[]` 元素结构相同。

不存在该资产记录时：`404` / `code: 40400`。

**curl**

```bash
curl -s "http://localhost:8080/v1/balances/USDT?user_id=1" \
  -H "Authorization: Bearer $TOKEN"
```

---

#### 3.6.3 调账 / 充值（内网 / 联调）

调整可用余额；幂等键为 `business` + `business_id`（与 gRPC `UpdateBalance` 一致）。

**调用方**：仅 **Web/BFF、清算或运营后台**（经内网访问 Gateway），**不是**终端用户浏览器。典型流程：支付回调成功 → Web 校验 → `POST` 本接口 → Order 加余额。

**生产**：Gateway 不对公网暴露；使用服务间鉴权（mTLS / 服务 JWT），与用户 Session 分离。Phase 1 本地 `localhost` + Bearer 仅用于联调。

```http
POST /v1/balances
```

**请求体**


| 字段            | 类型               | 必填  | 说明                             |
| ------------- | ---------------- | --- | ------------------------------ |
| `user_id`     | number           | 是*  | 用户 ID；也可用 `X-User-Id`          |
| `asset`       | string           | 是   | 资产符号，如 `USDT`                  |
| `business`    | string           | 是   | 业务类型，如 `deposit`，最长 32         |
| `business_id` | integer (uint64) | 是   | 业务幂等 ID，同 `business` 重复提交不重复加款 |
| `change`      | string           | 是   | 变动量，十进制；正数充值、负数扣款，不可为 0        |


**请求示例**

```json
{
  "user_id": 1,
  "asset": "USDT",
  "business": "deposit",
  "business_id": 1001,
  "change": "10000"
}
```

**响应 `data`**：更新后的余额对象（结构同 §3.6.2）。

**语义**

- `200`：成功（含幂等命中，返回当前余额）。
- 余额不足（扣款）：`422` / `code: 42201`。

**curl**

```bash
curl -s -X POST "http://localhost:8080/v1/balances" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":1,"asset":"USDT","business":"deposit","business_id":1001,"change":"10000"}'
```

---

### 3.7 查询成交记录

```http
GET /v1/trades
```

**查询参数**


| 参数           | 类型              | 必填  | 说明           |
| ------------ | --------------- | --- | ------------ |
| `symbol`     | string          | 否   | 交易对          |
| `order_id`   | string (uint64) | 否   | 指定订单的成交      |
| `start_time` | string          | 否   | 成交时间下限       |
| `end_time`   | string          | 否   | 成交时间上限       |
| `limit`      | integer         | 否   | 默认 50，最大 200 |
| `cursor`     | string          | 否   | 翻页游标         |


**响应 `data.items`（Trade 对象）**


| 字段          | 类型              | 说明       |
| ----------- | --------------- | -------- |
| `trade_id`  | string          | 成交 ID    |
| `order_id`  | string (uint64) | 关联订单     |
| `symbol`    | string          | 交易对      |
| `side`      | string          | 本订单方向    |
| `price`     | string          | 成交价      |
| `quantity`  | string          | 成交量      |
| `fee`       | string          | 手续费      |
| `fee_asset` | string          | 手续费币种    |
| `is_maker`  | boolean         | 是否 Maker |
| `traded_at` | string          | 成交时间     |


---

## 4. 行情查询（Market Data Service）

公开读接口；Gateway 转发至 Market Data gRPC。

### 4.1 深度（Order Book）

```http
GET /v1/market/depth
```

**查询参数**


| 参数       | 类型      | 必填  | 说明                 |
| -------- | ------- | --- | ------------------ |
| `symbol` | string  | 是   | 交易对                |
| `limit`  | integer | 否   | 每边档位数，默认 20，最大 100 |


**响应 `data`**

```json
{
  "symbol": "BTC-USDT",
  "last_update_id": 10293485721,
  "bids": [["65000.00", "1.5"], ["64999.50", "0.8"]],
  "asks": [["65001.00", "2.1"], ["65002.00", "0.3"]],
  "timestamp": "2026-05-20T08:00:00.000Z"
}
```

`bids` / `asks`：`[价格, 数量]` 字符串数组，价格降序（买）、升序（卖）。

---

### 4.2 24 小时 Ticker（单交易对 / 批量）

```http
GET /v1/market/ticker
```

**查询参数**（`symbol` 与 `symbols` 二选一）


| 参数        | 类型     | 必填  | 说明                                      |
| --------- | ------ | --- | --------------------------------------- |
| `symbol`  | string | 否   | 单个交易对                                   |
| `symbols` | string | 否   | 逗号分隔，最多 **100** 个，如 `BTC-USDT,ETH-USDT` |
| `fields`  | string | 否   | 逗号分隔字段名，减小响应体积                          |


**响应 `data`**

- 单 symbol：Ticker 对象（见下表）
- 多 symbols：`{ "items": [ Ticker, ... ] }`


| 字段                     | 说明             |
| ---------------------- | -------------- |
| `symbol`               | 交易对            |
| `last_price`           | 最新价            |
| `open_price`           | 24h 开盘价        |
| `high_price`           | 24h 最高价        |
| `low_price`            | 24h 最低价        |
| `volume`               | 24h 成交量（base）  |
| `quote_volume`         | 24h 成交额（quote） |
| `price_change_percent` | 24h 涨跌幅        |
| `timestamp`            | 统计时间           |


> 普通客户端请优先使用本接口或 WS `ticker:{symbol}`。全市场 Ticker 见 §4.2.1、§8.2（做市商）。

---

### 4.2.1 全市场 Ticker 快照（做市商 / `ticker@all` 冷启动）

供做市商、量化系统**冷启动、断线重连**拉取全量快照；实时更新请用 WebSocket `ticker@all`（§8.2），避免高频轮询本接口。

```http
GET /v1/market/ticker/all
```

**鉴权**：建议要求 API Key（做市商档位）；公开只读部署可放开，但须单独限流。

**查询参数**


| 参数            | 类型     | 必填  | 说明                              |
| ------------- | ------ | --- | ------------------------------- |
| `quote_asset` | string | 否   | 过滤计价币，如 `USDT`（只返回 `*-USDT`）    |
| `status`      | string | 否   | 默认 `TRADING`                    |
| `snapshot_id` | string | 否   | 若与服务端当前一致，返回 `304 Not Modified` |
| `format`      | string | 否   | `json`（默认）| `protobuf`（做市商推荐）   |


**响应头**


| 头                  | 说明               |
| ------------------ | ---------------- |
| `X-Snapshot-Id`    | 快照版本号（单调递增或内容哈希） |
| `X-Snapshot-Time`  | 快照生成时间（ISO 8601） |
| `Content-Encoding` | 建议 `gzip` / `br` |


**响应 `data`**

```json
{
  "snapshot_id": "snap-20260520-080000-abc123",
  "snapshot_time": "2026-05-20T08:00:00.000Z",
  "count": 1284,
  "items": [
    {
      "symbol": "BTC-USDT",
      "last_price": "65001.00",
      "volume": "1234.56",
      "quote_volume": "80123456.78",
      "price_change_percent": "2.35",
      "timestamp": "2026-05-20T08:00:00.000Z"
    }
  ]
}
```

**性能约定（服务端实现）**

- Market Data Service 每 **100～500ms** 在内存聚合全市场 Ticker，序列化写入 Redis：`ticker:all:{quote_asset}`（JSON 或 protobuf 二进制）。
- Gateway **只读 Redis**，不每次全量 gRPC 重算；响应开启 gzip。
- 客户端带 `If-None-Match: <snapshot_id>` 时，未变化返回 **304**，body 为空。
- 上千 symbol 全量 JSON gzip 后通常 **200KB～1MB**；做市商客户端应使用 `format=protobuf` 或 WS 二进制帧（§8.2）。

**curl**

```bash
curl -s "https://api.example.com/v1/market/ticker/all?quote_asset=USDT" \
  -H "X-API-KEY: $KEY" \
  -H "Accept-Encoding: gzip" \
  -H "If-None-Match: snap-20260520-080000-abc123"
```

---

### 4.3 交易对信息

```http
GET /v1/market/symbols
```

**响应 `data.items`**


| 字段                   | 说明                     |
| -------------------- | ---------------------- |
| `symbol`             | 交易对                    |
| `base_asset`         | 基础币种                   |
| `quote_asset`        | 计价币种                   |
| `price_precision`    | 价格小数位                  |
| `quantity_precision` | 数量小数位                  |
| `min_quantity`       | 最小下单量                  |
| `min_notional`       | 最小名义价值                 |
| `status`             | `TRADING` | `HALT`（停牌） |


---

## 5. K 线（Kline Service）

```http
GET /v1/klines
```

**查询参数**


| 参数           | 类型      | 必填  | 说明                             |
| ------------ | ------- | --- | ------------------------------ |
| `symbol`     | string  | 是   | 交易对                            |
| `interval`   | string  | 是   | `1m` `5m` `15m` `1h` `4h` `1d` |
| `start_time` | string  | 否   | ISO 8601                       |
| `end_time`   | string  | 否   | ISO 8601                       |
| `limit`      | integer | 否   | 默认 500，最大 1500                 |


**响应 `data.items`**


| 字段           | 说明      |
| ------------ | ------- |
| `open_time`  | K 线开盘时间 |
| `open`       | 开盘价     |
| `high`       | 最高价     |
| `low`        | 最低价     |
| `close`      | 收盘价     |
| `volume`     | 成交量     |
| `close_time` | K 线收盘时间 |
| `is_closed`  | 是否已闭合   |


---

## 6. 指数价格（Index Price Service）

```http
GET /v1/index-price
```

**查询参数**


| 参数       | 类型     | 必填  | 说明       |
| -------- | ------ | --- | -------- |
| `symbol` | string | 是   | 交易对或指数符号 |


**响应 `data`**


| 字段          | 说明         |
| ----------- | ---------- |
| `symbol`    | 符号         |
| `price`     | 指数价格       |
| `sources`   | 参与聚合的数据源数量 |
| `timestamp` | 更新时间       |


---

## 7. 系统接口

### 7.1 健康检查

```http
GET /v1/health
```

无需认证。`200` 且 `data.status` 为 `ok` 表示 Gateway 存活（不保证下游全部健康）。

### 7.2 服务时间

```http
GET /v1/time
```

**响应 `data`**

```json
{
  "server_time": "2026-05-20T08:00:00.000Z",
  "unix_ms": 1716192000123
}
```

用于客户端时钟校准与签名时间窗校验。

---

## 8. WebSocket

REST 负责命令；**实时数据**走 WebSocket，由 Gateway / Push Service 暴露（Phase 2+）。


| 项               | 值                                                 |
| --------------- | ------------------------------------------------- |
| URL（零售）         | `wss://api.example.com/v1/ws`                     |
| URL（做市商，可选独立入口） | `wss://stream-mm.example.com/v1/ws`               |
| 认证              | 连接后 `{"op":"auth","args":["<token_or_api_key>"]}` |
| 心跳              | 客户端每 **30s** `{"op":"ping"}`；服务端 `{"op":"pong"}`  |


### 8.1 通用帧格式

```json
{
  "stream": "ticker@all",
  "type": "snapshot",
  "snapshot_id": "snap-...",
  "ts": 1716192000123,
  "data": { }
}
```


| `type`      | 说明                 |
| ----------- | ------------------ |
| `snapshot`  | 全量快照（订阅后首条，或断线重连）  |
| `delta`     | 增量更新（仅包含变化 symbol） |
| `heartbeat` | 保活，无业务数据           |


### 8.2 全市场 Ticker：`ticker@all`（做市商）

**用途**：做市商、跨市场套利、全局风控需要**同一连接**内收到所有交易对 Ticker，延迟低于轮询 REST。

**订阅**

```json
{"op": "subscribe", "args": ["ticker@all"]}
```

或按计价币缩小范围（推荐，减少带宽）：

```json
{"op": "subscribe", "args": ["ticker@all:USDT"]}
```


| 频道                         | 说明                        |
| -------------------------- | ------------------------- |
| `ticker@all`               | 全部 `TRADING` 交易对          |
| `ticker@all:{quote_asset}` | 仅该计价币，如 `ticker@all:USDT` |


**鉴权与限流**


| 档位          | 要求                                                    |
| ----------- | ----------------------------------------------------- |
| 做市商 API Key | 可订阅 `ticker@all`；单账户 **1～3** 条并发连接                    |
| 普通用户        | 禁止 `ticker@all`；仅 `ticker:{symbol}`，每连接最多 50 个 symbol |


**推送流程（必须实现）**

```
1. subscribe 确认 → {"op":"subscribed","args":["ticker@all:USDT"]}
2. 立即推送 snapshot（与 REST /v1/market/ticker/all 同结构，可 gzip 二进制帧）
3. 之后每 100ms（可配置）合并推送 delta，仅含 price/volume 等变化字段
4. 每 60s 可选推送轻量 heartbeat（含 snapshot_id，便于检测丢包）
```

**Snapshot 示例**（`type=snapshot`）

```json
{
  "stream": "ticker@all:USDT",
  "type": "snapshot",
  "snapshot_id": "snap-20260520-080000-abc123",
  "ts": 1716192000123,
  "data": {
    "count": 1284,
    "items": [
      {
        "s": "BTC-USDT",
        "p": "65001.00",
        "v": "1234.56",
        "q": "80123456.78",
        "c": "2.35"
      }
    ]
  }
}
```

增量字段使用短 key（`s` symbol、`p` last_price 等）以减小帧体积。完整 key 定义见下表。


| 短 key | 全名                   | 说明      |
| ----- | -------------------- | ------- |
| `s`   | symbol               | 交易对     |
| `p`   | last_price           | 最新价     |
| `v`   | volume               | 24h 成交量 |
| `q`   | quote_volume         | 24h 成交额 |
| `c`   | price_change_percent | 24h 涨跌幅 |


**Delta 示例**（`type=delta`）

```json
{
  "stream": "ticker@all:USDT",
  "type": "delta",
  "snapshot_id": "snap-20260520-080000-abc123",
  "ts": 1716192000124,
  "data": {
    "items": [
      { "s": "BTC-USDT", "p": "65002.00" },
      { "s": "ETH-USDT", "v": "5678.90", "q": "12345678.00" }
    ]
  }
}
```

**二进制模式（做市商推荐）**

订阅时指定编码：

```json
{"op": "subscribe", "args": ["ticker@all:USDT"], "encode": "protobuf"}
```

- 帧体为 `TickerAllDelta` protobuf，外层仍可为 JSON 控制消息。
- 相较 JSON 全量 snapshot，体积通常再降 **30%～50%**。

**断线重连**

1. 记录最后收到的 `snapshot_id` 与 `ts`。
2. 重连并重新 `subscribe`。
3. 若服务端 `snapshot_id` 未变，可只收 delta；若已变或 gap 超过 **5s**，先调 `GET /v1/market/ticker/all?snapshot_id=...` 或等待 WS 新 snapshot。
4. 不要通过高频 REST 轮询代替 WS。

**服务端数据路径**

```
trade.events / match.events
        → Market Data Service（内存 Ticker 表）
        → 每 100ms  diff 上一帧 → Redis Pub/Sub channel: ticker@all:USDT
        → Push Service 合并广播 → Gateway WS
```

同一快照由 Market Data 写入 Redis `ticker:all:USDT`，REST 与 WS snapshot **同源**，保证 `snapshot_id` 一致。

### 8.3 其它频道

**订阅示例**

```json
{"op": "subscribe", "args": ["depth:BTC-USDT", "ticker:BTC-USDT", "order"]}
```


| 频道                                  | 说明                   |
| ----------------------------------- | -------------------- |
| `depth:{symbol}`                    | 深度增量/快照              |
| `ticker:{symbol}`                   | 单交易对 24h Ticker      |
| `ticker@all` / `ticker@all:{quote}` | 全市场 Ticker（§8.2，做市商） |
| `trade:{symbol}`                    | 公开市场成交               |
| `kline:{symbol}:{interval}`         | K 线更新                |
| `order`                             | 当前用户订单状态变更（需 auth）   |


---

## 9. 接口一览


| 方法       | 路径                                  | 说明                 | 上游服务                |
| -------- | ----------------------------------- | ------------------ | ------------------- |
| `POST`   | `/v1/orders`                        | 下单                 | Order               |
| `DELETE` | `/v1/orders/{order_id}`             | 撤单                 | Order               |
| `GET`    | `/v1/orders/{order_id}`             | 单个订单               | Order               |
| `GET`    | `/v1/orders`                        | 订单列表               | Order               |
| `GET`    | `/v1/balances`                      | 全部资产余额             | Order               |
| `GET`    | `/v1/balances/{asset}`              | 单资产余额              | Order               |
| `POST`   | `/v1/balances`                      | 调账/充值（联调）          | Order               |
| `GET`    | `/v1/trades`                        | 成交列表（Gateway 未实现）  | Order               |
| `GET`    | `/v1/market/depth`                  | 深度                 | Market Data         |
| `GET`    | `/v1/market/ticker`                 | Ticker（单/批量）       | Market Data         |
| `GET`    | `/v1/market/ticker/all`             | 全市场 Ticker 快照（做市商） | Market Data / Redis |
| `GET`    | `/v1/market/symbols`                | 交易对元数据             | Market Data / 配置    |
| `WS`     | `ticker@all` / `ticker@all:{quote}` | 全市场 Ticker 推送（做市商） | Push                |
| `GET`    | `/v1/klines`                        | K 线                | Kline               |
| `GET`    | `/v1/index-price`                   | 指数价格               | Index Price         |
| `GET`    | `/v1/health`                        | 健康检查               | Gateway             |
| `GET`    | `/v1/time`                          | 服务器时间              | Gateway             |
| `WS`     | `/v1/ws`                            | 实时推送               | Push                |


---

## 10. 客户端集成建议

1. **下单前**：`POST /v1/balances` 充值或确认 `GET /v1/balances/{asset}` 可用余额充足。
2. **下单后**：轮询 `GET /v1/orders/{id}` 或订阅 WS `order`，直到 `status` 为终态。
3. **幂等**：始终传唯一 `client_order_id`；网络超时可安全重试。
4. **撤单**：收到 `CANCELING` 后继续等待 `CANCELED`；`CANCELING` 期间仍可能少量成交（见架构 R5）。
5. **行情**：展示用 REST 拉快照；实时用 WS，避免高频轮询深度。
6. **做市商全市场**：冷启动 `GET /v1/market/ticker/all` → 订阅 `ticker@all:USDT` 收 delta；禁止秒级轮询 REST。
7. **时钟**：程序化签名前调用 `GET /v1/time` 校准偏差。

---

## 11. 修订记录


| 版本  | 日期         | 说明                                                                         |
| --- | ---------- | -------------------------------------------------------------------------- |
| 1.0 | 2026-05-20 | 初稿，对齐 architecture-spec v1.0                                               |
| 1.1 | 2026-05-20 | 新增做市商 `ticker@all`（REST 快照 + WS snapshot/delta）                            |
| 1.2 | 2026-05-20 | `order_id` 改为 uint64（JSON 十进制字符串）；`client_order_id` 保持 string              |
| 1.3 | 2026-05-26 | 新增 §3.6 账户余额 REST（`GET/POST /v1/balances`），Gateway 已实现                     |
| 1.4 | 2026-05-26 | §1.1～§1.2 明确内网 Gateway + 公网 Web/BFF 分层；§3.6.3 调账调用方说明                      |
| 1.5 | 2026-05-26 | §2.1～§2.2 请求指定 `user_id`（body / `X-User-Id` / query）；移除配置 `static_user_id` |


