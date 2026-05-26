# API Gateway 开发计划（第 5 步）

**版本**: 1.0  
**日期**: 2026-05-25  
**状态**: 进行中  
**关联**: [development-roadmap.md §第 5 步](./development-roadmap.md#第-5-步api-gateway约-1-周) · [rest-api.md](./rest-api.md) · [order-api.md](./order-api.md) · [architecture-spec.md](./architecture-spec.md)

本文档记录第 5 步 **API Gateway** 的详细开发步骤、范围界定、与 Order Service 的差异点及验收清单。实现时以 [rest-api.md](./rest-api.md) 为对外契约，以 [order-api.md](./order-api.md) 为上游 gRPC 参考。

---

## 1. 范围界定

### 1.1 本步必做

| 项 | 说明 |
|----|------|
| 订单 REST | `POST` / `DELETE` / `GET /v1/orders`、`GET /v1/orders/{order_id}` |
| 系统 REST | `GET /v1/health`、`GET /v1/time`（Gateway 本地，不调下游） |
| 鉴权 | Phase 1：`Authorization: Bearer` + 配置 `static_token`；`user_id` 由请求传入 |
| 横切 | `X-Request-Id`、统一 JSON 信封、gRPC 错误 → HTTP/code |
| 转换 | JSON ↔ `order.v1.OrderService` gRPC；`order_id` 十进制字符串 |

### 1.2 本步不做（后续步骤）

| 项 | 归属 |
|----|------|
| WebSocket `/v1/ws` | 第 6 步 |
| 行情 `/v1/market/*`、K 线、指数价 | 第 6 步 |
| API Key + HMAC 签名 | Phase 4（可先 stub） |
| 生产级限流 | 可先 stub；默认配额见 rest-api §2.5 |
| `GET /v1/trades` | Order 尚无 `ListTrades` RPC，见 §1.3 |

### 1.3 与下游 / 文档的差异（实现前确认）

| 差异点 | REST 文档 | 当前 Order gRPC | Phase 1 建议 |
|--------|-----------|-----------------|--------------|
| 分页 | `cursor` + `has_more` | `page` + `page_size`（最大 100） | 用 `limit` 映射 `page_size`，`page=1`；`cursor`/`has_more` 可简化或后续补 |
| 订单字段 | `avg_price`、`time_in_force`、`version` | `OrderInfo` 未包含 | 响应缺字段用 `""` 或省略，文档注明限制 |
| `time_in_force` | 请求体可选 | 未实现 | 接受 JSON 但忽略，或返回 400（二选一并在 gateway-api 说明） |
| 幂等 HTTP 状态 | 新单 `201`，幂等命中 `200` | `PlaceOrderResponse.idempotent_hit` | 按 `idempotent_hit` 区分状态码 |
| `order_id` | JSON/路径为十进制 **string** | proto `uint64` | Gateway 内 `strconv.ParseUint`，禁止 `float64` 解析 |

充值/调账仍用 **grpcurl → `BalanceService`**（Gateway 暂不封装）。

---

## 2. 目录与依赖

### 2.1 目标目录（仓库尚无 gateway，需新建）

| 路径 | 职责 |
|------|------|
| `cmd/gateway/main.go` | HTTP 服务入口、优雅退出 |
| `internal/gateway/config/` | JSON 配置加载 |
| `internal/gateway/client/` | Order Service gRPC 客户端 |
| `internal/gateway/middleware/` | RequestID、Auth、Recover、访问日志 |
| `internal/gateway/handler/` | 路由与 HTTP handler |
| `internal/gateway/convert/` | JSON ↔ proto、枚举与时间格式 |
| `internal/gateway/response/` | 统一成功/失败信封 |
| `configs/gateway.json` | 默认配置 |

### 2.2 配置项建议（`configs/gateway.json`）

| 字段 | 说明 | 默认 |
|------|------|------|
| `http_listen` | HTTP 监听地址 | `:8080` |
| `order_grpc_addr` | Order Service gRPC | `localhost:50051` |
| `auth.static_token` | Phase 1 Bearer token | 联调用固定值 |
| `log.*` | 日志 | 与 `configs/order.json` 风格一致 |

### 2.3 构建

- `Makefile` 增加 `build-gateway`，产出 `bin/gateway`
- HTTP 路由：标准库 `net/http` 或 `chi`（团队选定其一；当前 `go.mod` 无 chi/gin）

---

## 3. 开发步骤

### 阶段 0：环境与骨架（约 0.5 天）

- [ ] **0.1** 确认第 4 步联调基线：PG + Kafka + `bin/order` + `bin/matching`，grpcurl 可下单且 `GetOrder` 状态会变（见 [order-api.md](./order-api.md)）
- [ ] **0.2** 新建 §2.1 目录结构
- [ ] **0.3** 选定路由库（`chi`）
- [ ] **0.4** `Makefile` 增加 `build-gateway`

### 阶段 1：进程与横切能力（约 1 天）

- [x] **1.1** `internal/gateway/config`：加载 `configs/gateway.json`
- [x] **1.2** `cmd/gateway/main.go`：logger → gRPC 连接 → HTTP Server → `SIGINT`/`SIGTERM` 优雅退出
- [x] **1.3** `internal/gateway/client/order.go`：`OrderServiceClient`，连接超时
- [x] **1.4** 中间件链（顺序固定）：
  - `RequestID`：读/生成 `X-Request-Id`，写入 context 与响应头
  - `Auth`：`Bearer` 校验；失败 → `401` / `code: 40100`
  - `Recover` + 访问日志（method、path、status、latency、request_id）
- [x] **1.5** `internal/gateway/response`：统一信封 `code` / `message` / `request_id` / `data`
- [x] **1.6** 系统接口（不依赖 Order）：
  - `GET /v1/health` → `200`，`data.status=ok`
  - `GET /v1/time` → `server_time` + `unix_ms`

### 阶段 2：错误映射与类型转换（约 1 天）

- [x] **2.1** `grpc/status` → REST（[rest-api.md §2.4](./rest-api.md#24-错误码)）：

| gRPC code | HTTP | code |
|-----------|------|------|
| `InvalidArgument` | 400 | 40000 |
| `Unauthenticated` | 401 | 40100 |
| `NotFound` | 404 | 40400 |
| `FailedPrecondition` | 422 | 42201（余额不足等；撤单不可撤可细分 42202） |
| `Unavailable` | 503 | 50300 |
| 其他 | 500 | 50000 |

- [x] **2.2** `internal/gateway/convert`（优先单测）：
  - `order_id`：`uint64` ↔ 十进制 string
  - `side`：`BUY`/`SELL` ↔ `common.v1.Side`
  - `type`：`LIMIT`/`MARKET` ↔ `common.v1.OrderType`
  - `price` / `quantity`：string ↔ `common.v1.Decimal`
  - 时间：`timestamppb` ↔ ISO 8601 UTC（`2006-01-02T15:04:05.000Z`）

### 阶段 3：订单 Handler（约 2 天）

- [x] **3.1** `POST /v1/orders`
  - 解析 JSON → 校验必填 → `PlaceOrder`（`user_id` 来自 Auth）
  - `idempotent_hit=true` → **HTTP 200**；否则 **201**
  - 映射 `PlaceOrderResponse` → JSON（`order_id` 为字符串）
- [x] **3.2** `DELETE /v1/orders/{order_id}`
  - 路径参数解析 `order_id`；可选 `?symbol=` 仅日志/校验
  - 映射 `CancelOrderResponse`（`status` 一般为 `CANCELING`）
- [x] **3.3** `GET /v1/orders/{order_id}`
  - `GetOrder` → 完整 Order JSON（缺字段按 §1.3 处理）
- [x] **3.4** `GET /v1/orders`
  - Query：`symbol`、`status`（逗号分隔）、`side`、`start_time`/`end_time`、`limit`
  - 映射 `ListOrders`（`page=1`，`page_size=min(limit,100)`）
  - 响应 `data.items`；`cursor`/`has_more` Phase 1 可简化
- [x] **3.5** 路由注册：注意 `/v1/orders` 与 `/v1/orders/{order_id}` 匹配顺序

### 阶段 4：测试与文档（约 1～1.5 天）

- [ ] **4.1** 单元测试：`convert`、`mapGRPCError`、handler（`httptest` + mock gRPC client）
- [ ] **4.2** 端到端联调（见 §4）
- [ ] **4.3** 更新 [development-roadmap.md](./development-roadmap.md) 第 5 步任务勾选；可选补充 `docs/gateway-api.md` curl 示例

### 阶段 5：可选增强（时间有余）

- [ ] **5.1** `GET /v1/trades`：需 Order 增加 `ListTrades` RPC，不阻塞主路径
- [ ] **5.2** 按 `user_id` 的简单内存限流 → `429` / `42900`
- [ ] **5.3** 启动时探测 Order gRPC 可达性

---

## 4. 实施顺序

```mermaid
flowchart LR
  A[配置+main+gRPC客户端] --> B[中间件+统一响应]
  B --> C[health/time]
  C --> D[convert+错误映射单测]
  D --> E[POST orders]
  E --> F[GET/DELETE orders]
  F --> G[联调验收+文档]
```

---

## 5. 联调与验收

### 5.1 启动顺序

```bash
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh
./scripts/matching.sh start --build
./bin/order -config configs/order.json
./bin/gateway -config configs/gateway.json   # 实现后
```

### 5.2 验收命令

```bash
# 健康
curl -s http://localhost:8080/v1/health

# 时间
curl -s http://localhost:8080/v1/time

# 充值（仍用 grpcurl，见 order-api.md §3.5）
export PROTO_ARGS="-import-path proto -proto proto/common/v1/types.proto -proto proto/order/v1/balance.proto"
grpcurl -plaintext $PROTO_ARGS -d '{"user_id":1,"asset":"USDT","business":"deposit","business_id":1001,"change":{"value":"10000"}}' \
  localhost:50051 order.v1.BalanceService/UpdateBalance

# 下单（TOKEN 与 configs/gateway.json 中 auth.static_token 一致）
curl -s -X POST http://localhost:8080/v1/orders \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"client_order_id":"gw-001","symbol":"BTC-USDT","side":"BUY","type":"LIMIT","price":"100","quantity":"1"}'

# 查单
curl -s http://localhost:8080/v1/orders/<order_id> -H "Authorization: Bearer <TOKEN>"

# 幂等：相同 client_order_id 再 POST → HTTP 200

# 撤单
curl -s -X DELETE "http://localhost:8080/v1/orders/<order_id>?symbol=BTC-USDT" \
  -H "Authorization: Bearer <TOKEN>"
```

### 5.3 完成定义（DoD）

- [ ] `make build`（或 `make build-gateway`）产出 `bin/gateway`
- [ ] 四个订单 REST 接口可用，**仅**经 gRPC 调 Order，不直连 Kafka/撮合
- [ ] `order_id` 全程十进制字符串，无 JSON 浮点精度问题
- [ ] 错误体符合 [rest-api.md §2.3～§2.4](./rest-api.md#23-统一响应结构)
- [ ] 端到端：REST 下单 → Matching 成交 → `GET /v1/orders/{id}` 可见 `PARTIAL` / `FILLED`
- [ ] `go test ./internal/gateway/...` 通过

---

## 6. 待拍板项

| # | 问题 | 建议 |
|---|------|------|
| 1 | 路由库：`net/http` 还是 `chi`？ | 无偏好时用 `chi`，路由更清晰 |
| 2 | 第 5 步是否包含 `GET /v1/orders` 列表？ | **建议做**，与 rest-api 一致，实现成本低 |
| 3 | `time_in_force` 请求字段 | Phase 1 **忽略** 并在对外文档注明 |

---

## 7. 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2026-05-25 | 初稿：第 5 步详细开发步骤与验收 |
