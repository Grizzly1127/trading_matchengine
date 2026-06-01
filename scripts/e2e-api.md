# API 联调命令手册（E2E）

可执行脚本：[e2e-api.sh](./e2e-api.sh)

```bash
# 启动全部服务后（static，默认）
./scripts/dev.sh start --build
./scripts/e2e-api.sh          # 全流程：需 jq，对 health/充值/撮合/订单/成交/行情做断言

# JWT + scope 联调（Gateway/Push 走 jwt 配置）
./scripts/dev.sh start --build --auth --jwt
./scripts/e2e-api.sh jwt      # 向 :8090 换 token 后跑与 static 相同断言

# 仅验证能换到 JWT
./scripts/e2e-api.sh jwt step jwt-auth

# 分步（无 jq 时可用 SKIP_ASSERT=1）
./scripts/e2e-api.sh step deposit
./scripts/e2e-api.sh step orders
./scripts/e2e-api.sh step query   # 含 GET /v1/trades
```

若出现 `orderbook BTC-USDT not found`：多为行情尚未消费到 `ORDER_ACCEPTED`（固定 `sleep` 不够）。脚本已改为轮询；可调大 `PIPELINE_WAIT_SEC=60`，并确认 `matching`、`marketdata`、Kafka 正常。

统一变量（与脚本一致）：

```bash
export BASE_URL="${BASE_URL:-http://localhost:8080}"
export TOKEN="${TOKEN:-dev-token-change-me}"
export SYMBOL="${SYMBOL:-BTC-USDT}"
```

经 Nginx 统一入口时：

```bash
export BASE_URL="http://localhost"   # 80 → gateway + push
```

---

## 1. 健康检查

```bash
curl -s "${BASE_URL}/v1/health" | jq .
curl -s "${BASE_URL}/v1/time" | jq .
```

---

## 2. 用户资产充值（内网调账）

买方充值 USDT：

```bash
curl -s -X POST "${BASE_URL}/v1/balances" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "asset": "USDT",
    "business": "deposit",
    "business_id": 10001,
    "change": "100000"
  }' | jq .
```

卖方充值 USDT + BTC（用于卖单）：

```bash
curl -s -X POST "${BASE_URL}/v1/balances" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 2,
    "asset": "USDT",
    "business": "deposit",
    "business_id": 10002,
    "change": "100000"
  }' | jq .

curl -s -X POST "${BASE_URL}/v1/balances" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 2,
    "asset": "BTC",
    "business": "deposit",
    "business_id": 10003,
    "change": "10"
  }' | jq .
```

---

## 3. 资产查询

```bash
# 全部资产
curl -s "${BASE_URL}/v1/balances?user_id=1" \
  -H "Authorization: Bearer ${TOKEN}" | jq .

# 单资产
curl -s "${BASE_URL}/v1/balances/USDT?user_id=1" \
  -H "Authorization: Bearer ${TOKEN}" | jq .

curl -s "${BASE_URL}/v1/balances/BTC?user_id=2" \
  -H "Authorization: Bearer ${TOKEN}" | jq .
```

---

## 4. 限价下单与撮合

**步骤**：用户 2 先挂卖单 → 用户 1 同价买单 → 预期成交。

```bash
# 4.1 限价卖（挂入订单簿）
curl -s -X POST "${BASE_URL}/v1/orders" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 2,
    "client_order_id": "demo-sell-limit-001",
    "symbol": "BTC-USDT",
    "side": "SELL",
    "type": "LIMIT",
    "price": "65000",
    "quantity": "0.01",
    "time_in_force": "GTC"
  }' | jq .

sleep 2

# 4.2 限价买（与卖单撮合）
curl -s -X POST "${BASE_URL}/v1/orders" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "client_order_id": "demo-buy-limit-001",
    "symbol": "BTC-USDT",
    "side": "BUY",
    "type": "LIMIT",
    "price": "65000",
    "quantity": "0.01",
    "time_in_force": "GTC"
  }' | jq .

sleep 2
```

---

## 5. 市价下单

市价买需盘口有卖单；可先重复一笔限价卖，再市价买。

```bash
# 5.1 提供流动性：限价卖
curl -s -X POST "${BASE_URL}/v1/orders" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 2,
    "client_order_id": "demo-sell-for-market-001",
    "symbol": "BTC-USDT",
    "side": "SELL",
    "type": "LIMIT",
    "price": "65000",
    "quantity": "0.01",
    "time_in_force": "GTC"
  }' | jq .

sleep 2

# 5.2 市价买（无需 price；time_in_force 建议 IOC）
curl -s -X POST "${BASE_URL}/v1/orders" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "client_order_id": "demo-buy-market-001",
    "symbol": "BTC-USDT",
    "side": "BUY",
    "type": "MARKET",
    "quantity": "0.001",
    "time_in_force": "IOC"
  }' | jq .
```

市价卖示例：

```bash
curl -s -X POST "${BASE_URL}/v1/orders" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 2,
    "client_order_id": "demo-sell-market-001",
    "symbol": "BTC-USDT",
    "side": "SELL",
    "type": "MARKET",
    "quantity": "0.001",
    "time_in_force": "IOC"
  }' | jq .
```

---

## 6. 深度查询

```bash
curl -s "${BASE_URL}/v1/market/depth?symbol=BTC-USDT&limit=20" | jq .
```

---

## 7. Ticker

```bash
curl -s "${BASE_URL}/v1/market/ticker?symbol=BTC-USDT" | jq .

# 多交易对
curl -s "${BASE_URL}/v1/market/ticker?symbols=BTC-USDT,ETH-USDT" | jq .
```

---

## 8. K 线查询

```bash
curl -s "${BASE_URL}/v1/klines?symbol=BTC-USDT&interval=1m&limit=10" \
  -H "Authorization: Bearer ${TOKEN}" | jq .

curl -s "${BASE_URL}/v1/klines?symbol=BTC-USDT&interval=1s&limit=5" \
  -H "Authorization: Bearer ${TOKEN}" | jq .
```

---

## 9. 订单查询 / 撤单

```bash
# 列表
curl -s "${BASE_URL}/v1/orders?user_id=1&symbol=BTC-USDT&limit=20" \
  -H "Authorization: Bearer ${TOKEN}" | jq .

# 单笔（替换 ORDER_ID）
ORDER_ID=1000000001
curl -s "${BASE_URL}/v1/orders/${ORDER_ID}?user_id=1&symbol=BTC-USDT" \
  -H "Authorization: Bearer ${TOKEN}" | jq .

# 撤单
curl -s -X DELETE "${BASE_URL}/v1/orders/${ORDER_ID}?user_id=1&symbol=BTC-USDT" \
  -H "Authorization: Bearer ${TOKEN}" | jq .
```

---

## 10. WebSocket（Push 服务）

REST 走 Gateway（8080 或 Nginx 80）；WS 走 Push（8081 或 Nginx `/v1/ws`）。

```bash
# 直连 Push
wscat -c "ws://localhost:8081/v1/ws"

# 经 Nginx
wscat -c "ws://localhost/v1/ws"
```

连接后：

```json
{"op":"auth","args":["dev-token-change-me"]}
{"op":"subscribe","args":["depth:BTC-USDT","ticker:BTC-USDT","kline:BTC-USDT:1m"]}
```

---

## 11. 常见问题

| 现象 | 排查 |
|------|------|
| `401 unauthorized` | `TOKEN` 与 `configs/gateway.json` 中 `auth.static_token` 一致 |
| `42201` 余额不足 | 先执行充值；卖单需 BTC，买单需 USDT |
| 限价买不成交 | 卖单先入簿；价格需可交叉；等待 1～2s 撮合与 outbox |
| 市价买失败 | 需 Market Data 可用（参考价）；盘口需有对手盘 |
| K 线为空 | 需有成交且 `kline` 服务在跑；新部署仅 forward 新 trade |
| `connection refused` | `./scripts/dev.sh status` 检查各进程 |

---

## 12. 服务依赖关系（联调顺序）

```text
migrate（可选）→ matching → order → marketdata → kline → push → gateway
                     ↑___________________成交→行情/K线________________|
```

一键启动：`./scripts/dev.sh start --build`
