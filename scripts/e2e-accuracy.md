# 撮合与资产准确性 E2E

脚本：`scripts/e2e-accuracy.sh`

## 目的

在**干净环境**下验证：

- 限价卖先入簿、限价买同价成交后，订单 `status` / `filled_quantity` / `avg_price` 正确
- 买卖双方 `GET /v1/trades?order_id=` 成交价、量、方向一致
- 充值基数固定时，成交后 `balance` 满足 `USDT/BTC` 守恒（`price × quantity`）
- 成交后 `frozen` 为 0；深度上该卖价档位无残留

与 `e2e-api.sh` 不同：默认 **停服 → `reset-dev.sh` → `dev.sh start --build`**，且默认卖价 **70000**（避免 L2 残留 bid @65000 导致卖单立即成交、深度无 asks）。

## 前置

- `jq`、`python3`、`curl`
- Docker：`deploy/docker-compose.yml` 中 Postgres / Redis / Kafka（`reset-dev.sh` 会拉起）
- 本机可编译 Go 服务（`dev.sh start --build`）

## 用法

```bash
# 全流程（推荐）
./scripts/e2e-accuracy.sh

# 环境已 reset 且服务已起，只跑断言
./scripts/e2e-accuracy.sh --test-only
```

## 环境变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `LIMIT_PRICE` | `75000 + (RUN_ID/10)%10000` | 卖/买限价；可显式覆盖。推荐全量 `./scripts/e2e-accuracy.sh` |
| `LIMIT_QTY` | `0.01` | 成交量 |
| `PIPELINE_WAIT_SEC` | `30` | Outbox→撮合→Order/行情 轮询超时 |
| `START_WAIT_SEC` | `120` | reset 后首次启服等待 Gateway |
| `SKIP_ASSERT` | — | 设为 `1` 仅打印响应 |

其余与 `e2e-api.sh` 相同：`BASE_URL`、`TOKEN`、`USER_BUYER`、`USER_SELLER`、`SYMBOL`。

## 余额模型（无手续费）

脚本在**本 run 充值后**读取余额快照，再断言成交相对快照的变化：

- 买方 USDT：`- price×qty`，BTC：`+ qty`
- 卖方 USDT：`+ price×qty`，BTC：`- qty`

干净环境（reset 后仅本 run 充值）时，快照即为 `100000/1` 与 `100000/10`。

联调 REST 成交 `fee` 固定为 `"0"`（见 Gateway 转换层）。

## 失败排查

| 现象 | 可能原因 |
|------|----------|
| 卖单后深度无 asks | 未 reset、卖价低于盘口 best bid，卖单被吃 |
| 订单长期 PENDING | matching / kafka / order 未起 |
| 余额不等 | 重复跑未 reset、或另有订单占用冻结 |
| Gateway 超时 | 增大 `START_WAIT_SEC` 或检查 `logs/` |
