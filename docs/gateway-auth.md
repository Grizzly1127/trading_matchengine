# Gateway / Push 内网鉴权（JWT + mTLS）

架构见 [architecture-spec.md §2.1.1](./architecture-spec.md)：终端用户 JWT 在 **Web/BFF**；Gateway/Push 校验 **服务间** 身份。

## 模式

| `auth.mode` | 用途 |
|-------------|------|
| `static` | 本地联调（默认 `configs/gateway.json`） |
| `jwt` | 生产：仅 JWT |
| `static_or_jwt` | 迁移期 |

## Scope

| Scope | 路由 |
|-------|------|
| `orders:read` | `GET /v1/orders`、`GET /v1/trades` |
| `orders:write` | `POST/DELETE /v1/orders` |
| `balances:read` | `GET /v1/balances` |
| `balances:admin` | `POST /v1/balances` |
| `market:read` | `GET /v1/klines` |
| `push:connect` | WebSocket `/v1/ws` |
| `push:ticker_all` | WS 订阅 `ticker@all` / `ticker@all:{quote}`；`GET /v1/market/ticker/all`（做市商） |

`static` 模式默认按 `configs/gateway.json` / `configs/push.json` 的 **`static_scopes`**（普通档位，**不含** `push:ticker_all`）。做市商联调使用 **`market_maker_static_token`**（含 `push:ticker_all`），或与 `cmd/auth` 的 `market-maker` 客户端签发 JWT 一致。

Push 限流（`configs/push.json` → `limits`）：普通用户每 subject 默认最多 **5** 条连接、每连接最多 **50** 个 symbol 频道；做市商（含 `push:ticker_all`）每 subject 默认最多 **3** 条连接。

## 轻量签发（dev/staging）

```bash
cp configs/auth-dev-hs256.secret.example configs/auth-dev-hs256.secret
go run ./cmd/auth -config configs/auth.json

curl -s -X POST http://localhost:8090/v1/token \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"web-bff","client_secret":"dev-client-secret-change-me"}' | jq .
```

Gateway 使用 JWT 时示例见 `configs/gateway.prod.json.example`（可同时配置外部 JWKS + 本地 HS256 issuer）。

## mTLS

`tls.enabled=true` 且配置 `cert_file` / `key_file` / `client_ca_file` 时启用 HTTPS + 可选客户端证书校验。与 JWT **叠加**：须先建立 mTLS，再带 `Authorization: Bearer <jwt>`。

生成测试证书（示例，勿用于生产）：

```bash
./scripts/gen-dev-mtls.sh
# 将 gateway.prod.json.example 中 tls 路径指向 configs/dev-mtls/
```

本地 JWT 联调（无 mTLS）：

```bash
./scripts/dev.sh start --build --auth --jwt
./scripts/e2e-api.sh jwt
```

或手动：`cp configs/*.jwt-dev.json.example` → `configs/*.jwt-dev.json`，分别启动 `auth.sh`、`gateway.sh`、`push.sh`（见 `scripts/dev.sh --jwt` 导出的环境变量）。

## Push

与 Gateway 共用 `auth.jwt` 配置形态；握手需 `Authorization: Bearer` 或查询参数 `?token=`，且 JWT 含 `push:connect`。
