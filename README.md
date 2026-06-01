# trading_matchengine

虚拟货币交易所撮合引擎与服务集群（Go）。设计目标：**高性能 · 高可用 · 低延迟 · 高并发**（见 [docs/architecture-spec.md](docs/architecture-spec.md)）。

## 服务一览

| 进程 | 入口 | 默认端口 | 职责 |
|------|------|----------|------|
| Matching Engine | `cmd/matching` | —（Kafka 消费） | 撮合、WAL、发布 `match.events` / `trade.events` |
| Order Service | `cmd/order` | gRPC `:50051` | 下单/撤单/余额、Outbox、消费撮合事件 |
| Market Data | `cmd/marketdata` | gRPC `:50052`、metrics `:9102` | 深度/Ticker 聚合、写 Redis |
| Kline Service | `cmd/kline` | gRPC `:50053` | K 线聚合、PostgreSQL + Redis |
| Push Service | `cmd/push` | HTTP/WS `:8081` | WebSocket `/v1/ws`、Redis 扇出 |
| API Gateway | `cmd/gateway` | HTTP `:8080` | 对外 REST（转发 gRPC） |
| Index Price | `cmd/indexprice` | — | 占位，未实现 |

```text
Client ──REST──▶ Gateway ──gRPC──▶ Order / Market Data / Kline
Client ──WS────▶ Push ◀── Redis Pub/Sub ◀── Market Data / Kline
                    ▲
              Kafka: order.commands → Matching → match/trade.events
```

## 文档

| 文档 | 说明 |
|------|------|
| [docs/development-roadmap.md](docs/development-roadmap.md) | **开发顺序（建议从这里开始）** |
| [docs/development-checklist.md](docs/development-checklist.md) | **开发清单**（已完成 `[x]` / 待办 `[ ]`） |
| [docs/architecture-spec.md](docs/architecture-spec.md) | 架构与 SLA |
| [docs/rest-api.md](docs/rest-api.md) | 对外 REST / WebSocket |
| [docs/matching-api.md](docs/matching-api.md) | Matching（Kafka / JSONL / 配置） |
| [docs/order-api.md](docs/order-api.md) | Order gRPC |
| [docs/kafka-data.md](docs/kafka-data.md) | Kafka Topic、消息格式、生产/消费 |
| [docs/redis-data.md](docs/redis-data.md) | Redis Key、Pub/Sub、JSON、生产/消费 |
| [scripts/e2e-api.md](scripts/e2e-api.md) | 联调 curl 命令手册 |
| [deploy/nginx/README.md](deploy/nginx/README.md) | Nginx 统一入口（REST + WS） |

## 快速开始

### 1. 环境

- **Go 1.22+**
- **Docker**：PostgreSQL、Redis、Kafka（见 `deploy/docker-compose.yml`）
- 可选：`jq`、`wscat`（联调）

```bash
go version
```

### 2. 依赖与测试

```bash
cd trading_matchengine
go mod tidy
make test
```

### 3. 基础设施

```bash
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh    # order.commands / match.events / trade.events
./scripts/migrate-up.sh             # 需 psql；Order/Kline 也可启动时自动迁移
```

### 4. 一键启动（推荐）

按依赖顺序启动全部已实现服务（matching → order → marketdata → kline → push → gateway）：

```bash
make build
./scripts/dev.sh start --build
./scripts/dev.sh status
```

| 端点 | 地址 |
|------|------|
| REST API | `http://localhost:8080` |
| WebSocket | `ws://localhost:8081/v1/ws` |
| 健康检查 | `GET http://localhost:8080/v1/health` |

联调脚本（充值、限价/市价下单、深度、K 线等）：

```bash
./scripts/e2e-api.sh
# 或分步：./scripts/e2e-api.sh step deposit
```

默认 Token：`configs/gateway.json` 中 `auth.static_token`（与 Push 相同）。

### 5. 单服务脚本

| 脚本 | 服务 |
|------|------|
| `./scripts/matching.sh` | 撮合（默认 `configs/matching.kafka.json`） |
| `./scripts/order.sh` | 订单 |
| `./scripts/marketdata.sh` | 行情 |
| `./scripts/kline.sh` | K 线 |
| `./scripts/push.sh` | WebSocket 推送 |
| `./scripts/gateway.sh` | API 网关 |

```bash
./scripts/order.sh start --build
./scripts/order.sh status
./scripts/order.sh stop
```

日志与 PID：`logs/*.log`、`run/*.pid`。

**重置环境**（清空 DB / Redis / Kafka / WAL / 快照等）：

```bash
./scripts/reset-dev.sh -y --migrate --kafka-topics
./scripts/dev.sh start --build
```

### 6. 仅撮合本地调试（JSONL）

不依赖 Kafka，stdin 逐行 JSON：

```bash
make build-matching
./bin/matching -config configs/matching.json
```

示例输入：

```json
{"op":"new_order","order_id":1,"symbol":"BTC-USDT","side":"sell","price":"100","quantity":"1"}
```

## 构建与代码生成

```bash
make help
make gen-proto          # 生成 pkg/pb/*
make build              # bin/matching order gateway push kline marketdata
make build-order        # 仅编译单个服务（另有 build-matching 等）
make migrate-up
make clean
```

配置文件集中在 `configs/`（如 `order.json`、`gateway.json`、`marketdata.json`、`kline.json`、`push.json`）。

## 目录结构

```text
cmd/                    # 各服务 main
internal/
  matching/             # 撮合引擎（engine、consumer、WAL）
  order/                # 订单、Outbox、余额
  marketdata/           # 行情聚合、Redis 发布
  kline/                # K 线聚合、Worker
  push/                 # WS Hub、Redis 订阅
  gateway/              # REST、gRPC 客户端
pkg/                    # kafka、redis、logger、wal…
proto/                  # Protobuf 定义
migrations/             # 根目录 SQL 迁移
scripts/                # 进程管理、联调、e2e
deploy/                 # docker-compose、nginx
docs/                   # 设计文档
```

## Nginx 统一入口（可选）

对外单一域名：REST → Gateway，WS → Push。

```bash
# 见 deploy/nginx/README.md
sudo cp deploy/nginx/trading-api.conf /etc/nginx/sites-available/
./scripts/dev.sh start --build
curl http://localhost/v1/health
```

## 设计要点（简）

- **撮合热路径**：每 symbol 单线程；先 WAL `fsync` 再改 orderbook（见 `.cursor/rules/trading-engine-sla.mdc`）。
- **命令投递**：Order → Transactional Outbox → Kafka `order.commands`。
- **行情/K 线**：消费 `trade.events` / `match.events`，内存聚合后写 Redis，Push 扇出 WS。
- **账务真相**：PostgreSQL；Redis 仅缓存与推送，详见 [kafka-data.md](docs/kafka-data.md)、[redis-data.md](docs/redis-data.md)。

## License

内部项目；未指定开源协议前请勿对外分发。
