# trading_matchengine

虚拟货币交易所撮合引擎与服务集群（Go）。

## 文档

| 文档 | 说明 |
|------|------|
| [docs/architecture-spec.md](docs/architecture-spec.md) | 架构设计 |
| [docs/development-roadmap.md](docs/development-roadmap.md) | **开发顺序（从这里开始）** |
| [docs/matching-api.md](docs/matching-api.md) | **Matching 接口（Kafka / JSONL / 配置）** |
| [docs/matching-message-flow.md](docs/matching-message-flow.md) | Matching 消息处理与调用链 |
| [docs/order-api.md](docs/order-api.md) | **Order Service gRPC（第 4 步）** |
| [docs/rest-api.md](docs/rest-api.md) | 对外 REST / WebSocket |

## 快速开始（第 0 步）

### 1. 安装 Go

需要 **Go 1.22+**：

```bash
sudo snap install go --classic
# 或: sudo apt install golang-go
go version
```

### 2. 下载依赖并测试

```bash
cd trading_matchengine
go mod tidy
make test
```

预期：`go test ./...` 全部通过。

### 3. 目录说明

```
cmd/              # 各服务 main（后续步骤添加）
internal/         # 业务代码（不可被外部 import）
  matching/engine # 第 1 步：撮合核心
pkg/              # 可复用库（logger、wal、kafka…）
proto/            # Protobuf
migrations/       # SQL 迁移
deploy/           # docker-compose 等
docs/             # 设计文档
```

### 4. 本地撮合进程（第 3.1 步 JSONL）

```bash
make build
./bin/matching -config configs/matching.json
# stdin 每行一条 JSON，例如：
# {"op":"new_order","order_id":1,"symbol":"BTC-USDT","side":"sell","price":"100","quantity":"1"}
```

### 5. Kafka 模式（第 3.2 步）

```bash
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh
./scripts/matching.sh start --build
./scripts/matching.sh status
```

配置 `kafka.enabled=true` 时消费 `order.commands`，WAL fsync 成功后提交 offset，并发布 `match.events` / `trade.events`。未启用时仍走 JSONL 本地模式。

### 6. 进程管理脚本

```bash
./scripts/matching.sh start              # 默认 configs/matching.kafka.json
./scripts/matching.sh stop               # SIGTERM，等待 snapshot_on_exit
./scripts/matching.sh restart --build
./scripts/matching.sh status

# JSONL 本地模式
MATCHING_CONFIG=configs/matching.json ./scripts/matching.sh start
```

| 路径 | 说明 |
|------|------|
| `run/matching.pid` | 进程 PID |
| `logs/matching.log` | 业务日志（配置内 log.file） |
| `logs/matching.stdout` | 进程 stdout/stderr 兜底 |

### 7. Order Service（第 4.1 步 gRPC）

```bash
make build
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh
./scripts/matching.sh start --build
./bin/order -config configs/order.json
```

详见 [docs/order-api.md](docs/order-api.md)（`PlaceOrder` + grpcurl 示例）。

## 基础设施（第 3 步起）

```bash
docker compose -f deploy/docker-compose.yml up -d
```
