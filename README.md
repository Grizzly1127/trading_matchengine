# trading_matchengine

虚拟货币交易所撮合引擎与服务集群（Go）。

## 文档

| 文档 | 说明 |
|------|------|
| [docs/architecture-spec.md](docs/architecture-spec.md) | 架构设计 |
| [docs/development-roadmap.md](docs/development-roadmap.md) | **开发顺序（从这里开始）** |
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

### 4. 下一步

按 [development-roadmap.md](docs/development-roadmap.md) **第 1 步** 实现 `internal/matching/engine` 的 orderbook 与 matcher。

## 基础设施（可选，第 3 步起）

```bash
docker compose -f deploy/docker-compose.yml up -d
```
