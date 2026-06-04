# trading_matchengine

虚拟货币交易所撮合引擎与服务集群（Go）。设计目标：**高性能 · 高可用 · 低延迟 · 高并发**。

## 服务一览

| 进程 | 入口 | 默认端口 | 职责 |
|------|------|----------|------|
| Matching Engine | `cmd/matching` | Admin gRPC `:50061`、metrics `:9101` | 撮合、WAL、消费 `order.commands`、发布 `match.events` / `trade.events` |
| Order Service | `cmd/order` | gRPC `:50051`、metrics `:9104` | 下单/撤单/余额、Outbox、消费撮合事件 |
| Market Data | `cmd/marketdata` | gRPC `:50052`、metrics `:9102` | 深度/Ticker 聚合、写 Redis |
| Kline Service | `cmd/kline` | gRPC `:50053`、metrics `:9105` | K 线聚合、PostgreSQL + Redis |
| Push Service | `cmd/push` | HTTP/WS `:8081` | WebSocket `/v1/ws`、Redis 扇出 |
| API Gateway | `cmd/gateway` | HTTP `:8080` | 对外 REST（转发 gRPC） |
| Index Price | `cmd/indexprice` | gRPC `:50054` | 多交易所指数价、Redis/Kafka、PostgreSQL 审计 |
| Auth（可选） | `cmd/auth` | HTTP `:8090` | 内网 JWT 签发（dev/staging） |

```text
                    ┌──────────── Nginx（生产统一入口 :443）────────────┐
                    │  /v1/* REST → Gateway    /v1/ws → Push           │
                    └───────────────┬─────────────────┬────────────────┘
                                    │                 │
Client ──HTTPS/WS─────────────────┘                 │
                                                      │
         Gateway ──gRPC──▶ Order / Market Data / Kline / Index Price
         Push ◀── Redis Pub/Sub ◀── Market Data / Kline
              ▲
    Kafka: order.commands → Matching（StatefulSet + PVC）→ match/trade.events → Order
```

| 组件 | 有状态？ | 扩展方式 |
|------|----------|----------|
| Matching | 是（WAL + 快照，按 **shard**） | StatefulSet + PVC，见 [deploy/k8s/README.md](deploy/k8s/README.md) |
| Order | 否（权威在 PostgreSQL） | Deployment 水平扩 |
| Gateway / Push | 否 | Deployment + Nginx 负载 |

## 文档

| 文档 | 说明 |
|------|------|
| [docs/rest-api.md](docs/rest-api.md) | 对外 REST / WebSocket |
| [docs/matching-api.md](docs/matching-api.md) | Matching（Kafka / JSONL / 配置） |
| [docs/order-api.md](docs/order-api.md) | Order gRPC |
| [docs/kafka-data.md](docs/kafka-data.md) | Kafka Topic、消息格式、生产/消费 |
| [docs/redis-data.md](docs/redis-data.md) | Redis Key、Pub/Sub |
| [docs/gateway-auth.md](docs/gateway-auth.md) | 生产 JWT / mTLS |
| [scripts/e2e-api.md](scripts/e2e-api.md) | 联调 curl 命令手册 |
| [deploy/k8s/README.md](deploy/k8s/README.md) | Kubernetes / Helm |
| [deploy/docker/README.md](deploy/docker/README.md) | 镜像构建 |
| [deploy/nginx/README.md](deploy/nginx/README.md) | Nginx 配置说明 |
| [docs/develop_docs/architecture-spec.md](docs/develop_docs/architecture-spec.md) | 架构与 Phase 验收 |
| [docs/benchmark.md](docs/benchmark.md) | 性能基准与压测（L0～L3） |

---

## 本地开发（快速开始）

### 1. 环境

- **Go 1.26+**（见 `go.mod`）
- **Docker**：PostgreSQL、Redis、Kafka（`deploy/docker-compose.yml`）
- 可选：`jq`、`wscat`、`nginx`（统一入口联调）

```bash
go version
```

### 2. 依赖与测试

```bash
cd trading_matchengine
go mod tidy
make test
make bench-l0          # 微基准（matcher / WAL / skiplist）
# make bench-l2        # 需先 dev.sh start；见 docs/benchmark.md
```

### 3. 基础设施

```bash
docker compose -f deploy/docker-compose.yml up -d
./scripts/kafka-create-topics.sh    # order.commands / match.events / trade.events
./scripts/migrate-up.sh             # 需 psql；Order/Kline 也可启动时自动迁移
```

### 4. 一键启动（推荐）

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

```bash
./scripts/e2e-api.sh
```

默认联调：`configs/gateway.json` 中 `auth.mode=static`。JWT 全栈：`./scripts/dev.sh start --build --auth --jwt`（见 [docs/gateway-auth.md](docs/gateway-auth.md)）。

### 5. 单服务 / 重置 / JSONL 撮合

| 脚本 | 服务 |
|------|------|
| `./scripts/matching.sh` | 撮合（`configs/matching.json` 或 Kafka 配置） |
| `./scripts/order.sh` | 订单 |
| `./scripts/marketdata.sh` / `./scripts/kline.sh` / `./scripts/push.sh` | 行情 / K 线 / WS |
| `./scripts/gateway.sh` | API 网关 |

```bash
./scripts/reset-dev.sh -y --migrate --kafka-topics
./scripts/dev.sh start --build
```

仅撮合本地调试（无 Kafka）：`make build-matching` → `./bin/matching -config configs/matching.json`。

### 6. 本机 Nginx 统一入口（可选）

与生产相同路径：`/` → Gateway，`/v1/ws` → Push。

```bash
./scripts/dev.sh start --build
sudo cp deploy/nginx/trading-api.conf /etc/nginx/sites-available/trading-api.conf
sudo ln -sf /etc/nginx/sites-available/trading-api.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

curl -s http://localhost/v1/health
# wscat -c ws://localhost/v1/ws
```

详见 [deploy/nginx/README.md](deploy/nginx/README.md)。

---

## 生产环境部署（完整流程）

以下流程假设：**应用在 Kubernetes 内运行**，**Nginx 作为集群外或 Ingress 前的统一入口**（TLS 终结、REST/WS 分流）。中间件（Kafka / PostgreSQL / Redis）可用云托管或集群内 Helm，与 [架构 Phase 3](docs/develop_docs/architecture-spec.md) 一致。

### 架构关系（生产）

```text
Internet
   │
   ▼
[Nginx :443]  ──REST──▶ gateway:8080 (Deployment, 可多副本)
   │          ──WS────▶ push:8081    (Deployment)
   │
   ├── order:50051          (Deployment → 共享 PostgreSQL)
   ├── matching-shard-*     (StatefulSet + PVC，每 shard 独立 WAL)
   ├── marketdata / kline / indexprice（按需 Deployment）
   │
   ├── Kafka（order.commands / match.events / trade.events）
   ├── PostgreSQL（订单、余额、成交）
   └── Redis（行情 Pub/Sub、可选幂等缓存）
```

**分片**：Order 无状态、多副本写同一库；Matching 按 `configs/shards.json`（或 `shards.prod.json.example`）路由 Kafka partition。见 `pkg/shardmgr`。

### 步骤 0：准备清单

| 项 | 说明 |
|----|------|
| 容器仓库 | 推送 `trading/<service>:<tag>` |
| K8s 集群 | 建议节点本地盘或低延迟 StorageClass 给 Matching PVC |
| 域名与证书 | `api.example.com`，TLS 证书（Let’s Encrypt 或企业 CA） |
| 密钥 | DB 连接串、JWT HS256 / JWKS、mTLS 证书 |
| 配置 | `shards.json`、`symbols.json`、各服务生产 JSON |

### 步骤 1：构建并推送镜像

在仓库根目录：

```bash
make docker-build
# 或单服务：make docker-build-matching
```

打 tag 并推送到仓库（示例）：

```bash
export REG=registry.example.com/trading
export TAG=v1.0.0
for s in matching order gateway push marketdata kline indexprice auth; do
  docker tag trading/${s}:dev ${REG}/${s}:${TAG}
  docker push ${REG}/${s}:${TAG}
done
```

Dockerfile 说明：[deploy/docker/README.md](deploy/docker/README.md)（多阶段 `alpine`，运行用户 uid `1000`）。

### 步骤 2：部署中间件

开发环境可继续用 compose 仅起中间件：

```bash
docker compose -f deploy/docker-compose.yml up -d
```

**生产**请单独规划（清单 Phase 3.2 后续项）：

- **Kafka**：3 副本；Topic `order.commands` 分区数 ≥ Matching shard 数
- **PostgreSQL**：主从 + pgbouncer；执行 `migrations/` / `internal/order/repository/migrations/`
- **Redis**：Cluster；行情与 WS 扇出

应用在 K8s 内通过 Service 名访问，例如 `kafka:9092`、`postgres:5432`、`redis:6379`（与 `deploy/k8s/config/*.json` 一致）。

### 步骤 3：配置生产参数

1. **分片** — 复制并按环境修改：
   - `configs/shards.prod.json.example` → 集群 ConfigMap（热门 symbol 可独占 shard/partition）
   - Helm 嵌入目录：`deploy/k8s/helm/trading-engine/config-embed/`

2. **Gateway / Push 鉴权** — 勿使用 `static` token：
   - 参考 `configs/gateway.prod.json.example`、`configs/push.prod.json.example`
   - `auth.mode=jwt`，配置 JWKS 或 `hs256_secret_file`
   - 内网 mTLS：见 [docs/gateway-auth.md](docs/gateway-auth.md)、`./scripts/gen-dev-mtls.sh`（仅测试）

3. **Matching 每 shard** — 在 `deploy/k8s/helm/trading-engine/files/` 增加 `matching-<shardId>.json`：
   - `data_dir`: `/data`（挂载 PVC）
   - `kafka.partition` 与 Shard Manager 一致
   - `order_service.grpc_addr`: `order:50051`

4. **敏感信息** — 使用 K8s Secret 注入 `database_url`、Redis 密码、JWT secret，不要写进镜像。

### 步骤 4：部署应用到 Kubernetes

**Helm（推荐，支持多 shard）：**

```bash
helm upgrade --install trading deploy/k8s/helm/trading-engine \
  -n trading --create-namespace \
  --set global.imageRegistry=registry.example.com/trading \
  --set global.imageTag=v1.0.0 \
  --set matching.storageClassName=local-ssd   # 按集群填写
```

按需开启 `values.yaml` 中的 `marketdata`、`kline`、`push` 等。

**Kustomize：**

```bash
# 先构建并载入镜像（kind 示例）
make docker-build
kind load docker-image trading/matching:dev trading/order:dev trading/gateway:dev

kubectl apply -k deploy/k8s/manifests
```

核对：

```bash
kubectl -n trading get pods,pvc,svc
kubectl -n trading logs -f matching-shard-0-0
```

**Matching 有状态要点**：

- `StatefulSet` + `volumeClaimTemplates` → `/data/wal/{shard_id}/`、`/data/snapshots/`
- 同一 `shard_id` 不得挂载同一块 PVC 到多个 Pod
- 验收：删除 Pod 后 **&lt; 30s** 从 WAL/快照恢复（Phase 3.4）

详见 [deploy/k8s/README.md](deploy/k8s/README.md)。

### 步骤 5：部署 Nginx 统一入口

对外只暴露 **Nginx**（或云 LB + Nginx），路径与开发一致：

| 路径 | 后端（K8s Service） |
|------|---------------------|
| `/v1/ws` | `push:8081`（WebSocket） |
| 其余 `/v1/*` | `gateway:8080`（REST） |

**方式 A — 集群外 Nginx（指向 NodePort / LB）**

编辑 [deploy/nginx/trading-api.conf](deploy/nginx/trading-api.conf) 中 `upstream`：

```nginx
upstream trading_gateway {
    server <GATEWAY_LB_OR_NODEIP>:8080;
    keepalive 32;
}
upstream trading_push {
    server <PUSH_LB_OR_NODEIP>:8081;
    keepalive 8;
}
```

启用 HTTPS：使用文件内注释的 `listen 443 ssl` server 块，配置 `ssl_certificate` / `ssl_certificate_key`，`server_name` 改为真实域名。

```bash
sudo cp deploy/nginx/trading-api.conf /etc/nginx/sites-available/trading-api.conf
sudo ln -sf /etc/nginx/sites-available/trading-api.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

**方式 B — K8s 内 Nginx Deployment**

将 `upstream` 改为集群 DNS：

```nginx
upstream trading_gateway { server gateway.trading.svc.cluster.local:8080; }
upstream trading_push    { server push.trading.svc.cluster.local:8081; }
```

**方式 C — Ingress Controller**

需两个路由规则：普通 HTTP → Gateway；`/v1/ws` → Push 并启用 WebSocket annotations（各 Ingress 实现不同，逻辑与 `trading-api.conf` 相同）。

生产注意（[deploy/nginx/README.md](deploy/nginx/README.md)）：

- WebSocket 路径必须为 `/v1/ws`
- 已设置 `X-Forwarded-Proto`，Gateway 可识别 HTTPS
- `client_max_body_size` 按业务调整

### 步骤 6：上线检查

```bash
# 经 Nginx
curl -s https://api.example.com/v1/health
curl -s https://api.example.com/v1/time

# 带 JWT（生产）
export TOKEN="<service-jwt>"
curl -s -H "Authorization: Bearer $TOKEN" https://api.example.com/v1/market/symbols

# 集群内
kubectl -n trading get pods
kubectl -n trading top pods
# Prometheus：matching P99、WAL 写延迟、Kafka lag（Phase 3.3）
```

**建议顺序**：中间件就绪 → Matching StatefulSet Ready → Order → 行情链路 → Gateway/Push → Nginx → 外网探测。

**撮合恢复演练**：

```bash
kubectl -n trading delete pod matching-shard-0-0
kubectl -n trading logs -f matching-shard-0-0
# 确认 PVC 仍在、消费 order.commands 恢复
```

### 步骤 7：与开发环境的差异

| 项 | 开发 (`dev.sh`) | 生产 |
|----|-----------------|------|
| 入口 | 直连 `:8080` / `:8081` 或本机 Nginx `:80` | Nginx `:443` + JWT/mTLS |
| Matching | 单进程、本地 `data/` | StatefulSet + PVC + 多 shard |
| Order | 单副本 | Deployment 多副本 + 共享 DB |
| Kafka 消费 | 常固定 partition `0` | 按 `shards.json` 发命令；Matching 按 shard 消费 |
| 配置 | `configs/*.json` | ConfigMap / Secret + `deploy/k8s/config/` |

---

## 构建与代码生成

```bash
make help
make gen-proto              # 生成 pkg/pb/*
make build                  # bin/* 全服务
make docker-build           # 全服务镜像 trading/*:dev
make helm-template          # 渲染 Helm（需 helm）
make kustomize-build        # 渲染 Kustomize
make migrate-up
make clean
```

配置文件：`configs/`。K8s 覆盖配置：`deploy/k8s/config/`、`deploy/k8s/helm/trading-engine/files/`。

## 目录结构

```text
cmd/                    # 各服务 main
internal/               # matching、order、gateway、push…
pkg/                    # kafka、redis、wal、shardmgr…
proto/                  # Protobuf
migrations/             # SQL 迁移
scripts/                # dev.sh、e2e、单服务脚本
deploy/
  docker/               # Dockerfile.* 镜像
  k8s/                  # Helm Chart、Kustomize manifests
  nginx/                # 生产/开发统一入口
  docker-compose.yml    # 本地 PG / Redis / Kafka
docs/                   # API 与设计文档
```

## 设计要点（简）

- **撮合热路径**：每 symbol 单线程；先 WAL `fsync` 再改 orderbook（见 `.cursor/rules/trading-engine-sla.mdc`）。
- **命令投递**：Order → Transactional Outbox → Kafka `order.commands`（分区由 Shard Manager 决定）。
- **行情/K 线**：消费 `trade.events` / `match.events`，写 Redis，Push 扇出 WS。
- **账务真相**：PostgreSQL；Redis 为缓存与推送。
