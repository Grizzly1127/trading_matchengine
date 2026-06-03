# Kubernetes 部署

## 目录

| 路径 | 说明 |
|------|------|
| `config/` | 集群内服务发现用 JSON 配置（DNS：`kafka`、`postgres`、`order` 等） |
| `manifests/` | Kustomize 清单（Matching **StatefulSet + PVC**、Order/Gateway Deployment） |
| `helm/trading-engine/` | Helm Chart（与 manifests 等价，支持多 shard `values.yaml`） |

## 前置条件

1. 构建镜像（仓库根目录）：

```bash
make docker-build
```

2. 集群内已有 **Kafka、PostgreSQL、Redis**（开发可用 `deploy/docker-compose.yml` 起基础设施，应用在 `trading` 命名空间通过 Service 名访问）。

3. 将镜像载入集群（kind/minikube）或推送到仓库后修改 `values.yaml` 的 `global.imageRegistry`。

## 方式一：Kustomize

```bash
kubectl apply -k deploy/k8s/manifests
```

Matching 使用 `StatefulSet` + `volumeClaimTemplates`（每 Pod 50Gi `ReadWriteOnce`），数据目录挂载为 `/data`（WAL：`/data/wal/{shard_id}/`）。

## 方式二：Helm

```bash
helm upgrade --install trading deploy/k8s/helm/trading-engine \
  -n trading --create-namespace \
  --set global.imageTag=dev
```

多 shard 示例（与 `configs/shards.prod.json.example` 对齐）：

```yaml
# values 片段
matching:
  shards:
    - shardId: shard-btc
      kafkaPartition: 0
      storageSize: 100Gi
    - shardId: shard-0
      kafkaPartition: 1
      storageSize: 50Gi
```

为 `shard-btc` 增加 `files/matching-shard-btc.json`（可复制 `matching.shard-0.json` 并改 `shard_id` / `partition` / `group_id`）。

## Phase 3.4 验收：Pod 重启

```bash
kubectl delete pod -n trading matching-shard-0-0
kubectl logs -n trading -f matching-shard-0-0
```

确认 PVC 未丢、进程从快照 + WAL 恢复并开始消费 Kafka（目标恢复时间 < 30s，视 WAL 增量而定）。

## 存储类

生产在 `values.yaml` 设置 `matching.storageClassName` 为低延迟 SSD（架构 §6.4）；留空则使用集群默认 StorageClass。
