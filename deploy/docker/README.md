# Docker 镜像构建

各服务使用 `deploy/docker/Dockerfile.<service>` 多阶段构建（`CGO_ENABLED=0`），默认用户 `app`（uid 1000）。

## 构建示例

在仓库根目录执行：

```bash
docker build -f deploy/docker/Dockerfile.matching -t trading/matching:dev .
docker build -f deploy/docker/Dockerfile.order -t trading/order:dev .
docker build -f deploy/docker/Dockerfile.gateway -t trading/gateway:dev .
```

或使用通用 Dockerfile：

```bash
docker build -f deploy/docker/Dockerfile --build-arg SERVICE=matching -t trading/matching:dev .
```

## Makefile

```bash
make docker-build          # 构建全部服务镜像（tag: trading/<svc>:dev）
make docker-build-matching # 仅 matching
```

## K8s 运行说明

- **Matching**：配置与数据目录通过 ConfigMap + PVC 挂载，见 `deploy/k8s/`。
- **无状态服务**（order、gateway 等）：使用 Deployment，配置由 Helm `values.yaml` 注入集群内 DNS（`kafka`、`postgres`、`redis` 等）。

镜像内自带 `configs/` 仅作默认；生产以挂载的 `/app/config/*.json` 为准。
