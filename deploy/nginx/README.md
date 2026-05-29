# Nginx 统一入口

对外只暴露一个域名/端口，由 Nginx 按路径分流：

| 路径 | 后端 | 默认 upstream |
|------|------|-----------------|
| `/v1/ws` | Push（WebSocket） | `127.0.0.1:8081` |
| 其余（REST） | Gateway | `127.0.0.1:8080` |

配置文件：[trading-api.conf](./trading-api.conf)

## 前置条件

先启动后端（示例）：

```bash
./scripts/dev.sh start --build
```

## 本机启用

```bash
sudo cp deploy/nginx/trading-api.conf /etc/nginx/sites-available/trading-api.conf
sudo ln -sf /etc/nginx/sites-available/trading-api.conf /etc/nginx/sites-enabled/trading-api.conf
# 如有 default 站点冲突，可 sudo rm /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
```

## 验证

```bash
curl -s http://localhost/v1/health
curl -s http://localhost/v1/time

# WebSocket（需 wscat）
wscat -c ws://localhost/v1/ws
```

认证帧与订阅格式见 [rest-api.md §8](../../docs/rest-api.md#8-websocket)。

## Docker / K8s

将 `upstream` 中的 `127.0.0.1` 改为服务名，例如：

```nginx
upstream trading_gateway {
    server gateway:8080;
}
upstream trading_push {
    server push:8081;
}
```

## 生产注意

- 使用 HTTPS（`trading-api.conf` 内已附注释示例 server 块）。
- WebSocket 路径保持 `/v1/ws`，与 Push 进程路由一致。
- 若 Gateway 需感知 HTTPS，依赖 `X-Forwarded-Proto`（配置已设置）。
