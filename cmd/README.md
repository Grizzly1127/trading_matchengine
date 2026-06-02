# cmd

各微服务入口，按 [development-roadmap.md](../docs/development-roadmap.md) 逐步添加：

| 目录 | 步骤 | 说明 |
|------|------|------|
| `matching/` | 第 3 步 | 撮合引擎（`-config configs/matching.json`） |
| `order/` | 第 4 步 | 订单服务 |
| `gateway/` | 第 5 步 | API 网关（REST） |
| `push/` | 第 6 步 | WebSocket 推送（`/v1/ws`，默认 `:8081`） |
| `marketdata/` | 第 6 步 | 行情服务 |
| `kline/` | 第 6 步 | K 线服务 |
| `indexprice/` | 已实现 | 指数价格（gRPC `:50054`，Redis `index:{symbol}`） |
