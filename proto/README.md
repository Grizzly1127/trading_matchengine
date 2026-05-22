# proto

Protobuf 定义，统一放在本目录。生成代码输出到 `pkg/pb/`。

## 文件规划

| 路径 | 说明 |
|------|------|
| `common/types.proto` | 共享：`Side`、`Order`、`Trade`、`Decimal` |
| `order/order.proto` | 订单 gRPC：`PlaceOrder` 等 |
| `matching/matching.proto` | `NewOrderCommand`、`CancelOrderCommand`、成交/状态事件 |

## 订单标识（与架构 §2.2.1 一致）

| 字段 | Protobuf 类型 |
|------|----------------|
| `client_order_id` | `string` |
| `order_id` | `uint64` |
| `command_id`（Outbox/命令幂等） | `uint64`（可用 `order_outbox.id`） |

`Order` 消息示例：

```protobuf
message Order {
  uint64 order_id = 1;
  string client_order_id = 2;
  string symbol = 3;
  // ...
}
```

REST JSON 中 `order_id` 仍写作十进制 **string**，由 Gateway 与 `uint64` 互转。

## 生成 Go 代码

**import 路径**：`import "common/v1/types.proto"` 是相对于 **`proto/` 目录** 的，不是仓库根目录。

```bash
# 仓库根目录
chmod +x scripts/gen-proto.sh
./scripts/gen-proto.sh
```

等价命令：

```bash
protoc --proto_path=proto \
  --go_out=pkg/pb --go_opt=paths=source_relative \
  common/v1/types.proto \
  matching/v1/commands.proto
```

### IDE 报错 “Import was not found or had errors”

1. **proto_path**：在 Cursor/VS Code 的 Protobuf 插件里把 import 根设为 `${workspaceFolder}/proto`。
2. **被 import 文件有语法错误**：例如 `types.proto` 里字段名不能叫 `option`（保留字），会导致连带报 import 失败。

生成脚本见 `scripts/gen-proto.sh`。
