# 市价买单冻结方案（方案 C）

**版本**: 1.0  
**日期**: 2026-05-24  
**状态**: 待实现（依赖 Market Data Service）  
**关联**: [order-api.md](../order-api.md) · [development-roadmap.md](../development-roadmap.md) · [architecture-spec.md](../architecture-spec.md)

---

## 1. 背景

`PlaceOrder` 在撮合前必须在 PostgreSQL 单事务内完成余额冻结（见 architecture-spec §4.3）。冻结发生在 **Matching 给出成交价之前**，因此市价买单无法像限价单那样用用户指定的 `price × quantity` 精确计算。

| 方向 | 冻结资产 | 是否需要 price |
|------|----------|----------------|
| 限价买 / 卖 | quote / base | 限价买需要；卖只需 quantity |
| **市价卖** | base = `quantity` | **不需要** |
| **市价买** | quote（如 USDT） | **需要估算上限**（本文档） |

撮合侧：市价单 `price` 可为空，按盘口最优价成交（见 [matching-api.md](../matching-api.md)）。Order 侧冻结与撮合 price **解耦**。

---

## 2. 当前实现（临时，Phase 1）

**未接 Market Data 前的折中**：

- **市价卖**：`ComputeFreeze` 冻结 `quantity` 的 base 资产。
- **市价买**：`validatePlaceOrder` **拒绝**不传 `price` 的请求（`MARKET buy requires price for balance freeze`）。
- 若强行传入 `price`，则当作「保护价 / 冻结上限」，公式仍为 `quote = price × quantity`。

代码位置：

- `internal/order/repository/freeze.go` — `ComputeFreeze` / `RemainingFreeze`
- `internal/order/service/service.go` — `validatePlaceOrder`

**局限**：与常见 REST 语义不一致（市价买不应要求用户填 price）；保护价由用户自行估算，体验差且易误填。

---

## 3. 目标方案 C：行情估算 + 滑点缓冲

**原则**：Order Service 下单时调用 **Market Data Service** 获取参考价，按公式冻结 quote；用户 **不必** 为市价买单填写 `price`。

### 3.1 参考价选取

| 优先级 | 数据源 | 说明 |
|--------|--------|------|
| 1 | **Best Ask**（卖一） | 市价买单最贴近「立即成交」成本；无卖盘时降级 |
| 2 | **Mark Price** / **Last Price** | 来自 Ticker；流动性差时的兜底 |
| 3 | — | 均无有效报价 → 拒单 `FailedPrecondition` / `Unavailable` |

gRPC 接口（Market Data 第 6 步实现后约定，名称可调整）：

```protobuf
// 示例，以实现时 proto 为准
rpc GetReferencePrice(GetReferencePriceRequest) returns (GetReferencePriceResponse);

message GetReferencePriceRequest {
  string symbol = 1;
  ReferencePriceKind kind = 2; // BEST_ASK | MARK | LAST
}
message GetReferencePriceResponse {
  common.v1.Decimal price = 1;
  int64 updated_at_ms = 2;
}
```

### 3.2 冻结公式

```text
freeze_quote = reference_price × quantity × (1 + slippage_buffer)
```

| 参数 | 来源 | 说明 |
|------|------|------|
| `reference_price` | Market Data | 见 §3.1 |
| `quantity` | 用户请求 | base 数量 |
| `slippage_buffer` | 配置 / 交易对元数据 | 如 `0.005`（0.5%）；可 per-symbol，后续风控可调 |

**精度**：使用 `shopspring/decimal`；向上取整到 quote 资产精度（如 USDT 2 位），避免冻结不足。

### 3.3 写入 orders 表（建议扩展）

市价买单落库时保存冻结快照，供撤单释放与对账：

| 字段（建议） | 类型 | 说明 |
|--------------|------|------|
| `freeze_price` | NUMERIC / TEXT | 下单时采用的 `reference_price` |
| `freeze_slippage` | NUMERIC | 使用的 `slippage_buffer` |
| `frozen_amount` | NUMERIC | 实际冻结的 quote 数量 |
| `price` | 可 NULL | 撮合语义仍无限价；**不**把 freeze_price 当作限价发给 Matching |

`RemainingFreeze` 对市价买单改为：

```text
remaining_quote = frozen_amount × (remaining_qty / original_qty)
```

或等价：`freeze_price × (1 + slippage) × remaining_qty`（与落库字段一致即可）。

---

## 4. 成交流程中的冻结扣减

成交结算（`ApplyTradeEvent`）对买单：

```text
consume_frozen_quote = trade_price × trade_qty   // 按实际成交价
```

- 若 `consume_frozen_quote <` 本笔应从冻结扣减的预期：按实际扣减。
- 部分成交后，剩余冻结仍按 §3.3 的 `remaining_quote` 逻辑；**FILLED / CANCELED** 时释放未用冻结。
- 若成交价高于冻结上限（极端滑点）：当前方案 **拒单或整单 REJECTED + 释放**（实现时需与风控对齐；可选后续：允许负 balance 告警 + 人工补款，Phase 1 不做）。

---

## 5. 依赖与启用时机

| 依赖 | 阶段 | 说明 |
|------|------|------|
| Market Data Service gRPC | 第 6 步 6.1 | 至少提供 symbol 级 Best Ask / Mark / Last |
| Order 配置 | 与 6.1 同期 | `marketdata.grpc_addr`、`slippage_buffer_default`、per-symbol 覆盖 |
| 健康检查 | 6.1 | 报价过期（如 `updated_at_ms` 超过 5s）→ 拒单或降级 Mark Price |

**路线图位置**：[development-roadmap.md §第 6 步](../development-roadmap.md#第-6-步行情与推送phase-2约-3-4-周) 完成 **6.1 Market Data Service** 后，在 Order Service 中实现本文档。

---

## 6. API 行为变更（实现后）

| 场景 | 当前 | 实现方案 C 后 |
|------|------|----------------|
| `ORDER_TYPE_MARKET` + `SIDE_BUY` + 无 `price` | `InvalidArgument` | **允许**；服务端查价并冻结 |
| `ORDER_TYPE_MARKET` + `SIDE_BUY` + 有 `price` | 当作保护价冻结 | **可选**：忽略用户 price，或当作 `max_notional` 上限（产品待定） |
| `ORDER_TYPE_LIMIT` | 不变 | 不变 |
| `ORDER_TYPE_MARKET` + `SIDE_SELL` | 不变 | 不变 |

REST Gateway（第 5 步）与 gRPC 保持一致；[rest-api.md](../rest-api.md) 中市价买单 `price` 字段改为可选。

---

## 7. 失败与降级

| 情况 | 行为 |
|------|------|
| Market Data 超时 / 不可用 | 拒单，不冻结；返回 `Unavailable` |
| 无卖盘且无 Mark/Last | 拒单 `FailedPrecondition` |
| 报价过期 | 拒单或刷新一次后重试（可配置） |
| 冻结后 MD 价格剧变 | 不影响已 **fill-wins**；靠 `slippage_buffer` + 成交后按实价扣减 + 终态释放 |

不在 Order Service 热路径上缓存长期行情；单次 PlaceOrder 同步 RPC 查价即可（非撮合热路径）。

---

## 8. 实现清单（Market Data 就绪后）

- [ ] Market Data：`GetReferencePrice`（或等价 RPC）
- [ ] `internal/order/marketdata/`：gRPC client + 超时/熔断
- [ ] `configs/order.json`：`marketdata` 段 + `slippage_buffer`
- [ ] migration：`orders` 增加 `freeze_price` / `frozen_amount`（或 JSONB `freeze_meta`）
- [ ] `ComputeFreeze`：市价买分支调用 MD，去掉「必须 price」
- [ ] `validatePlaceOrder`：市价买不再要求 `price`
- [ ] `RemainingFreeze`：市价买按冻结快照释放
- [ ] `InsertPending`：同事务写入"冻结快照"字段
- [ ] 单测：mock MD client；集成测：testcontainers + stub MD
- [ ] 更新 [order-api.md](../order-api.md)、[rest-api.md](../rest-api.md)

\* 字段命名以实现时 migration 为准。

---

## 9. 相关代码

| 路径 | 变更类型 |
|------|----------|
| `internal/order/repository/freeze.go` | 市价买冻结逻辑 |
| `internal/order/service/service.go` | 校验规则 |
| `internal/order/repository/repository.go` | `InsertPending` 写冻结快照 |
| `internal/order/repository/match_apply.go` | 终态释放（若 RemainingFreeze 变更） |
| `internal/order/repository/trade_apply.go` | 成交 consume frozen（已有，可能微调） |

---

## 10. 未选方案（备忘）

| 方案 | 简述 | 未采用原因 |
|------|------|------------|
| A | 用户传 price 作保护价 | 当前临时实现；UX 差 |
| B | `quote_quantity` 下单 | 需改 API 语义；可与 C 并存，后续再议 |
| D | 冻结全部可用 quote | 锁死资金，生产不可用 |
