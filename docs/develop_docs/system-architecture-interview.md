# 撮合引擎项目 — 系统架构面试题与参考答案

**版本**: 1.0  
**日期**: 2026-06-07  
**关联**: [architecture-spec.md](./architecture-spec.md) · [matching-event-outbox-design.md](./matching-event-outbox-design.md) · [trading-gate-design.md](./trading-gate-design.md) · [development-checklist.md](./development-checklist.md)

> 本文档面向本仓库「虚拟货币交易所撮合引擎」项目的架构面试准备。每题含**参考答案要点**；实际口述时可按「背景 → 设计 → 不变式 → 故障行为 → 指标」展开。

---

## 目录

1. [总体架构与服务划分](#一总体架构与服务划分)
2. [一致性模型](#二一致性模型)
3. [撮合引擎核心设计](#三撮合引擎核心设计)
4. [WAL、快照与恢复](#四wal快照与恢复)
5. [分片与扩展](#五分片与扩展)
6. [Order Service 与状态机](#六order-service-与状态机)
7. [高可用与 Trading Gate](#七高可用与-trading-gate)
8. [行情、推送与下游](#八行情推送与下游)
9. [故障与边界场景](#九故障与边界场景)
10. [性能、观测与工程实践](#十性能观测与工程实践)
11. [设计权衡与开放题](#十一设计权衡与开放题)

---

## 一、总体架构与服务划分

### Q1. 为什么采用「模块化单集群微服务 + Kafka 事件总线」，而不是单体或强同步 RPC 链？

**参考答案：**

- **职责分离**：Gateway（协议适配）、Order（账务与状态机）、Matching（有状态撮合）、Market Data / Kline / Push（只读聚合）生命周期和扩缩容需求不同，拆服务可独立部署。
- **异步解耦**：下单 API 不必等待撮合完成；Order 通过 Transactional Outbox 投递命令，Matching 按自身节奏消费，避免 RPC 链路过长导致 tail latency 放大。
- **有状态 vs 无状态**：Matching 依赖本地 WAL + Snapshot，不适合与 Order 混在同一进程无限水平扩展；Kafka 按 symbol/shard 分区保证命令有序。
- **不做 2PC**：跨服务用 Outbox + 幂等 + 补偿保证最终一致，符合「成长型生产、不追求机构级极低延迟」的定位（见 architecture-spec §1.1）。

---

### Q2. Gateway、Order、Matching、Market Data、Push 各自的职责边界是什么？哪些可以水平扩展？

**参考答案：**

| 服务 | 职责 | 状态 | 扩展 |
|------|------|------|------|
| **Gateway** | REST/WS 协议适配、鉴权、限流、路由 | 无状态 | 可 HPA 水平扩 |
| **Order** | 下单/撤单/查单、余额冻结、Outbox、消费 match/trade 事件 | PostgreSQL 有状态 | 多副本 Deployment + DB |
| **Matching** | Orderbook、撮合、WAL、Snapshot、Event Outbox | 本地磁盘 + 内存 | **按 shard 分片**，单 partition 单 consumer |
| **Market Data** | 消费事件、深度/Ticker 聚合、Redis 缓存 | 内存 + Redis | 可水平扩（消费组） |
| **Push** | WebSocket 连接、订阅 Redis Pub/Sub | 连接状态放 Redis | 可水平扩 |

**原则**：服务间禁止直连对方数据库；同步用 gRPC，异步用 Kafka（§9 架构约束）。

---

### Q3. 为什么推荐「公网 Web/BFF + 内网 Gateway」？

**参考答案：**

- **安全边界**：用户登录、Session/JWT、充值/KYC 在 Web/BFF；Gateway 监听内网/VPC，不对公网开放。
- **权限隔离**：Gateway 的 `Authorization` 面向**服务账号或 mTLS**，与用户 Session 分离；`UpdateBalance`（调账/充值）仅 Web 或清算服务可调用，禁止浏览器直连。
- **Gateway 保持薄**：不做支付验签、不做业务编排，只做 REST → gRPC 集成（§2.1.1）。
- Phase 1 联调常 localhost 同机，易误解为「用户 API」；生产必须按两层 HTTP 拆分。

---

### Q4. `order.commands` / `match.events` / `trade.events` 为什么要拆成三个 Topic？

**参考答案：**

- **`order.commands`**：Order → Matching 的**命令流**（NewOrder / CancelOrder），按 symbol 路由 partition，Matching 唯一写入方（经 Outbox Relay）。
- **`match.events`**：Matching → 下游的**订单状态变更**（Accepted / Partial / Canceled / Rejected），Order 更新状态机，Market Data 维护盘口镜像。
- **`trade.events`**：**成交事实**，多下游订阅（Order 写 trades 表、Market Data 更新 Ticker、Kline 聚合 OHLCV），保留期更长（30 天 vs 7 天）。
- 拆分便于：独立 consumer group、不同 retention、不同扩缩容与监控；避免一个 Topic 混杂命令与事件导致消费语义混乱。

---

### Q5. 请描述「用户下单到成交回写 DB」的完整链路。

**参考答案：**

```
Client POST /v1/orders
  → Gateway（JWT/限流）→ gRPC PlaceOrder
  → Order Service：
       幂等检查 → 单 PG 事务（冻结余额 + INSERT orders PENDING + INSERT order_outbox）
       → Outbox Relay 异步 → Kafka order.commands[symbol]
  → Matching Engine：
       消费命令 → 命令 WAL fsync → Apply 撮合
       → Event Outbox append + fsync → commit order.commands offset
       → Event Relay 异步 → Kafka match.events / trade.events
  → Order Service 消费 match.events / trade.events：
       更新 orders 状态 + INSERT trades + 扣减/释放冻结余额
```

**同步段**：Client → Order DB 事务提交。  
**异步段**：Outbox → Kafka → 撮合 → Event Outbox → Relay → 事件消费回写。  
**不保证**：API 200 时订单已在 Orderbook（§4.6）。

---

## 二、一致性模型

### Q6. 不用 2PC/XA，跨服务一致性怎么保证？

**参考答案：**

采用 **L1–L6 分层一致性**（§4.1）：

| 层级 | 保证 | 手段 |
|------|------|------|
| L1 | Order 库内强一致 | PG 单事务：冻结 + 订单 + Outbox |
| L2 | Order → Kafka 至少一次 | Transactional Outbox + Relay |
| L3 | Kafka → Matching 有序 + 至少一次 | symbol 固定 partition + order_id 幂等 |
| L4 | Matching 进程内命令原子 | 命令 WAL fsync 后再 apply |
| L4b | 事件至少落盘一次 | 本地 Event Outbox fsync |
| L4c | offset 不超过已落盘事件 | Outbox durable 后才 commit |
| L5 | Matching → Order 最终一致 | Relay + 幂等消费 |
| L6 | 用户语义最终一致 | 状态机 + 超时补偿 + 对账 |

辅以：**乐观锁状态迁移**、**Reconciler 补偿**、**启动对账 §5.6**。

---

### Q7. `PlaceOrder` 返回成功，是否意味着订单已经在 Orderbook 里？

**参考答案：**

**否。** API 契约（§4.6）保证：

- 订单**已持久化**到 PostgreSQL；
- 同一事务内 **Outbox 已写入**（`published_at IS NULL` 待 Relay 投递）；
- 余额**已预冻结**。

**不保证**：命令已进 Kafka、Matching 已消费、已挂单或成交。客户端应依据 `status`（PENDING → ACCEPTED → …）判断生命周期。WebSocket 订单推送应在 Order 消费 `match.events` **之后**发出，避免客户端先于 DB 看到终态。

---

### Q8. Transactional Outbox 解决了什么问题？

**参考答案：**

解决 **「DB 已提交但 Kafka 未发出」** 的原子性缺口：

- 若在事务内直接 `publish` Kafka，可能出现：PG commit 成功但进程崩溃未发消息 → 订单永远 PENDING、撮合永远收不到命令。
- Outbox 模式：命令 payload 与订单**同一事务**写入 `order_outbox`；Relay 异步投递，成功后 `published_at=now()`。
- 投递失败可重试；超过 max_retry 告警人工处理。Matching 侧对称地有 **Event Outbox**（本地磁盘）保证事件不丢。

---

### Q9. Kafka At-Least-Once 下如何防止重复撮合 / 重复扣款？

**参考答案：**

**Matching 侧：**

- 每条 `NewOrderCommand` 含 `uint64 order_id`；
- 撮合前检查 `order_map` / seen 集合，已处理则幂等跳过；
- `CancelOrderCommand` 对不存在订单直接忽略。

**Order 侧：**

- `trades` 表 `trade_id` **唯一索引**，`ON CONFLICT DO NOTHING`；
- 订单状态迁移用 `UPDATE ... WHERE id=? AND status=? AND version=?` 乐观锁；
- 重复 `match.events` 不会改变终态。

**Trade ID 派生**：`DeriveTradeID(wal_seq, maker, taker)` 等确定性算法，保证重复投递产生相同 trade_id。

---

### Q10. L4 / L4b / L4c 分别指什么？Event Outbox 后 offset commit 条件有何变化？

**参考答案：**

- **L4**：Matching 进程内**命令**原子 — 命令 WAL fsync → 再改 orderbook。
- **L4b**：Matching 进程内**事件**至少落盘 — Event Outbox append + fsync。
- **L4c**：`order.commands` offset commit 不超过已 durable 的事件边界。

**变化：**

| | 同步 Publish（旧） | Event Outbox（现） |
|--|-------------------|-------------------|
| commit 条件 | Kafka publish 成功 | **Event Outbox fsync 成功** |
| Kafka 慢的影响 | 拖住 consumer 热路径 | 仅增大 `event_outbox_pending` |
| 端到端可见延迟 | 较短 | 略增（多一跳 Relay） |

不变式 **I2/I3**：禁止在事件未 durable 时 commit offset，否则崩溃会永久丢事件。

---

### Q11. 「先写 WAL 再改内存」如果反过来会怎样？

**参考答案：**

若 **先改 orderbook 再写 WAL**：

- 进程在内存变更后、WAL fsync 前崩溃 → 重启后内存状态丢失，但 Kafka 可能已 commit（旧模式）或未 commit；
- 已撮合的成交/挂单状态无法从 WAL 恢复，与 Kafka 重投命令叠加可能导致**重复撮合**或**状态不一致**；
- 违反架构约束 #11：**绝对禁止先改内存后写 WAL**。

正确顺序：`AppendLog → fsync → Apply → Event Outbox fsync → Commit offset`。

---

### Q12. 哪些场景用户会感知「中间态」？API 与 WS 如何设计？

**参考答案：**

**中间态：**

- `PENDING`：DB 已落库，撮合未确认；
- `CANCELING`：撤单命令已 Outbox，盘口可能尚未移除；
- 成交回写 lag 时：Matching 已成交，DB 仍为 PARTIAL/PENDING。

**设计：**

- REST 返回明确 `status`，文档声明 API 成功 ≠ 已撮合（§4.6）；
- `GetOrder` 以 DB 为准，成交明细以 `trades` 表为准；
- WS 推送在 Order **消费 match.events 并写库之后**再发，避免「先推送后持久化」；
- Trading Gate（设计中）在 Matching 异常时 **503 TRADING_SUSPENDED**，避免 silent PENDING。

---

## 三、撮合引擎核心设计

### Q13. 为什么每个 symbol 必须单线程串行处理？

**参考答案：**

- **价格-时间优先**要求全局确定性顺序：同一 symbol 的命令必须按到达顺序处理，多 goroutine 竞争会导致撮合结果不可重现。
- **无锁热路径**：单 goroutine 串行避免 orderbook 上的读写锁，降低延迟。
- **WAL 顺序**：命令 WAL 与 apply 顺序一一对应，便于回放恢复。
- 扩展靠 **多 symbol 多 SymbolEngine** 或 **多 shard 多进程**，而非单 symbol 多线程。

---

### Q14. Orderbook 数据结构怎么设计？

**参考答案：**

- **本仓库实现**（`internal/matching/engine/orderbook.go`）：买卖各一棵 **跳表** `*skiplist.SkipList`，元素为整笔 `Order`；比较器内嵌 **价格-时间-订单 ID** 优先级：
  - 卖盘 `compareAsk`：价格升序 → 同价 `UpdateTime` 升序（FIFO）→ 同价同时间 `order_id` 升序；
  - 买盘 `compareBid`：价格降序 → 同价时间 / order_id 规则同上。
- **`order_map`**：`map[uint64]Order`，O(1) 按 `order_id` 撤单/对账；与跳表双结构配合。
- **撮合取最优价**：`Front()` / `Iterator()` 从跳表头顺序吃单（`matcher.go`）。
- **快照/导出**：运行时按跳表遍历，**聚合**为 `PriceLevel`（同价订单数组）写入 protobuf Snapshot；恢复时再拆回单笔订单插入跳表。
- **与 architecture-spec 的差异**：架构文档 §2.3 写的是概念模型 `map[price]*PriceLevel` + FIFO 队列；语义等价，实现选用跳表是为有序插入/删除/取最优价 O(log n)。面试时应**以代码为准**说明跳表。
- 热路径避免：反射、无界 channel、全局锁；`orderMap` 预分配 capacity（如 2048）。

---

### Q15. 限价单、市价单、部分成交、撤单在撮合层怎么走？

**参考答案：**

**限价单：**

1. WAL 记录 NewOrderCommand → fsync → TryMatch；
2. 与对手盘价格交叉则按价格-时间成交，产出 TradeEvent；
3. 剩余量挂入 Orderbook → 发 OrderAccepted；
4. 部分成交 → OrderPartialFilled + TradeEvent。

**市价单：**

- 无价格限制，按对手盘最优价依次吃单，直到数量耗尽或盘口为空。

**撤单：**

1. WAL CancelOrderCommand → fsync；
2. 从 Orderbook / order_map 移除；
3. 发 OrderCanceled match event。

重复 order_id / 不存在订单：幂等跳过或忽略。

---

### Q16. Matching 热路径为什么禁止 PostgreSQL / Redis？

**参考答案：**

- 撮合是 **延迟敏感热路径**（目标 P99 ≤ 10ms 量级）；
- PG/Redis 网络 I/O 不可控，可能阻塞 consumer 单 goroutine，拖垮整个 symbol/shard 吞吐；
- 有状态已在本地 WAL + 内存；跨服务一致性通过 **Kafka 事件 + Order 侧 PG** 异步完成；
- Event Outbox 也用 **本地磁盘** fsync，与命令 WAL 同级，遵守 I6。

---

### Q17. 组提交（group commit）是什么？如何权衡？

**参考答案：**

- 配置 `sync_every_records` / `sync_interval_ms`：多条 WAL / Outbox 记录共享一次 `fdatasync`，摊薄磁盘 fsync 成本。
- **`CommitBatch`**：`Sync()` 成功后**按序** apply 该批命令；Sync 前禁止改 orderbook、禁止 commit Kafka offset。
- **权衡**：更大批次 → 更高吞吐、更高单次延迟；更小批次 → 更低延迟、更多 fsync。
- 崩溃时：已 durable 前缀可 WAL 回放；未 durable 部分靠 Kafka 重投 + order_id 幂等。

---

### Q18. 同步 Kafka Publish vs Event Outbox + Relay，为什么优化？

**参考答案：**

**瓶颈（L2 压测 `20260607-003907`）：**

- 热路径：`WAL Sync → Apply → 同步 Kafka Publish → Commit offset`；
- `matching_publish_latency_ms` P99 ≈ **24ms/批**，稳态 TPS ≈ **4373/s**（目标 5000/s）；
- Kafka 墙钟阻塞 consumer **单 goroutine**，是单 symbol 吞吐主矛盾。

**Event Outbox 方案：**

- 热路径止于 Event Outbox fsync + commit offset；
- Relay 独立 goroutine 异步写 Kafka；
- 遵守 I1–I7；下游幂等吸收至少一次投递。

---

## 四、WAL、快照与恢复

### Q19. 撮合引擎重启后如何恢复？请按步骤说明。

**参考答案：**

```
Step 1: 加载 shard manifest + 各 symbol 最新 Snapshot
  → 恢复 Orderbook + order_map
  → shard_recovered_offset = manifest.recovered_offset

Step 2: 回放命令 WAL（offset > recovered_offset 的增量）
  → 按 symbol 分发给 SymbolEngine 重放
  → 仅恢复内存，不重新发布 Kafka 事件

Step 2b: Event Outbox Relay 追平
  → 从 last_published_outbox_seq 续投未发布事件

Step 3: Kafka consumer seek 到 wal_recovered_offset + 1，恢复消费

Step 4: Recovery Verify（§5.6）
  → checksum/CRC 校验 + 与 Order Service 对账
```

K8s：StatefulSet + PVC，同盘 WAL/Snapshot/Outbox。

---

### Q20. Snapshot 里存什么？为什么需要 order_map 和 checksum？

**参考答案：**

**Snapshot 内容（§5.3）：**

- `shard_id`, `symbol`, `seq_id`（已应用 Kafka offset）
- `bids` / `asks` 各 PriceLevel 队列
- **`order_map`**：活跃订单完整索引
- `checksum`（FNV-64 of bids+asks）

**order_map**：撤单、幂等检查、对账 diff 需要 O(1) 按 order_id 查找；仅从价格树推导易不一致。

**checksum**：加载后重算比对，检测磁盘损坏或不完整写入；配合 WAL CRC32 双重校验。

写入：先 `.tmp` → fsync → 原子 rename；保留最近 3 份。

---

### Q21. WAL 回放为什么不重新发布 Kafka 事件？事件丢失怎么办？

**参考答案：**

- 命令 WAL 回放目的仅是 **恢复 orderbook 内存状态**；
- 若回放时再 publish，可能与已 commit offset 之前已发事件 **重复**，或与 Event Outbox 双通道冲突。

**事件恢复**由 **Event Outbox + Relay** 负责：

- Outbox 在 apply 后已 durable（I2）；
- 崩溃在 commit 前：Kafka 重投命令，幂等 skip apply，Relay 发事件；
- 崩溃在 commit 后、relay 前：Relay 从 `last_published_outbox_seq` 续发（场景 E）。

**禁止**从 apply 重复推导发布（避免双通道）。

---

### Q22. Snapshot 损坏或 checksum 不匹配怎么处理？

**参考答案：**

- 加载时 checksum 失败 → 尝试**上一份** Snapshot（保留最近 3 份）；
- 若均失败 → 从更早 WAL 全量回放（更慢但可恢复）；
- WAL CRC 失败 → 告警，该段 WAL 可能磁盘损坏，需运维介入；
- 恢复后 **§5.6 对账**：与 Order PENDING/PARTIAL diff，异常 symbol 进 read_only；
- 生产：Snapshot 定期备份 S3/OSS 用于灾难恢复（Phase 4）。

---

### Q23. 启动对账不一致时，为什么只 read_only 该 symbol，不停整个 shard？

**参考答案：**

- 一个 Matching 进程可承载**多个 SymbolEngine**（共享 shard / partition）；
- 对账 diff 可能是**单 symbol** 数据问题（如事件 lag、补偿滞后），不应连累同 shard 其他正常交易对；
- `SetSymbolReadOnly` 后：拒新单、仍允许撤单（与 MigrationHaltOnlyCancel 对齐）；
- 告警 + 人工排查 root cause，修复后解除 read_only。

---

### Q24. `enable.auto.commit=false`，offset 何时 commit？重复消费是否安全？

**参考答案：**

**commit 条件（Event Outbox 模式，§8.1）：**

对 offset O 的消息 M，当且仅当：

1. M 已写入命令 WAL 且 fsync；
2. M 已 apply（含幂等/只读跳过）；
3. M 产生的所有 Event Outbox 记录已 fsync（无 outbound 则 vacuously 成立）。

**重复消费安全：**

- offset 未 commit → Kafka 重投 M → order_id 幂等 skip；
- 已 commit → 不会重投；
- 下游 Order 亦按 trade_id / 状态机 CAS 幂等。

---

## 五、分片与扩展

### Q25. 「1 交易对 = 1 进程」有什么问题？混合分片模型是什么？

**参考答案：**

**问题：**

- 冷门交易对独占进程 → 资源利用率低；
- 进程数随交易对线性增长 → 运维复杂；
- 并非所有交易对都需要独占 CPU/磁盘。

**混合分片（§2.3.1）：**

| 类型 | 部署 | 场景 |
|------|------|------|
| 热门 | 1 symbol 独占 1 shard/partition/Pod | BTC-USDT 高 TPS |
| 次热门 | 少量 symbol 共享 1 shard | 稳定流量 |
| 冷门 | 多 symbol 合并共享 shard | 低频，提高利用率 |

约束：每个 symbol 仍由**唯一 SymbolEngine 单线程**处理。

---

### Q26. `symbol → shard_id → kafka_partition → node` 映射由谁维护？

**参考答案：**

- **`Shard Manager`**（`pkg/shardmgr`，`configs/shards.json`）维护映射；
- **Order Service** 在 `PlaceOrder` / Outbox Relay 投递前读取映射，确定 `partition_key=symbol` 与目标 partition；
- Matching 进程按 shard 消费对应 partition；
- 第一版可静态配置；后续按 TPS、挂单数、lag、CPU 动态评估（§2.3.1）。

---

### Q27. 热门 symbol 迁移 shard 需要注意什么？

**参考答案：**

- **停牌窗口**：迁移须在停牌 / 只撤单 / 短暂停写内完成；
- **避免双消费**：同一 symbol **不能**同时被两个 shard 处理；
- 流程概要：停新单 → 追平 lag → 对账 → 更新 shard 映射 → 新 shard 从 snapshot/WAL 或 Kafka 续消费 → 恢复交易；
- 与 `shardmgr.MigrationHaltOnlyCancel`、Trading Gate 配合。

---

### Q28. 水平扩展撮合的本质是什么？

**参考答案：**

- 靠 **分片（shard）**：增加 Kafka partition + Matching Pod（StatefulSet），每个 partition **单 consumer 实例**保证有序；
- **不是**单进程无限加 goroutine（单 symbol 仍单线程）；
- 热门 symbol 独占 partition/shard 获得隔离的 CPU、磁盘 I/O、consumer 循环；
- Order / Gateway 无状态水平扩；Matching 有状态垂直 + 分片扩。

---

### Q29. Kafka partition 与 shard 关系？lag 对 Trading Gate 的影响？

**参考答案：**

- 通常 **1 shard ↔ 1 order.commands partition**（可配置）；shard 内多 symbol 共享 partition offset 空间；
- **lag** 在 consumer 侧为 **partition 级**（该 shard 全部 symbol 共享）；
- Trading Gate Monitor 轮询 `GetShardStatus.kafka_lag`，超 `lag_open_threshold` 则该 shard **全部 symbol OPEN**（拒新单）；
- 未来热门 symbol 独占 partition 时可演进为 symbol 级 lag 映射。

---

## 六、Order Service 与状态机

### Q30. `client_order_id` 和 `order_id` 为什么分开？REST 为何用十进制字符串？

**参考答案：**

| 字段 | 类型 | 生成方 | 用途 |
|------|------|--------|------|
| `client_order_id` | string | 客户端 | 幂等键，用户维度唯一，最长 64 |
| `order_id` | uint64 | Order Service | 系统主键，DB/Kafka/撮合/WAL 统一 |

- 禁止把 `client_order_id` 直接当 `order_id`（客户端不可控、非单调）；
- 发号：PG IDENTITY 或 Snowflake（多实例用 instance_id 防撞号）；
- REST JSON 中 `order_id` 用**十进制字符串**（如 `"1000000001"`），避免 JS `Number` 超 2^53 丢精度。

---

### Q31. 订单状态机有哪些状态和合法迁移？

**参考答案：**

**状态：** PENDING → ACCEPTED → PARTIAL → FILLED / CANCELED；任意可撤态 → CANCELING → CANCELED；PENDING 可 → REJECTED。

**要点：**

- `PlaceOrder` → PENDING（Outbox 同事务）；
- 撮合确认 → ACCEPTED；部分成交 → PARTIAL；全成 → FILLED；
- `CancelOrder` → CANCELING → 撮合确认 → CANCELED；
- 超时/拒单/风控 → REJECTED。

**并发：** 乐观锁 CAS；CANCELING 拒绝重复撤单；FILLED/CANCELED/REJECTED 为终态。

---

### Q32. 乐观锁在什么并发场景下必要？

**参考答案：**

- **并发撤单 vs 成交**：同时到达 CANCELING 与 FILLED，只允许一个终态胜出；
- **重复 match.events**：同一 order 多次 PartialFilled，version 防止乱序覆盖；
- **补偿任务 vs 正常消费**：Reconciler REJECTED 与用户撤单竞态；
- SQL：`UPDATE orders SET status=?, version=version+1 WHERE id=? AND status=? AND version=?`，affected rows=0 则重试或放弃。

---

### Q33. 余额预冻结在哪一步？成交/撤单如何结算？

**参考答案：**

**预冻结：** `PlaceOrder` 单 PG 事务内 `account_balances.frozen += X`（与 orders + outbox 同事务）。

**成交：** 消费 `trade.events`，按成交比例 **frozen → 扣减 available**（买卖方向不同，更新 base/quote）。

**撤单：** 消费 OrderCanceled，`status=CANCELED`，**释放剩余 frozen**。

**拒单：** REJECTED，释放全部 frozen。

市价买单：调用 Market Data `GetReferencePrice` 估算冻结额，防余额不足。

---

### Q34. Reconciler 补偿任务处理哪些超时场景？

**参考答案（§4.5）：**

| 场景 | 触发 | 动作 |
|------|------|------|
| Outbox 未发出 | PENDING + outbox unpublished > 30s | Relay 加急；>5min 告警 |
| 撮合未确认 | PENDING + outbox 已发 > 60s 无 Accepted/Rejected | 查 Kafka / Matching 对账 → REJECTED + 解冻 |
| 撤单悬挂 | CANCELING > 30s 无 Canceled | 重发 CancelCommand；>5min 对账 |
| 成交回写 lag | trade.events 已发，Order lag 高 | 扩消费者，不在撮合侧重试 |

内置 scheduler，建议每分钟 + 指标触发。

---

### Q35. 市价买单为什么要 ReferencePrice 冻结？

**参考答案：**

- 市价买单消耗 quote（如 USDT），成交价未知，需按**参考价 × 数量 × 安全系数**预估最大消耗；
- Order Service 在下单时调用 Market Data `GetReferencePrice` 计算冻结额，防止无限制吃单导致余额透支；
- 成交后按实际成交价多退少补（释放多余 frozen 或拒绝不足单）。

---

## 七、高可用与 Trading Gate

### Q36. Matching 为什么采用 L1（StatefulSet + PVC）而不是双活热备？

**参考答案（trading-gate-design §1.4）：**

- 撮合有 **强本地状态**（orderbook + WAL + Outbox），双活需 WAL 实时复制、冲突解决，复杂度高；
- 成长型生产定位：**进程级恢复 + Gate 拒新单** 即可，不做 L3 冷备 Standby、不做双活；
- L1：Pod 崩溃 → K8s 同 PVC 重启 → WAL/Snapshot/Outbox 恢复 → 续消费；
- 恢复期间 Trading Gate OPEN，避免 silent PENDING。

---

### Q37. Matching 宕机时 Order 仍接受下单有什么问题？Gate 如何熔断？

**参考答案：**

**问题：**

- 用户以为下单成功 = 可成交，实际 PENDING 积压；
- 余额长期冻结，Kafka lag 上涨；
- Reconciler 在 Admin 不可达时可能**跳过**超时拒单，PENDING 窗口拉长。

**Gate（按 symbol）：**

- Monitor 轮询 `MatchingAdmin.GetShardStatus`；
- Admin 不可达 / consumer 停 / lag 超阈值 / apply 超时 / read_only → **OPEN**；
- `PlaceOrder` → **503 TRADING_SUSPENDED**，**不写 DB/Outbox**；
- `CancelOrder` 不受影响。

---

### Q38. Gate OPEN/CLOSED 依据哪些信号？为何 hysteresis？

**参考答案：**

**OPEN 条件（任一满足）：**

- Admin gRPC 连续 N 次失败；
- `consumer_running=false`；
- `kafka_lag > lag_open_threshold`；
- `last_command_applied` 超 `stale_apply_seconds`；
- `symbols[].read_only=true`（**仅该 symbol**）。

**恢复 CLOSED：** 全部条件满足且连续 `normal_window_seconds` 正常。

**Hysteresis：** `lag_open_threshold`（如 5000）> `lag_close_threshold`（如 100），避免 lag 在阈值附近抖动导致频繁开关。

---

### Q39. 为什么只熔断 PlaceOrder，不熔断 CancelOrder？

**参考答案：**

- 已有 PENDING/ACCEPTED/PARTIAL 订单在 Matching 异常时，用户仍需**撤单释放风险**；
- 与 `MigrationHaltOnlyCancel`、read_only 语义一致：拒新单、允撤单；
- Cancel 命令积压在 Kafka 可接受，恢复后幂等处理；比让用户无法撤单更安全。

---

### Q40. 多 Order Pod 如何共享 Gate 状态？

**参考答案：**

- Monitor 各 Pod（或 leader）poll Matching → 计算 per-symbol 状态 → **Redis SET** `trading:gate:symbol:{symbol}`；
- `PlaceOrder` 读 Redis（miss 读本地 cache；均无则 **fail-closed** 默认）；
- TTL = `poll_interval_ms * 3`，防止 stale；
- 保证多副本 PlaceOrder 决策一致。

---

### Q41. Gate、Reconciler、启动对账 — 三道防线各解决什么？

**参考答案：**

| 防线 | 时机 | 作用 |
|------|------|------|
| **Trading Gate** | 下单前 | 预防：Matching 异常时不产生新 PENDING |
| **Reconciler** | 运行中定时 | 兜底：已 PENDING/CANCELING 超时补偿 |
| **启动对账 §5.6** | Matching 重启后 | 检测：Orderbook vs DB diff，symbol read_only |

互补：Gate 减少无效单；Reconciler 清历史悬挂；对账防数据不一致扩大。

---

## 八、行情、推送与下游

### Q42. Market Data 为什么不直接读 Matching 内存 Orderbook？

**参考答案：**

- **解耦**：Matching 热路径不服务查询 RPC；Market Data 独立扩缩容；
- **一致性边界**：Matching 内存是权威盘口，但下游需 **事件驱动** 副本，与 Order 看到的状态同源（match.events）；
- **多订阅者**：Push、Gateway REST、Redis 缓存共享 Market Data 聚合结果；
- 禁止跨服务直读内存或对方 DB（§9）。

---

### Q43. 深度 100ms 定时推送 vs 事件驱动增量？

**参考答案：**

**100ms 定时快照（当前）：**

- 合并高频 match 事件，降低 WS 推送风暴；
- 客户端获得稳定刷新率的 top-N 深度。

**纯事件驱动：**

- 更低延迟，但 tick 密集时 CPU/带宽压力大。

**实践：** match.events 增量更新内存镜像 + 定时全量/增量 diff 推送 Redis Pub/Sub。

---

### Q44. Push Service 无状态如何实现？

**参考答案：**

- WebSocket 连接在 Push Pod 本地，**订阅关系**可存 Redis；
- 行情数据来自 **Redis Pub/Sub**（`depth:{symbol}`、`ticker:{symbol}`），任意 Push 实例可订阅；
- 客户端连任意 Push Pod，该 Pod 订阅 Redis 并转发 WS frame；
- 水平扩：加 Push 副本 + LB（WS 需 sticky 或连接级路由）。

---

### Q45. `trade.events` 多消费者如何互不干扰？

**参考答案：**

- Kafka **独立 consumer group**：Order、Market Data、Kline 各一组，各自维护 offset；
- 各服务 **幂等消费**（trade_id / 事件去重）；
- 单条 trade 事件只写一次 trades 表，Market Data 更新 Ticker，Kline 更新 Bar；
- Topic 保留 30 天，支持新 consumer 回溯或 replay。

---

## 九、故障与边界场景

### Q46. Order DB 成功，Outbox Relay 挂 5 分钟，会怎样？

**参考答案：**

- 订单 **PENDING**，`order_outbox.published_at IS NULL`；
- 撮合 **未收到**命令，Orderbook 无此单；
- 余额 **已冻结**；
- Relay 恢复后按 `created_at` 升序重投；>30s Reconciler 加急，>5min 告警；
- 用户查单仍见 PENDING，属预期中间态。

---

### Q47. Matching 已成交，Event Outbox fsync 后、commit offset 前崩溃？

**参考答案：**

- 命令 WAL durable，orderbook **已变更**；
- Event Outbox **durable**，含 match/trade 事件；
- offset **未 commit** → 重启后 Kafka **重投**该命令；
- apply 时 **order_id 幂等 skip**（不重复撮合）；
- Event Relay 从 Outbox **发布事件** → Order 正常回写（场景 D）。

---

### Q48. Kafka 重复投递同一 NewOrderCommand，撮合是否正确？

**参考答案：**

- **正确。** 第二次 apply 检测到 order_id 已在 order_map/seen → 跳过撮合；
- 若第一次已产生 Outbox 事件，Relay 可能重复发 → 下游 trade_id/状态机幂等；
- 关键：**At-Least-Once + 幂等 = 有效恰好一次语义**。

---

### Q49. 撮合已发 trade.events，Order lag 高，用户查单仍 PENDING？

**参考答案：**

- **原因**：L5 最终一致，Order consumer 尚未处理事件；
- **处理**：扩 Order consumer、监控 lag；**不在 Matching 重发成交**；
- **用户解释**：以 DB status 为准，稍后会变 PARTIAL/FILLED；可展示「结算中」；
- Reconciler 对账可验证 Matching 已有成交而 DB 滞后，触发告警。

---

### Q50. 同 shard 内 BTC 对账失败 read_only，ETH 还能交易吗？

**参考答案：**

- **能**（若 ETH 对账通过且未 read_only）；
- read_only **按 symbol** 粒度，不是 shard 级停服；
- BTC 拒新单、允撤单；ETH 正常 PlaceOrder（Gate 亦按 symbol 展开，除非 shard 级 lag 导致全部 OPEN）。

---

### Q51. 重复提交相同 client_order_id，API 应返回什么？

**参考答案：**

- **幂等**：返回**同一 `order_id`** 与当前 DB 状态，不重复冻结、不重复 Outbox；
- 实现：DB `client_order_id` 唯一索引 + Redis SET NX 快速拦截（Redis 过期后以 DB 为准）；
- HTTP 通常仍 200（或 409 视 API 设计），关键是业务层 idempotent。

---

## 十、性能、观测与工程实践

### Q52. 关注哪些 Prometheus 指标？

**参考答案：**

| 指标 | 服务 | 含义 |
|------|------|------|
| `processing_latency_ms` | Kafka consumer | 批处理墙钟延迟 |
| `matching_kafka_lag` / consumer lag | Matching | 命令积压 |
| `matching_publish_latency_ms` | Matching | 发布延迟（Outbox 后应下降） |
| `event_outbox_pending` | Matching | 未 relay 事件数 |
| `outbox_pending_count` | Order | 未投递命令数 |
| `order_stuck_pending_seconds` | Order | PENDING 悬挂时长 |
| WAL fsync 延迟 | Matching | 磁盘瓶颈 |

告警：对账失败、outbox 积压、lag 超阈值、Gate OPEN。

---

### Q53. 撮合热路径要避免哪些 Go 性能陷阱？

**参考答案（SLA + §9）：**

- 反射、热路径 `interface{}` 装箱；
- 无界 channel、阻塞 I/O；
- 全局锁竞争；
- 循环内 `append` 导致频繁扩容；
- 热路径打 PostgreSQL / Redis / HTTP。

**推荐：** 预分配、`uint64` ID、sync.Pool（有度量再引入）、单 symbol 单 goroutine、组提交摊薄 fsync。

---

### Q54. WAL 滚动与 Snapshot 触发如何选型？

**参考答案：**

- **WAL 滚动**：100MB 或 10 分钟 — 控制单文件大小、便于回收；
- **Snapshot**：每 10,000 条或 5 分钟 — 缩短重启回放时间；
- 取**较早触发**者，平衡磁盘占用 vs 恢复速度；
- 已 snapshot 覆盖 + Outbox published 的旧 WAL/Outbox 段可删除（保留 N 段审计）。

---

### Q55. 为什么 Kafka `acks=all`、`min.insync.replicas=2`？

**参考答案：**

- 保证 broker 故障时不丢已 ack 消息；
- Order Outbox Relay、Event Relay 均 `acks=all`，与持久化语义一致；
- Matching consumer **手动 commit**，且 commit 点在本地 durable 之后，形成「本地 + Kafka」双重边界；
- 生产 3 副本 ISR ≥ 2，平衡可用与持久。

---

### Q56. Phase 1 / 2 / 3 架构演进重点？

**参考答案：**

| Phase | 重点 | 验收 |
|-------|------|------|
| **1 MVP** | 单交易对撮合 + WAL + Order Outbox + Gateway REST | 重启不丢单、不重复撮合 |
| **2** | Market Data、Kline、Push WS、Index Price | 实时行情推送 |
| **3** | Shard Manager、多 symbol、K8s StatefulSet、Prometheus、对账告警 | Pod 重启 <30s 恢复 |
| **4** | API Key、审计、限流、S3 备份、压测 5000 TPS | 合规与 DR 演练 |

当前仓库 Phase 1–2 大部分已完成；Event Outbox、Trading Gate 为 L2/L3 优化项。

---

## 十一、设计权衡与开放题

### Q57. TPS 从 ~4373 提升到 5000+，优先优化哪段？

**参考答案：**

1. **Event Outbox**（已实现方向）：去掉热路径同步 Kafka RTT — 主矛盾；
2. **组提交**：调 `sync_every_records` / `sync_interval_ms` 摊薄 fsync；
3. **批量 ProcessBatch**：增大 consumer batch size（在延迟允许范围内）；
4. **磁盘**：本地 SSD、减少 sync 次数；
5. **不加 Matching 副本**处理同一 partition — 会破坏单 consumer 有序语义。

---

### Q58. Event Outbox 引入后，业务如何接受「撮合完成 ≠ Kafka 立即可见」？

**参考答案：**

- 架构本为 **L5/L6 最终一致**，API 从不保证同步可见；
- Outbox 仅增加 **毫秒~秒级** relay 延迟，Order/Market Data 本即异步；
- 监控 `event_outbox_pending`、relay lag；SLA 声明「状态更新延迟 P99 < Xs」；
- 用户侧以 DB/WS 推送为准，非「下单响应即时进盘口」。

---

### Q59. 为什么不用 gRPC 直调 Matching，而坚持 Kafka + Outbox？

**参考答案：**

- **缓冲削峰**：Order 峰值 != Matching 处理能力，Kafka 队列吸收；
- **有序**：partition 内严格顺序，gRPC 多连接难保证；
- **可恢复**：Matching 宕机期间命令在 Kafka 持久化，恢复后续消费；
- **解耦部署**：Matching 有状态单 consumer，Order 无状态多副本；
- gRPC 适合 **Admin 对账、Gate 探测** 等低频控制面，不适合命令主路径。

---

### Q60. 跨机房多活，当前架构最大改造点？

**参考答案：**

- **Matching 状态**：WAL/Snapshot/Outbox 本地盘 → 需 **单写主** 或 CRDT 级改造，极难双活；
- **Kafka**：MirrorMaker / 集群联邦，注意 offset 与顺序；
- **symbol 归属**：每 symbol 仅一 active shard，跨机房切换需 **停写 + lag 追平 + 对账**；
- **Order DB**：多主/冲突解决或单元化按用户分片；
- **仍不做 2PC**；Outbox + 幂等 + 补偿模式可保留，但 RPO/RTO 需重新定义。

---

### Q61. 「不追求机构级极低延迟」在哪些设计上体现？

**参考答案：**

- Kafka 异步命令路径 vs 内存 RPC 直连；
- Event Outbox 异步发布 vs 同步等待 broker ack；
- 组提交摊薄 fsync vs 每条命令 sync；
- 最终一致 + 补偿 vs 跨服务强一致；
- 混合分片提高利用率 vs 每 symbol 独占硬件；
- 深度 100ms 聚合推送 vs tick 级推送；
- Gate + Reconciler 分钟级兜底 vs 毫秒级故障切换。

---

### Q62. 设计「symbol 迁移 shard」Runbook 要点。

**参考答案：**

```
1. 公告 / Trading Gate：symbol OPEN（拒新单），允撤单
2. 等待 order.commands lag → 0
3. 源 shard：刷 Snapshot + flush Outbox，记录 recovered_offset
4. Order Reconciler：确认无 PENDING 悬挂（或仅允许撤单）
5. 更新 shardmgr 映射：symbol → 新 shard/partition
6. 目标 shard：加载 snapshot（或从 Kafka seek）启动 consumer
7. Recovery Verify + Order 对账 diff
8. Gate CLOSED，恢复 PlaceOrder
9. 监控 lag、pending、对账指标 24h
```

**红线：** 禁止迁移窗口内两 shard 同时消费同一 symbol。

---

## 附录：口述建议

回答架构题推荐结构：

1. **一句话结论**
2. **设计选择与 trade-off**
3. **关键不变式**（如先 WAL 后内存、Outbox 后 commit）
4. **故障时行为**（崩溃点 A–E）
5. **可观测指标与运维动作**

**必背清单（P0）：** 下单全链路、L1–L6、WAL+Snapshot 恢复、双 Outbox、symbol 单线程、幂等键（order_id / trade_id / client_order_id）。

---

*文档随 architecture-spec 与 L2/L3 设计演进更新；实现状态见 [development-checklist.md](./development-checklist.md)。*
