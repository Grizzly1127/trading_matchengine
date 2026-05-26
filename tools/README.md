## tools：本地调试小工具

### 1) 解码撮合快照（snapshot）

将 `data/snapshots/.../snapshot_*.pb` 解成 JSON（默认 pretty）：

```bash
go run ./tools/snapshotdump -in data/snapshots/shard-0/BTC-USDT/snapshot_15.pb
```

### 2) 解码 WAL（逐条输出 JSONL）

读取 `data/wal/{shard_id}` 下的所有 `wal_*.log`，按 seq 顺序逐条输出（每条一行 JSON）：

```bash
go run ./tools/waldump -dir data/wal/shard-0
```

只看某个 seq 之后的记录：

```bash
go run ./tools/waldump -dir data/wal/shard-0 -from 10
```

需要把 protobuf payload 也完整输出（体积更大）：

```bash
go run ./tools/waldump -dir data/wal/shard-0 -full
```

