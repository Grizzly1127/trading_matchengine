# 压测脚本

详见 [docs/benchmark.md](../../docs/benchmark.md)。L3 Outbox 优化路线图：[l3-optimization-roadmap.md](../../docs/develop_docs/l3-optimization-roadmap.md)。

```bash
chmod +x scripts/bench/*.sh
make bench-l0                          # L0：报告在 reports/<timestamp>-l0/
./scripts/bench/run-l0.sh --count 10
./scripts/bench/run-l2.sh              # L2：Kafka → Matching
./scripts/bench/e1-orders.sh --rate 500 --duration 3m  # L3：自动充值 + 多用户轮询
./scripts/bench/collect-metrics.sh --delta 60
```
