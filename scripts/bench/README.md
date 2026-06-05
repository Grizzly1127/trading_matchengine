# 压测脚本

详见 [docs/benchmark.md](../../docs/benchmark.md)。

```bash
chmod +x scripts/bench/*.sh
make bench-l0                          # L0：报告在 reports/<timestamp>-l0/
./scripts/bench/run-l0.sh --count 10
./scripts/bench/run-l2.sh              # L2：Kafka → Matching
./scripts/bench/e1-orders.sh --deposit # L3：Gateway（需 vegeta）
./scripts/bench/collect-metrics.sh --delta 60
```
