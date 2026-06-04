// bench-producer 向 Kafka order.commands 发送压测命令（L2 进程基准）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/benchutil"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	kafkago "github.com/segmentio/kafka-go"
)

const benchKafkaBatch = 256

func main() {
	var (
		brokers   = flag.String("brokers", "localhost:9092", "Kafka brokers，逗号分隔")
		topic     = flag.String("topic", "order.commands", "命令 topic")
		partition = flag.Int("partition", 0, "目标分区（与 shard kafka_partition 一致）")
		symbol    = flag.String("symbol", "BTC-USDT", "交易对")
		scenario  = flag.String("scenario", "m3", "负载场景: seed|m1|m2|m3|m4")
		rate      = flag.Int("rate", 5000, "目标发送速率（条/秒），seed 场景忽略")
		duration  = flag.Duration("duration", 5*time.Minute, "压测时长（seed/warmup 忽略）")
		warmup    = flag.Uint64("warmup", 0, "正式计时前预热条数（不计入统计）")
		startID   = flag.Uint64("start-order-id", 1_000_000_000, "起始 order_id / command_id")
		seedDepth = flag.Int("seed-depth", 5000, "seed 场景卖盘档位数")
		price     = flag.String("price", "65000", "限价价格（交叉价）")
		restPrice = flag.String("rest-price", "64000", "非交叉限价（m1/m3 挂单）")
		qty       = flag.String("qty", "0.001", "每单数量")
		prefix    = flag.String("client-prefix", "bench", "client_order_id 前缀")
	)
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// 默认 Writer BatchTimeout=1s；逐条 WriteMessages 会导致 seed 5000 条约等 80+ 分钟。
	w := newBenchWriter(brokerList)
	defer func() { _ = w.Close() }()

	ctx := context.Background()
	part := int32(*partition)
	if part < 0 {
		log.Fatal("partition must be >= 0")
	}

	switch strings.ToLower(*scenario) {
	case "seed":
		if err := runSeed(ctx, w, *topic, int(part), *symbol, *price, *qty, *prefix, *startID, *seedDepth); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("seed done: %d sell orders @ %s\n", *seedDepth, *price)
	case "warmup":
		if err := runWarmup(ctx, w, *topic, int(part), *symbol, *price, *restPrice, *qty, *prefix, *startID, *warmup); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("warmup done: %d commands\n", *warmup)
	default:
		if *warmup > 0 {
			if err := runWarmup(ctx, w, *topic, int(part), *symbol, *price, *restPrice, *qty, *prefix, *startID, *warmup); err != nil {
				log.Fatal(err)
			}
			*startID += *warmup
		}
		sent, err := runScenario(ctx, w, *topic, int(part), strings.ToLower(*scenario), *symbol, *price, *restPrice, *qty, *prefix, *startID, *rate, *duration)
		if err != nil {
			log.Fatal(err)
		}
		elapsed := duration.Seconds()
		if elapsed > 0 {
			fmt.Printf("scenario=%s sent=%d avg_rate=%.0f/s\n", *scenario, sent, float64(sent)/elapsed)
		}
	}
}

func newBenchWriter(brokers []string) *kafka.EventWriter {
	return kafka.NewEventWriter(kafka.WriterConfig{
		Brokers:      brokers,
		RequiredAcks: kafkago.RequireOne,
		BatchSize:    benchKafkaBatch,
		BatchTimeout: 2 * time.Millisecond,
	})
}

func runSeed(ctx context.Context, w *kafka.EventWriter, topic string, partition int, symbol, crossPrice, qty, prefix string, startID uint64, depth int) error {
	key := []byte(symbol)
	const chunk = 200
	buf := make([][]byte, 0, chunk)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := w.WriteBatchAt(ctx, topic, partition, key, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}
	for i := 0; i < depth; i++ {
		id := startID + uint64(i)
		body, err := benchutil.MarshalNewOrderEnvelope(benchutil.NewOrderParams{
			CommandID: id, OrderID: id,
			ClientOrderID: benchutil.ClientOrderID(prefix+"-seed", id),
			Symbol:        symbol, Side: commonv1.Side_SIDE_SELL,
			Price: crossPrice, Quantity: qty,
			Partition: uint32(partition),
		})
		if err != nil {
			return err
		}
		buf = append(buf, body)
		if len(buf) >= chunk {
			if err := flush(); err != nil {
				return err
			}
			if (i+1)%1000 == 0 || i+1 == depth {
				log.Printf("seed progress: %d / %d", i+1, depth)
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("seed progress: %d / %d", depth, depth)
	return nil
}

func runWarmup(ctx context.Context, w *kafka.EventWriter, topic string, partition int, symbol, crossPrice, restPrice, qty, prefix string, startID, n uint64) error {
	key := []byte(symbol)
	const chunk = 200
	buf := make([][]byte, 0, chunk)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		err := w.WriteBatchAt(ctx, topic, partition, key, buf)
		buf = buf[:0]
		return err
	}
	for i := uint64(0); i < n; i++ {
		id := startID + i
		price := crossPrice
		if i%2 == 0 {
			price = restPrice
		}
		body, err := marshalBuy(symbol, price, qty, prefix, id, partition)
		if err != nil {
			return err
		}
		buf = append(buf, body)
		if len(buf) >= chunk {
			if err := flush(); err != nil {
				return err
			}
			if (i+1)%2000 == 0 {
				log.Printf("warmup progress: %d / %d", i+1, n)
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("warmup progress: %d / %d", n, n)
	return nil
}

// commandBuffer 压测阶段批量写 Kafka，避免逐条 WriteAt 被 BatchTimeout 限制在 ~200/s。
type commandBuffer struct {
	w         *kafka.EventWriter
	topic     string
	partition int
	key       []byte
	chunk     int
	pending   [][]byte
}

func newCommandBuffer(w *kafka.EventWriter, topic string, partition int, symbol string, chunk int) *commandBuffer {
	return &commandBuffer{
		w: w, topic: topic, partition: partition, key: []byte(symbol), chunk: chunk,
	}
}

func (b *commandBuffer) append(body []byte) error {
	b.pending = append(b.pending, body)
	if len(b.pending) >= b.chunk {
		return b.flush()
	}
	return nil
}

func (b *commandBuffer) flush() error {
	if len(b.pending) == 0 {
		return nil
	}
	if err := b.w.WriteBatchAt(context.Background(), b.topic, b.partition, b.key, b.pending); err != nil {
		return err
	}
	b.pending = b.pending[:0]
	return nil
}

func runScenario(ctx context.Context, w *kafka.EventWriter, topic string, partition int, scenario, symbol, crossPrice, restPrice, qty, prefix string, startID uint64, rate int, duration time.Duration) (int, error) {
	if rate <= 0 {
		return 0, fmt.Errorf("rate must be > 0")
	}
	pipe := newKafkaSendPipeline(w, topic, partition, symbol, queueCapForRate(rate), benchKafkaBatch)
	defer func() {
		if err := pipe.closeAndWait(); err != nil {
			log.Printf("pipeline flush: %v", err)
		}
	}()

	start := time.Now()
	deadline := start.Add(duration)
	var seq uint64
	var sent int
	var lastRestID uint64

	for {
		if err := pipe.firstErr(); err != nil {
			return sent, err
		}
		select {
		case <-ctx.Done():
			return sent, ctx.Err()
		default:
		}
		if !time.Now().Before(deadline) {
			return sent, nil
		}
		if sent > 0 {
			nextAt := start.Add(time.Duration(sent) * time.Second / time.Duration(rate))
			if wait := time.Until(nextAt); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return sent, ctx.Err()
				case <-timer.C:
				}
			}
		}

		id := startID + seq
		seq++
		extraSeq, err := enqueueScenarioTick(ctx, pipe, scenario, symbol, crossPrice, restPrice, qty, prefix, id, partition, seq, &lastRestID)
		if err != nil {
			return sent, err
		}
		seq += extraSeq
		sent++
	}
}

// enqueueScenarioTick 按场景生成一条 tick 的消息并入队（m4 可能 0～2 条 Kafka 消息）。
func enqueueScenarioTick(
	ctx context.Context,
	pipe *kafkaSendPipeline,
	scenario, symbol, crossPrice, restPrice, qty, prefix string,
	id uint64,
	partition int,
	seq uint64,
	lastRestID *uint64,
) (extraSeq uint64, err error) {
	switch scenario {
	case "m1":
		return 0, enqueueBuy(ctx, pipe, symbol, restPrice, qty, prefix, id, partition)
	case "m2":
		return 0, enqueueBuy(ctx, pipe, symbol, crossPrice, qty, prefix, id, partition)
	case "m3":
		if seq%10 < 7 {
			return 0, enqueueBuy(ctx, pipe, symbol, crossPrice, qty, prefix, id, partition)
		}
		return 0, enqueueBuy(ctx, pipe, symbol, restPrice, qty, prefix, id, partition)
	case "m4":
		if *lastRestID != 0 {
			body, mErr := benchutil.MarshalCancelEnvelope(id, symbol, *lastRestID, uint32(partition), 0)
			if mErr != nil {
				return 0, mErr
			}
			if err := pipe.enqueue(ctx, body); err != nil {
				return 0, err
			}
		}
		if err := enqueueBuy(ctx, pipe, symbol, restPrice, qty, prefix, id+1, partition); err != nil {
			return 0, err
		}
		*lastRestID = id + 1
		return 1, nil
	default:
		return 0, fmt.Errorf("unknown scenario %q (use m1|m2|m3|m4)", scenario)
	}
}

func enqueueBuy(ctx context.Context, pipe *kafkaSendPipeline, symbol, price, qty, prefix string, id uint64, partition int) error {
	body, err := marshalBuy(symbol, price, qty, prefix, id, partition)
	if err != nil {
		return err
	}
	return pipe.enqueue(ctx, body)
}

func marshalBuy(symbol, price, qty, prefix string, id uint64, partition int) ([]byte, error) {
	return benchutil.MarshalNewOrderEnvelope(benchutil.NewOrderParams{
		CommandID: id, OrderID: id,
		ClientOrderID: benchutil.ClientOrderID(prefix, id),
		Symbol:        symbol, Side: commonv1.Side_SIDE_BUY,
		Price: price, Quantity: qty,
		Partition: uint32(partition),
	})
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags)
}
