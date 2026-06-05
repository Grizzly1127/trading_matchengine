package main

import (
	"context"
	"sync"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// kafkaSendPipeline 异步写 Kafka：主 goroutine 只负责按速率 marshal 入队，避免 flush 阻塞限速。
type kafkaSendPipeline struct {
	ch    chan []byte
	errCh chan error
	wg    sync.WaitGroup
}

func newKafkaSendPipeline(
	w *kafka.EventWriter,
	topic string,
	partition int,
	symbol string,
	queueCap int,
	chunk int,
) *kafkaSendPipeline {
	if queueCap < 256 {
		queueCap = 256
	}
	p := &kafkaSendPipeline{
		ch:    make(chan []byte, queueCap),
		errCh: make(chan error, 1),
	}
	buf := newCommandBuffer(w, topic, partition, symbol, chunk)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for body := range p.ch {
			if err := buf.append(body); err != nil {
				p.sendErr(err)
				return
			}
		}
		if err := buf.flush(); err != nil {
			p.sendErr(err)
		}
	}()
	return p
}

func (p *kafkaSendPipeline) sendErr(err error) {
	select {
	case p.errCh <- err:
	default:
	}
}

// firstErr 返回 writer 侧已上报的首个错误。
func (p *kafkaSendPipeline) firstErr() error {
	select {
	case err := <-p.errCh:
		return err
	default:
		return nil
	}
}

func (p *kafkaSendPipeline) enqueue(ctx context.Context, body []byte) error {
	if err := p.firstErr(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.ch <- body:
		return nil
	}
}

func (p *kafkaSendPipeline) closeAndWait() error {
	close(p.ch)
	p.wg.Wait()
	return p.firstErr()
}

// queueCapForRate 发送队列容量：约 2s 积压，避免 writer 短暂抖动时阻塞限速循环。
func queueCapForRate(rate int) int {
	cap := rate * 2
	if cap < 4096 {
		return 4096
	}
	if cap > 65536 {
		return 65536
	}
	return cap
}
