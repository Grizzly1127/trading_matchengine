package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

type mockConsumer struct {
	msgs []kafka.Message
	idx  int
}

func (m *mockConsumer) Read(ctx context.Context) (kafka.Message, error) {
	if m.idx >= len(m.msgs) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := m.msgs[m.idx]
	m.idx++
	return msg, nil
}

func (m *mockConsumer) Commit(context.Context, kafka.Message) error { return nil }
func (m *mockConsumer) Close() error                                { return nil }

type countingProcessor struct {
	calls int
}

func (p *countingProcessor) Process(context.Context, kafka.Message) error {
	p.calls++
	return nil
}

func TestRun_CanceledBeforeRead(t *testing.T) {
	c := &mockConsumer{msgs: []kafka.Message{{Offset: 1}}}
	p := &countingProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Run(ctx, c, p); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if p.calls != 0 {
		t.Fatalf("calls=%d want 0", p.calls)
	}
}

func TestRun_ProcessesOneMessage(t *testing.T) {
	c := &mockConsumer{msgs: []kafka.Message{{Offset: 10}}}
	p := &countingProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, c, p) }()

	deadline := time.Now().Add(2 * time.Second)
	for p.calls < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if p.calls != 1 {
		t.Fatalf("calls=%d want 1", p.calls)
	}
}

type staleProcessor struct {
	calls int
}

func (p *staleProcessor) Process(context.Context, kafka.Message) error {
	p.calls++
	return fmt.Errorf("%w: 99", repository.ErrOrderNotFound)
}

func TestRun_SkipsStaleOrderNotFound(t *testing.T) {
	c := &mockConsumer{msgs: []kafka.Message{{Offset: 1}, {Offset: 2}}}
	p := &staleProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, c, p) }()

	deadline := time.Now().Add(2 * time.Second)
	for p.calls < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if p.calls != 2 {
		t.Fatalf("calls=%d want 2", p.calls)
	}
	if c.idx != 2 {
		t.Fatalf("idx=%d want 2 (both committed)", c.idx)
	}
}
