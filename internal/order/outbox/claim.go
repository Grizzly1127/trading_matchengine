package outbox

import (
	"context"
)

// Entry 待投递 Outbox 行。
type Entry struct {
	ID           uint64
	AggregateID  uint64
	EventType    string
	Payload      []byte
	Topic        string
	PartitionKey string
	RetryCount   int
}

// ClaimHandle 在事务内 SKIP LOCKED 领取的一批 Outbox；Commit 前同行对其他 worker 不可见。
type ClaimHandle interface {
	Entries() []Entry
	MarkPublishedBatch(ctx context.Context, ids []uint64) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// emptyClaim 无待投递行时的空 claim。
type emptyClaim struct{}

func (emptyClaim) Entries() []Entry { return nil }

func (emptyClaim) MarkPublishedBatch(context.Context, []uint64) error { return nil }

func (emptyClaim) Commit(context.Context) error { return nil }

func (emptyClaim) Rollback(context.Context) error { return nil }

// EmptyClaim 返回空 claim（测试用）。
func EmptyClaim() ClaimHandle { return emptyClaim{} }
