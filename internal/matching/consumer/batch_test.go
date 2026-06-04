package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/consumer"
)

func TestRunOptions_batchedRequiresEngine(t *testing.T) {
	// 仅验证 Run 对 nil handler 报错；完整 batch 集成见 recovery/group_commit_test。
	err := consumer.Run(context.Background(), nil, nil, consumer.RunOptions{BatchMax: 8, BatchWait: time.Millisecond})
	if err == nil {
		t.Fatal("expected error")
	}
}
