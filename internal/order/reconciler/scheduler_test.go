package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type fakeReconcileStore struct {
	pendingIDs   []uint64
	cancelIDs    []uint64
	staleOutbox  int
	rejected     []uint64
	resent       []uint64
}

func (f *fakeReconcileStore) FindStalePendingForReject(_ context.Context, _ time.Time, _ int) ([]uint64, error) {
	return f.pendingIDs, nil
}

func (f *fakeReconcileStore) RejectStalePending(_ context.Context, orderID uint64, _ string) (bool, error) {
	f.rejected = append(f.rejected, orderID)
	return true, nil
}

func (f *fakeReconcileStore) FindStuckCancelingForResend(_ context.Context, _ time.Time, _ int) ([]uint64, error) {
	return f.cancelIDs, nil
}

func (f *fakeReconcileStore) ResendCancelCommand(_ context.Context, orderID uint64, _ string) (bool, error) {
	f.resent = append(f.resent, orderID)
	return true, nil
}

func (f *fakeReconcileStore) CountStaleUnpublishedOutbox(_ context.Context, _ time.Time) (int, error) {
	return f.staleOutbox, nil
}

func TestScheduler_tick(t *testing.T) {
	store := &fakeReconcileStore{
		pendingIDs:  []uint64{1},
		cancelIDs:   []uint64{2},
		staleOutbox: 3,
	}
	s := &Scheduler{
		Store: store,
		Log:   zerolog.Nop(),
		Config: Config{
			Enabled:              true,
			PendingAcceptTimeout: time.Minute,
			CancelConfirmTimeout:   time.Minute,
			OutboxStaleWarn:        time.Minute,
			CommandTopic:           "order.commands",
		},
	}
	s.tick(context.Background(), s.normalizedConfig())

	if len(store.rejected) != 1 || store.rejected[0] != 1 {
		t.Fatalf("rejected=%v", store.rejected)
	}
	if len(store.resent) != 1 || store.resent[0] != 2 {
		t.Fatalf("resent=%v", store.resent)
	}
}
