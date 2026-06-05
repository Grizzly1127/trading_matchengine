package recovery_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
)

func TestEngine_groupCommitBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.WALGroupCommit = recovery.WALGroupCommitConfig{SyncEveryRecords: 4}

	eng, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	if !eng.GroupCommitEnabled() {
		t.Fatal("group commit expected enabled")
	}

	for i := uint64(1); i <= 3; i++ {
		if err := eng.StageNewOrder(limitSell(i, "100", "1")); err != nil {
			t.Fatal(err)
		}
	}
	if eng.LastSeq() != 3 {
		t.Fatalf("lastSeq=%d want 3 before commit", eng.LastSeq())
	}

	out, err := eng.CommitBatch()
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("outcomes=%d want 3", len(out))
	}
	if eng.LastSeq() != 3 {
		t.Fatalf("lastSeq=%d want 3 after commit", eng.LastSeq())
	}
}
