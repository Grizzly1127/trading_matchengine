package snapshot

import (
	"path/filepath"
	"testing"
	"time"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

func sampleSnapshot(seq uint64) *matchingv1.Snapshot {
	return &matchingv1.Snapshot{
		ShardId:           "shard-0",
		Symbol:            "BTC-USDT",
		SeqId:             seq,
		TimestampUnixNano: time.Unix(100, 0).UnixNano(),
		Bids: []*matchingv1.PriceLevel{
			{
				Price: &commonv1.Decimal{Value: "99"},
				Orders: []*commonv1.Order{
					{
						OrderId:  1,
						Symbol:   "BTC-USDT",
						Side:     commonv1.Side_SIDE_BUY,
						Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
						Price:    &commonv1.Decimal{Value: "99"},
						Quantity: &commonv1.Decimal{Value: "1"},
					},
				},
			},
		},
		Asks: []*matchingv1.PriceLevel{
			{
				Price: &commonv1.Decimal{Value: "100"},
				Orders: []*commonv1.Order{
					{
						OrderId:  2,
						Symbol:   "BTC-USDT",
						Side:     commonv1.Side_SIDE_SELL,
						Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
						Price:    &commonv1.Decimal{Value: "100"},
						Quantity: &commonv1.Decimal{Value: "2"},
					},
				},
			},
		},
		OrderMap: map[uint64]*commonv1.Order{
			1: {OrderId: 1, Symbol: "BTC-USDT"},
			2: {OrderId: 2, Symbol: "BTC-USDT"},
		},
	}
}

func TestSaveLoad_roundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, Filename(100))
	want := sampleSnapshot(100)

	if err := Save(path, want); err != nil {
		t.Fatal(err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSeqId() != want.GetSeqId() || got.GetSymbol() != want.GetSymbol() {
		t.Fatalf("got = %+v", got)
	}
	if got.GetChecksum() == 0 {
		t.Fatal("expected checksum to be set on save")
	}
}

func TestLoad_checksumMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, Filename(1))
	snap := sampleSnapshot(1)
	snap.Checksum = 0 // 故意错误 checksum

	if err := writeAtomic(path, mustMarshal(t, snap)); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != ErrChecksum {
		t.Fatalf("err = %v, want %v", err, ErrChecksum)
	}
}

func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSaveManifestLoadManifest_roundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.pb")
	want := &matchingv1.ShardManifest{
		ShardId:           "shard-0",
		RecoveredOffset:   42,
		SymbolSeq:         map[string]uint64{"BTC-USDT": 42},
		UpdatedAtUnixNano: time.Now().UnixNano(),
	}

	if err := SaveManifest(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetRecoveredOffset() != 42 || got.GetSymbolSeq()["BTC-USDT"] != 42 {
		t.Fatalf("got = %+v", got)
	}
}

func TestPrune_keepsLatest(t *testing.T) {
	dir := t.TempDir()
	for _, seq := range []uint64{10, 20, 30, 40} {
		if err := Save(filepath.Join(dir, Filename(seq)), sampleSnapshot(seq)); err != nil {
			t.Fatal(err)
		}
	}
	if err := Prune(dir, 2); err != nil {
		t.Fatal(err)
	}

	paths, err := listSnapshotPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("len = %d, want 2", len(paths))
	}

	latest, ok, err := LatestPath(dir)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if filepath.Base(latest) != Filename(40) {
		t.Fatalf("latest = %s", latest)
	}
}

func TestLatestPath_emptyDir(t *testing.T) {
	_, ok, err := LatestPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no snapshot")
	}
}

func TestVerifyChecksum_detectsTamperedBids(t *testing.T) {
	snap := sampleSnapshot(1)
	snap.Checksum = ComputeChecksum(snap)
	snap.Bids[0].Price.Value = "98"
	if err := VerifyChecksum(snap); err != ErrChecksum {
		t.Fatalf("err = %v", err)
	}
}
