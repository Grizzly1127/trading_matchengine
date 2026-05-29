package interval

import "testing"

func TestBucketStartMs(t *testing.T) {
	tradeMs := int64(1_700_000_037_500)
	got := Min1.BucketStartMs(tradeMs)
	want := tradeMs - tradeMs%Min1.DurationMs()
	if got != want {
		t.Fatalf("bucket start: got %d want %d", got, want)
	}
}

func TestParse(t *testing.T) {
	iv, err := Parse("1m")
	if err != nil || iv != Min1 {
		t.Fatalf("parse 1m: %v %s", err, iv)
	}
	if _, err := Parse("2m"); err == nil {
		t.Fatal("expected error for 2m")
	}
}
