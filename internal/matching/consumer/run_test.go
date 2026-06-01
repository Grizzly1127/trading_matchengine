package consumer

import "testing"

func TestStartOffset_emptyWAL(t *testing.T) {
	if got := StartOffset(0, false); got != 0 {
		t.Fatalf("StartOffset(0,false)=%d want 0", got)
	}
}

func TestStartOffset_withResume(t *testing.T) {
	if got := StartOffset(10, true); got != 11 {
		t.Fatalf("StartOffset(10,true)=%d want 11", got)
	}
}
