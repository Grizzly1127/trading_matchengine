package status

import "testing"

func TestTargetStatus(t *testing.T) {
	got, err := TargetStatus(MatchOrderAccepted)
	if err != nil || got != Accepted {
		t.Fatalf("accepted: %q %v", got, err)
	}
}

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to string
		ok       bool
	}{
		{Pending, Accepted, true},
		{Accepted, Partial, true},
		{Partial, Partial, true},
		{Partial, Filled, true},
		{Pending, Rejected, true},
		{Pending, Canceling, true},
		{Accepted, Canceling, true},
		{Partial, Canceling, true},
		{Canceling, Canceled, true},
		{Canceling, Accepted, true},
		{Canceling, Partial, true},
		{Canceling, Filled, true},
		{Filled, Canceled, false}, // FILLED 终态后忽略 CANCELED 事件（在 ApplyMatchEvent 中 skip）
		{Filled, Canceling, false},
		{Canceled, Accepted, false},
	}
	for _, c := range cases {
		if CanTransition(c.from, c.to) != c.ok {
			t.Fatalf("%s -> %s want %v", c.from, c.to, c.ok)
		}
	}
}

func TestFillWinsOverCancel(t *testing.T) {
	if !FillWinsOverCancel(Canceling, Filled) {
		t.Fatal("expected fill wins over cancel")
	}
	if FillWinsOverCancel(Canceling, Canceled) {
		t.Fatal("cancel event is not fill progress")
	}
	if FillWinsOverCancel(Accepted, Filled) {
		t.Fatal("only applies from CANCELING")
	}
}

func TestIsTerminal(t *testing.T) {
	if !IsTerminal(Filled) || IsTerminal(Partial) {
		t.Fatal("terminal check failed")
	}
}
