package convert

import "testing"

func TestOrderIDRoundTrip(t *testing.T) {
	const id = uint64(18446744073709551615)
	s := OrderIDToString(id)
	got, err := ParseOrderID(s)
	if err != nil {
		t.Fatal(err)
	}
	if got != id {
		t.Fatalf("got %d want %d", got, id)
	}
}

func TestParseOrderID_Invalid(t *testing.T) {
	if _, err := ParseOrderID("abc"); err == nil {
		t.Fatal("expected error")
	}
}
