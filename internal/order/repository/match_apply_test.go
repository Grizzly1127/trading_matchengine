package repository

import "testing"

func TestFilledQuantityFromRemaining(t *testing.T) {
	got, err := FilledQuantityFromRemaining("1", "0.4")
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.6" {
		t.Fatalf("filled=%q want 0.6", got)
	}
}
