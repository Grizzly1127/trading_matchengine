package skiplist

import (
	"slices"
	"testing"
)

func intCompare(a, b any) int {
	ak := a.(int)
	bk := b.(int)
	switch {
	case ak < bk:
		return -1
	case ak > bk:
		return 1
	default:
		return 0
	}
}

func newIntList() *SkipList {
	return NewSkipList(intCompare)
}

func TestInsertSearch_andUpdate(t *testing.T) {
	sl := newIntList()

	sl.Insert(10)
	sl.Insert(5)
	sl.Insert(20)

	if sl.Size() != 3 {
		t.Fatalf("size = %d, want 3", sl.Size())
	}

	v, ok := sl.Search(5)
	if !ok || v != 5 {
		t.Fatalf("Search(5) = %v, %v", v, ok)
	}

	// 相同值再次 Insert 视为原地更新，不增加长度
	sl.Insert(10)
	v, ok = sl.Search(10)
	if !ok || v != 10 {
		t.Fatalf("Search(10) after update = %v, %v", v, ok)
	}
	if sl.Size() != 3 {
		t.Fatalf("size after update = %d, want 3", sl.Size())
	}
}

func TestSearch_missing(t *testing.T) {
	sl := newIntList()
	sl.Insert(1)

	_, ok := sl.Search(2)
	if ok {
		t.Fatal("Search(2) should miss")
	}
}

func TestDelete(t *testing.T) {
	sl := newIntList()
	for _, v := range []int{3, 1, 4, 1, 5} {
		sl.Insert(v)
	}
	if sl.Size() != 4 {
		t.Fatalf("after insert size = %d, want 4 (duplicate value 1 updates in place)", sl.Size())
	}

	if !sl.Delete(1) {
		t.Fatal("Delete(1) should succeed")
	}
	if sl.Contains(1) {
		t.Fatal("value 1 should be gone")
	}
	if sl.Size() != 3 {
		t.Fatalf("after delete size = %d, want 3", sl.Size())
	}

	if sl.Delete(99) {
		t.Fatal("Delete(99) should fail")
	}
}

func TestIterator_sortedOrder(t *testing.T) {
	sl := newIntList()
	vals := []int{30, 10, 20, 5, 25}
	for _, v := range vals {
		sl.Insert(v)
	}

	got := make([]int, 0, sl.Size())
	it := sl.Iterator()
	for it.HasNext() {
		v, ok := it.Next()
		if !ok {
			t.Fatal("Next returned false while HasNext was true")
		}
		got = append(got, v.(int))
	}

	want := slices.Clone(vals)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("iterate got %v, want %v", got, want)
	}
}

func TestFront(t *testing.T) {
	sl := newIntList()
	sl.Insert(30)
	sl.Insert(10)
	sl.Insert(20)

	v, ok := sl.Front()
	if !ok || v != 10 {
		t.Fatalf("Front() = %v, %v; want 10, true", v, ok)
	}

	sl.Delete(10)
	v, ok = sl.Front()
	if !ok || v != 20 {
		t.Fatalf("Front() after delete 10 = %v, %v; want 20, true", v, ok)
	}
}

func TestClear_andIsEmpty(t *testing.T) {
	sl := newIntList()
	if !sl.IsEmpty() {
		t.Fatal("new list should be empty")
	}

	sl.Insert(1)
	sl.Clear()

	if !sl.IsEmpty() || sl.Size() != 0 {
		t.Fatalf("after Clear: empty=%v size=%d", sl.IsEmpty(), sl.Size())
	}
	if sl.Contains(1) {
		t.Fatal("value should not exist after Clear")
	}
}

func TestDelete_lowersLevel(t *testing.T) {
	sl := newIntList()
	for i := range 50 {
		sl.Insert(i)
	}
	before := sl.level
	for i := range 50 {
		if !sl.Delete(i) {
			t.Fatalf("Delete(%d) failed", i)
		}
	}
	if !sl.IsEmpty() {
		t.Fatal("expected empty")
	}
	if before < 1 || sl.level != 1 {
		t.Fatalf("level after delete all: %d (before %d), want 1", sl.level, before)
	}
}

func TestContains(t *testing.T) {
	sl := newIntList()
	sl.Insert(42)
	if !sl.Contains(42) || sl.Contains(43) {
		t.Fatal("Contains mismatch")
	}
}

func TestFront_empty(t *testing.T) {
	sl := newIntList()
	_, ok := sl.Front()
	if ok {
		t.Fatal("Front on empty list should return ok=false")
	}
}
