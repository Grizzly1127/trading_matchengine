package skiplist

import "testing"

func BenchmarkSkipList_insert(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sl := newIntList()
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			sl.Insert(j)
		}
	}
}

func BenchmarkSkipList_insertDelete(b *testing.B) {
	sl := newIntList()
	for j := 0; j < 10_000; j++ {
		sl.Insert(j)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Insert(i)
		sl.Delete(i)
	}
}
