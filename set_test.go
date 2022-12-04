package rset

import (
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func BenchmarkSetAdd(b *testing.B) {
	b.StopTimer()
	s := New()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Add(uint32(i))
	}
}

func BenchmarkSetInsert(b *testing.B) {
	b.StopTimer()
	rand.Seed(0)
	s := New()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Insert(uint32(i), rand.Float32())
	}
}

func BenchmarkRoarAdd(b *testing.B) {
	b.StopTimer()
	r := roaring.New()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		r.Add(uint32(i))
	}
}
