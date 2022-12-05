package rset

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestSetAdd(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		s.Add(uint32(i * 10))
	}
	for i := 0; i < 10; i++ {
		if s.Has(uint32(1000 + i)) {
			t.Fatalf("%d is in set", 1000+i)
		}
		if !s.Has(uint32(i * 10)) {
			t.Fatalf("%d is not in set", i*10)
		}
	}
}

func TestSetRange(t *testing.T) {
	s := New()
	for i := 1; i < 10; i++ {
		s.Add(uint32(i))
	}
	buf := make([]uint32, 3)
	var last uint32
	s.Range(last, buf)
	if buf[0] != 1 || buf[1] != 2 || buf[2] != 3 {
		t.Fatalf("expected 1~3, got: %v", buf)
	}
	last = buf[len(buf)-1]
	s.Range(last, buf)
	if buf[0] != 4 || buf[1] != 5 || buf[2] != 6 {
		t.Fatalf("expected 4~6, got: %v", buf)
	}
}

func TestSetRank(t *testing.T) {
	s := New()
	for i := 1; i < 10; i++ {
		s.Insert(uint32(i), float32(10-i))
	}
	buf := make([]uint32, 3)
	s.Rank(0, buf)
	if buf[0] != 9 || buf[1] != 8 || buf[2] != 7 {
		t.Fatalf("expected 9~7, got: %v", buf)
	}
	if cnt := s.Rank(7, buf); cnt != 2 {
		t.Fatalf("exptected 2 items, got %d", cnt)
	}
	if buf[0] != 2 || buf[1] != 1 {
		t.Fatalf("expected 2~1, got: %v", buf)
	}
}

func TestSetSaveLoad(t *testing.T) {
	s1 := New()
	for i := 1; i < 4; i++ {
		s1.Insert(uint32(i), float32(4-i))
	}
	var bs bytes.Buffer
	s1.Save(&bs)
	s2 := New()
	s2.Load(&bs)
	b := make([]uint32, 1)
	offset := 0
	s1.Iterate(true, func(u uint32) bool {
		s2.Rank(offset, b)
		if b[0] != u {
			t.Fatalf("load/save mismatch: original=%v; loaded=%v", u, b[0])
		}
		offset++
		return true
	})
}

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
