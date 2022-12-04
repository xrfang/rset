package rset

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestAdd1(t *testing.T) {
	var its items
	for i := 0; i < 3; i++ {
		its.add(uint32(i), float32(-2*i))
	}
	if len(its.data) != 3 {
		t.Fatalf("expect 3 items, got %d", len(its.data))
	}
	t.Log("successfully added 3 items")
	for i, it := range its.data {
		if it.ID != uint32(i) {
			t.Fatalf("key of item #%d should be %d, got %d", i, i, it.ID)
		}
	}
	its.sortByScore()
	if !its.rank {
		t.Fatal("item still appear unranked after sortByVal()")
	}
	for i, it := range its.data {
		expkey := 2 - i
		expval := float32(-2 * expkey)
		if it.ID != uint32(expkey) {
			t.Fatalf("key of item #%d should be %d, got %d", i, expkey, it.ID)
		}
		if it.Score != expval {
			t.Fatalf("val of item #%d should be %f, got %f", i, expval, it.Score)
		}
	}
	t.Log("successfly sorted items by value")
}
func TestAdd2(t *testing.T) {
	var its items
	for i := 0; i < 3; i++ {
		its.add(uint32(i), float32(-2*i))
	}
	its.sortByScore()
	if !its.rank {
		t.Fatalf("items should be ranked")
	}
	for i := 3; i < 6; i++ {
		its.add(uint32(i), float32(-2*i))
	}
	if its.rank {
		t.Fatalf("items should not be ranked after adding new items")
	}
}

func TestVal(t *testing.T) {
	var its items
	for i := 0; i < 3; i++ {
		its.add(uint32(i), float32(i*2))
	}
	its.add(1, math.Pi)
	_, ok := its.val(3)
	if ok {
		t.Fatal("found non-existent key '3'")
	}
	pi, ok := its.val(1)
	if !ok {
		t.Fatal("failed to find value for '1'")
	}
	if pi != math.Pi {
		t.Fatalf("value for '1' is %f, expected %f", pi, math.Pi)
	}
}

func TestRemove(t *testing.T) {
	var its items
	for i := 0; i < 5; i++ {
		its.add(uint32(i), float32(-2*i))
	}
	its.remove(2)
	if len(its.data) != 4 {
		t.Fatalf("should have 4 items left, got %d", len(its.data))
	}
	_, ok := its.val(2)
	if ok {
		t.Fatal("value for '2' still exists after del()")
	}
}

func TestLoadSave(t *testing.T) {
	rand.Seed(time.Now().UnixMicro())
	var it1, it2 items
	for i := 0; i < 3; i++ {
		it1.add(uint32(i), rand.Float32())
	}
	it1.sortByScore()
	var bs bytes.Buffer
	if err := it1.save(&bs); err != nil {
		t.Fatalf("failed to save items: %v", err)
	}
	if it1.rank {
		t.Fatalf("items still ranked after save()")
	}
	if err := it2.load(&bs); err != nil {
		t.Fatalf("failed to load items: %v", err)
	}
	if len(it1.data) != len(it2.data) {
		t.Fatalf("item length mismatch after save/load (%d vs %d)",
			len(it1.data), len(it2.data))
	}
	for i, t1 := range it1.data {
		t2 := it2.data[i]
		if t1.ID != t2.ID || t1.Score != t2.Score {
			t.Fatalf("mismatch: it1=%+v; it2=%+v", it1, it2)
		}
	}
	t.Log("load/save succeeded")
}

func BenchmarkAdd(b *testing.B) {
	b.StopTimer()
	rand.Seed(0)
	var its items
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		its.add(uint32(i), rand.Float32())
	}
}

func BenchmarkAddMap(b *testing.B) {
	b.StopTimer()
	rand.Seed(0)
	itm := make(map[uint32]float32)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		itm[uint32(i)] = rand.Float32()
	}
}
