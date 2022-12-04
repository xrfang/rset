package rset

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type (
	Iterator func(uint32) bool
	Set      struct {
		rbm *roaring.Bitmap
		lst items
		sync.RWMutex
	}
)

func New() *Set {
	return &Set{rbm: roaring.New()}
}

func (s *Set) Add(v ...uint32) {
	s.Lock()
	defer s.Unlock()
	s.rbm.AddMany(v)
}

func (s *Set) Insert(key uint32, val float32) {
	s.Lock()
	defer s.Unlock()
	s.rbm.Add(key)
	s.lst.add(key, val)
}

func (s *Set) Remove(key uint32) {
	s.Lock()
	defer s.Unlock()
	s.rbm.Remove(key)
	s.lst.remove(key)
}

func (s *Set) Clear() {
	s.Lock()
	defer s.Unlock()
	s.rbm.Clear()
	s.lst.clear()
}

func (s *Set) Clone() *Set {
	s.Lock()
	defer s.Unlock()
	ns := Set{rbm: s.rbm.Clone()}
	ns.lst.data = make([]Item, len(s.lst.data))
	copy(ns.lst.data, s.lst.data)
	ns.lst.rank = s.lst.rank
	return &ns
}

func (s *Set) Iterate(ranked bool, f Iterator) {
	s.Lock()
	defer s.Unlock()
	if ranked {
		if !s.lst.rank {
			s.lst.sortByScore()
		}
		for _, it := range s.lst.data {
			if !f(it.ID) {
				return
			}
		}
	} else {
		s.rbm.Iterate(f)
	}
}

/*
TODO:
- 完成Range
- Set的load/save
- Set的testcase
- Export（思考是否需要）
*/
func (s *Set) Range(ranked bool, offset int, ids []uint32) int {
	s.Lock()
	defer s.Unlock()
	if len(ids) == 0 {
		return 0
	}
	if ranked {
		size := len(s.lst.data)
		if offset >= size {
			return 0
		}
		if !s.lst.rank {
			s.lst.sortByScore()
		}
		n := 0
		for i := offset; i < size; i++ {
			x := i - offset
			if x >= len(ids) {
				break
			}
			ids[x] = s.lst.data[i].ID
			n++
		}
		return n
	}
	//TODO: get slice from rbm...
}

func (s *Set) Count() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.rbm.GetCardinality()
}

func (s *Set) resort() {
	var list items
	for _, it := range s.lst.data {
		if s.rbm.Contains(it.ID) {
			list.add(it.ID, it.Score)
		}
	}
	s.lst = list
}

func (s *Set) And(s2 *Set) {
	s.Lock()
	defer s.Unlock()
	s.rbm.And(s2.rbm)
	s.resort()
}

func (s *Set) AndNot(s2 *Set) {
	s.Lock()
	defer s.Unlock()
	s.rbm.AndNot(s2.rbm)
	s.resort()
}

func (s *Set) AndAny(sets ...*Set) {
	s.Lock()
	defer s.Unlock()
	var rbms []*roaring.Bitmap
	for _, t := range sets {
		rbms = append(rbms, t.rbm)
	}
	s.rbm.AndAny(rbms...)
	s.resort()
}

func (s *Set) Or(s2 *Set) {
	s.Lock()
	defer s.Unlock()
	s.rbm.Or(s2.rbm)
	s.lst.clear()
}

func (s *Set) Xor(s2 *Set) {
	s.Lock()
	defer s.Unlock()
	s.rbm.Xor(s2.rbm)
	s.lst.clear()
}
