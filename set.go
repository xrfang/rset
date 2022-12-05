package rset

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
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

func (s *Set) Has(x uint32) bool {
	return s.rbm.Contains(x)
}

func (s *Set) Load(r io.Reader) error {
	s.Lock()
	defer s.Unlock()
	s.rbm.Clear()
	s.lst.clear()
	zr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return err
		}
		switch hdr.Name {
		case "rbm":
			if _, err = s.rbm.ReadFrom(tr); err != nil {
				return err
			}
		case "lst":
			if err = s.lst.load(tr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Set) Save(w io.Writer) (err error) {
	s.Lock()
	defer s.Unlock()
	zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	defer func() {
		ce := zw.Close()
		if err == nil {
			err = ce
		}
	}()
	tw := tar.NewWriter(zw)
	defer func() {
		ce := tw.Close()
		if err == nil {
			err = ce
		}
	}()
	save := func(fn string, bs *bytes.Buffer) error {
		hdr := &tar.Header{
			Name: fn,
			Mode: 0644,
			Size: int64(bs.Len()),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err := io.Copy(tw, bs)
		return err
	}
	var bs bytes.Buffer
	if _, err = s.rbm.WriteTo(&bs); err != nil {
		return
	}
	if err = save("rbm", &bs); err != nil {
		return
	}
	if len(s.lst.data) > 0 {
		bs.Reset()
		if err = s.lst.save(&bs); err != nil {
			return
		}
		return save("lst", &bs)
	}
	return nil
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

func (s *Set) Rank(offset int, ids []uint32) int {
	if len(ids) == 0 {
		return 0
	}
	size := len(s.lst.data)
	if offset >= size {
		return 0
	}
	if s.lst.rank {
		s.RLock()
		defer s.RUnlock()
	} else {
		s.Lock()
		defer s.Unlock()
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

func (s *Set) Range(last uint32, ids []uint32) int {
	s.RLock()
	m := s.rbm.Clone()
	s.RUnlock()
	m.RemoveRange(0, uint64(last)+1)
	return m.ManyIterator().NextMany(ids)
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
