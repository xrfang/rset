package rset

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
)

type (
	Item struct {
		ID    uint32
		Score float32
	}
	items struct {
		data []Item
		rank bool
	}
)

func (is *items) sortByID() {
	if !is.rank { //already sorted by ID
		return
	}
	sort.Slice(is.data, func(i, j int) bool {
		return is.data[i].ID <= is.data[j].ID
	})
	is.rank = false
}

func (is *items) sortByScore() {
	if is.rank { //already sorted by score
		return
	}
	sort.Slice(is.data, func(i, j int) bool {
		if is.data[i].Score == is.data[j].Score {
			return is.data[i].ID < is.data[j].ID
		}
		return is.data[i].Score < is.data[j].Score
	})
	is.rank = true
}

func (is *items) clear() {
	is.data = nil
	is.rank = false
}

func (is *items) add(key uint32, val float32) {
	if is.rank { //currently sorted by val
		is.sortByID()
	}
	found := false
	i := sort.Search(len(is.data), func(i int) bool {
		if is.data[i].ID == key {
			found = true
			return true
		}
		return key < is.data[i].ID
	})
	if found {
		is.data[i].Score = val
	} else {
		is.data = append(is.data, Item{})
		copy(is.data[i+1:], is.data[i:])
		is.data[i] = Item{ID: key, Score: val}
	}
}

func (is *items) val(key uint32) (float32, bool) {
	if is.rank {
		is.sortByID()
	}
	found := false
	i := sort.Search(len(is.data), func(i int) bool {
		if is.data[i].ID == key {
			found = true
			return true
		}
		return key < is.data[i].ID
	})
	if found {
		return is.data[i].Score, true
	}
	return 0, false
}

func (is *items) remove(key uint32) bool {
	if is.rank { //currently sorted by val
		is.sortByID()
	}
	found := false
	n := len(is.data)
	i := sort.Search(n, func(i int) bool {
		if is.data[i].ID == key {
			found = true
			return true
		}
		return key < is.data[i].ID
	})
	if !found {
		return false
	}
	copy(is.data[i:], is.data[i+1:])
	is.data = is.data[:n-1]
	return true
}

func (is *items) save(w io.Writer) error {
	if len(is.data) == 0 {
		return nil
	}
	if is.rank { //serialized data always sort by key!
		is.sortByID()
	}
	buf := make([]byte, 8)
	for _, it := range is.data {
		binary.LittleEndian.PutUint32(buf, it.ID)
		binary.LittleEndian.PutUint32(buf[4:], math.Float32bits(it.Score))
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n != 8 {
			return fmt.Errorf("writing 8 bytes, wrote %d bytes", n)
		}
	}
	return nil
}

func (is *items) load(r io.Reader) error {
	var data []Item
	buf := make([]byte, 8)
	for {
		_, err := io.ReadFull(r, buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		data = append(data, Item{
			ID:    binary.LittleEndian.Uint32(buf),
			Score: math.Float32frombits(binary.LittleEndian.Uint32(buf[4:])),
		})
	}
	is.data = data
	is.rank = false
	return nil
}
