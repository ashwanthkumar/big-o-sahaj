package main

import (
	"bytes"
	"sync/atomic"

	"github.com/huandu/skiplist"
)

type Memstore struct {
	// items: Uint64 (hash) -> skipList(key,value)
	items      skiplist.SkipList
	Hasher     *Hasher
	memorySize uint64 // use atomic to change the value
}

func (m *Memstore) Put(key []byte, value []byte) error {
	hash, err := m.Hasher.Hash(key)
	if err != nil {
		return err
	}
	item, present := m.items.GetValue(hash)
	hashSize := 0 // we assume the hash size is already accounted for
	if !present {
		item = skiplist.New(skiplist.Bytes)
		hashSize = 64
	}
	existingSkipList := item.(*skiplist.SkipList)
	// TODO: we'll move the value from skiplist to vLog and store the vlog pointer in here instead
	existingSkipList.Set(key, value)
	m.items.Set(hash, existingSkipList)
	atomic.AddUint64(&m.memorySize, uint64(len(key)+len(value)+hashSize))
	return nil
}

func (m *Memstore) Get(key []byte) ([]byte, error) {
	hash, err := m.Hasher.Hash(key)
	if err != nil {
		return nil, err
	}
	item, present := m.items.GetValue(hash)
	if !present {
		return nil, ErrKeyNotFound
	}
	existingSkipList := item.(*skiplist.SkipList)
	i := existingSkipList.Front()
	for {
		// exit condition
		if i == nil {
			break
		}
		if bytes.Equal(i.Key().([]byte), key) {
			return i.Value.([]byte), nil
		} else {
			i = i.Next()
		}
	}

	return nil, ErrKeyNotFound
}

func (m *Memstore) MemSize() uint64 {
	return m.memorySize
}

func NewMemstore(hasher *Hasher) Memstore {
	return Memstore{
		items:  *skiplist.New(skiplist.Uint64),
		Hasher: hasher,
	}
}
