package main

import (
	"sync"
	"sync/atomic"

	"github.com/huandu/skiplist"
)

type Memstore struct {
	Hasher *Hasher

	rwLock sync.RWMutex
	// items: Uint64 (hash) -> skipList(key,value)
	items      skiplist.SkipList
	memorySize uint64 // use atomic to change the value
}

func (m *Memstore) Put(key []byte, value []byte) error {
	hash, err := m.Hasher.Hash(key)
	if err != nil {
		return err
	}
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

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
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	item, present := m.items.GetValue(hash)
	if !present {
		return nil, ErrKeyNotFound
	}
	existingSkipList := item.(*skiplist.SkipList)
	item, present = existingSkipList.GetValue(key)
	if !present {
		return nil, ErrKeyNotFound
	}
	return item.([]byte), nil
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
