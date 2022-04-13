package main

import (
	"bytes"

	"github.com/huandu/skiplist"
)

type Memstore struct {
	// skipList stores Uint64 (hash) -> skipList(key,value)
	items  skiplist.SkipList
	Hasher *Hasher
}

func (m *Memstore) Put(key []byte, value []byte) error {
	hash, err := m.Hasher.Hash(key)
	if err != nil {
		return err
	}
	item, present := m.items.GetValue(hash)
	if !present {
		item = skiplist.New(skiplist.Bytes)
	}
	existingSkipList := item.(*skiplist.SkipList)
	// TODO: we'll move the value from skiplist to vLog and store the vlog pointer in here instead
	existingSkipList.Set(key, value)
	m.items.Set(hash, existingSkipList)
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

func NewMemstore(hasher *Hasher) Memstore {
	return Memstore{
		items:  *skiplist.New(skiplist.Uint64),
		Hasher: hasher,
	}
}
