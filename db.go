package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type DB struct {
	// TODO: Add options for dir, and other required params
	lastTimeTs uint32
	rwmutex    sync.RWMutex
	dir        string
	memstore   *Memstore2
	// we need this while the recently full memstore is being flushed,
	// it would not be available for querying from the disk. This value
	// can be nil, so you might want to guard it with that.
	prevMemstore *Memstore2
}

func (db *DB) monitorMemtable() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		// TODO: make 300K configurable
		if db.memstore.numRecords > 300_000 {
			fmt.Println("Flushing the current memstore and creating a new one")
			newTime := atomic.AddUint32(&db.lastTimeTs, 1)
			db.rwmutex.Lock()
			newMemstore, err := NewMemstore2(db.dir, newTime)
			if err != nil {
				panic(err)
			}
			db.prevMemstore = db.memstore
			db.memstore = newMemstore
			db.lastTimeTs = newTime
			db.rwmutex.Unlock()

			db.rwmutex.RLock()
			db.prevMemstore.Finish()
			db.rwmutex.RUnlock()

			// we need this because Get tries to read it from here which can use SEG faults
			// in some unique race condition case.
			db.rwmutex.Lock()
			db.prevMemstore = nil // for GC
			db.rwmutex.Unlock()
		}
	}
}

var (
	ErrKeyNotFound = fmt.Errorf("requested key is not found")
)

func OpenDb(dir string) (*DB, error) {
	// TODO: Today we fail if the dir doesn't exist, we might probably
	// want to create it if it doesn't exist.
	memstore, err := NewMemstore2(dir, 0)
	if err != nil {
		return nil, err
	}

	db := DB{
		dir:      dir,
		memstore: memstore,
	}
	go db.monitorMemtable()

	return &db, nil
}

// TODO: Basic Memstore-only based lookup
func (db *DB) Get(key string) (ValueStruct, bool) {
	db.rwmutex.RLock()
	defer db.rwmutex.RUnlock()
	value, present := db.memstore.Get(key)
	if !present && db.prevMemstore != nil {
		value, present = db.prevMemstore.Get(key)
		return value, present
	}
	return value, present
}

// Write a Key,Value into the KV Store
func (db *DB) Put(key string, value ValueStruct) error {
	db.rwmutex.RLock()
	defer db.rwmutex.RUnlock()
	return db.memstore.Set(key, value)
}
