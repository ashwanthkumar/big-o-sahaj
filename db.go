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
			oldMemstore := db.memstore
			db.memstore = newMemstore
			db.lastTimeTs = newTime
			db.rwmutex.Unlock()

			oldMemstore.Finish()
			oldMemstore = nil // for GC
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

// Get a value as []byte if it exists, else ErrKeyNotFound is returned
func (db *DB) Get(input string) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

// Write a Key,Value into the KV Store
func (db *DB) Put(key string, value ValueStruct) error {
	db.rwmutex.RLock()
	defer db.rwmutex.RUnlock()
	return db.memstore.Set(key, value)
}
