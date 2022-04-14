package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"time"

	cmap "github.com/orcaman/concurrent-map"
)

type ValueStruct struct {
	Timestamp int64
	Offset    int64
	Filename  string
}

type Entry struct {
	Value ValueStruct
	Key   string
}

type Memstore2 struct {
	data         cmap.ConcurrentMap
	wal          *os.File
	entryEncoder *gob.Encoder
	fileTs       uint32 // atomic access
}

func (m *Memstore2) Set(key string, value ValueStruct) error {
	// write to WAL
	entry := Entry{
		Key:   key,
		Value: value,
	}
	err := m.entryEncoder.Encode(entry)
	if err != nil {
		return err
	}

	// update the data in-memory
	m.data.Set(key, value)
	return nil
}

func (m *Memstore2) Get(key string) (ValueStruct, bool) {
	value, present := m.data.Get(key)
	if present {
		return value.(ValueStruct), present
	} else {
		return ValueStruct{}, present
	}
}

func (m *Memstore2) Close() {
	m.wal.Sync()
	m.wal.Close()
}

func (m *Memstore2) StartBackgroundFlush() {
	for {
		<-time.After(1 * time.Second)
		m.wal.Sync()
	}
}

func NewMemstore2(dir string, lastFileTs uint32) (*Memstore2, error) {
	newFileTs := atomic.AddUint32(&lastFileTs, 1)
	wal, err := os.Create(path.Join(dir, fmt.Sprintf("%05d.wal", newFileTs)))
	if err != nil {
		return nil, err
	}
	memstore := &Memstore2{
		data:         cmap.New(),
		wal:          wal,
		fileTs:       newFileTs,
		entryEncoder: gob.NewEncoder(wal),
	}
	go memstore.StartBackgroundFlush()
	return memstore, nil
}
