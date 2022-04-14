package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/bsm/extsort"
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
	fileTs       uint32 // atomic updates
	numRecords   uint64 // atomic updates

	flushTimer *time.Ticker
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
	atomic.AddUint64(&m.numRecords, 1)
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

func (m *Memstore2) Finish() {
	m.flushTimer.Stop()
	m.wal.Sync()
	m.wal.Close()
	// write
	sorter := extsort.New(&extsort.Options{})
	defer sorter.Close()
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)
	for tuple := range m.data.IterBuffered() {
		encoder.Encode(tuple.Val)
		sorter.Put([]byte(tuple.Key), buf.Bytes())
		buf.Reset()
	}
	iter, err := sorter.Sort()
	if err != nil {
		panic(err)
	}
	defer iter.Close()
	sstFile, err := os.Create(path.Join(path.Dir(m.wal.Name()), fmt.Sprintf("%05d.sst", m.fileTs)))
	if err != nil {
		panic(err)
	}
	defer sstFile.Close()
	sstWriter := bufio.NewWriterSize(sstFile, 8*4096)
	for iter.Next() {
		binary.Write(sstWriter, binary.LittleEndian, uint32(len(iter.Key())))
		binary.Write(sstWriter, binary.LittleEndian, uint32(len(iter.Value())))
		sstWriter.Write(iter.Key())
		sstWriter.Write(iter.Value())
	}
	err = sstWriter.Flush()
	if err != nil {
		panic(err)
	}
	err = os.Remove(m.wal.Name())
	if err != nil {
		panic(err)
	}
}

func (m *Memstore2) StartBackgroundFlush() {
	m.flushTimer = time.NewTicker(1 * time.Second)
	for range m.flushTimer.C {
		info, err := m.wal.Stat()
		if err == nil {
			fmt.Println(int(info.Size()/1024/1024), "MB")
		} else {
			fmt.Println("[ERROR]", err.Error())
		}
		m.wal.Sync()
	}
}

func NewMemstore2(dir string, lastFileTs uint32) (*Memstore2, error) {
	newFileTs := atomic.AddUint32(&lastFileTs, 1)
	walFilePath := path.Join(dir, fmt.Sprintf("%05d.wal", newFileTs))
	wal, err := os.Create(walFilePath)
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
