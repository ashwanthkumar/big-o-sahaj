package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
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

	// extsort.Options.Dedupe should be nil, if it's not then we drop the duplicate key values
	// if we move to start hash value as the key then it might cause problems.
	sorter := extsort.New(&extsort.Options{})
	defer sorter.Close()
	tempBufferForEncodingValue := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(tempBufferForEncodingValue)
	for tuple := range m.data.IterBuffered() {
		encoder.Encode(tuple.Val)
		sorter.Put([]byte(tuple.Key), tempBufferForEncodingValue.Bytes())
		tempBufferForEncodingValue.Reset()
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
	var startKey []byte
	var lastKey []byte
	// Writing the Data
	sstWriter := bufio.NewWriterSize(sstFile, 8*4096)
	for iter.Next() {
		if len(startKey) == 0 {
			startKey = iter.Key()
		}
		binary.Write(sstWriter, binary.LittleEndian, uint32(len(iter.Key())))
		binary.Write(sstWriter, binary.LittleEndian, uint32(len(iter.Value())))
		sstWriter.Write(iter.Key())
		sstWriter.Write(iter.Value())
		lastKey = iter.Key()
	}

	// Writing the metadata (smallest Key and the largest key)
	indexOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	indexOffset += int64(sstWriter.Size())
	encoder = gob.NewEncoder(sstWriter)
	encoder.Encode(FileMetadata{StartKey: startKey, LastKey: lastKey})
	// write the offset where we have the index at the end of the file
	// this should ideally be a fixed size footer that contains offsets
	// to various parts of the file for exact seeking and querying the
	// required information.
	binary.Write(sstWriter, binary.LittleEndian, indexOffset)

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
