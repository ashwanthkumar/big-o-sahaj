package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestMemstore2WithWal(t *testing.T) {
	PrintMemUsage()

	lastFileTs := rand.Uint32()
	memstore, err := NewMemstore2("test_db", lastFileTs)
	assert.NoError(t, err)

	keyGen := rapid.StringMatching("[a-zA-Z0-9]{100}")
	filename := "bfa032537a3d8cb1b79d161afe00819f"

	for i := 0; i < 10_000_000; i++ {
		if i%10_000 == 0 {
			fmt.Println("Processed", i, "item")
		}
		value := ValueStruct{
			Filename:  filename,
			Offset:    rand.Int63(),
			Timestamp: time.Now().UnixMilli(),
		}
		memstore.Set(keyGen.Example().(string), value)
	}
	fmt.Println("Done Writing 1M entries to Memstore")
	PrintMemUsage()

	runtime.GC()
	PrintMemUsage()

	memstore.Close()

}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
