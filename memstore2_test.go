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

// Replace the below with TestMemstore2WithWalStress(t testing.T) to actually run the stress test
// You might want to increase the go test timeout accordingly so the stress test can complete
func Memstore2WithWalStressTest(t *testing.T) {
	// func TestMemstore2WithWalStressTest(t *testing.T) {
	const size = 1_000_000
	PrintMemUsage()

	db, err := OpenDb("test_db")
	assert.NoError(t, err)

	keyGen := rapid.StringMatching("[a-zA-Z0-9]{100}")
	filename := "bfa032537a3d8cb1b79d161afe00819f"

	for i := 0; i < size; i++ {
		// if i%10_000 == 0 {
		// fmt.Println("Processed", i, "item")
		// }
		value := ValueStruct{
			Filename:  filename,
			Offset:    rand.Int63(),
			Timestamp: time.Now().UnixMilli(),
		}
		db.Put(keyGen.Example().(string), value)
	}
	fmt.Println("Done Writing", size, "entries to Memstore")
	PrintMemUsage()

	runtime.GC()
	PrintMemUsage()
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
