package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestSimpleAddAndGetOnMemstore(t *testing.T) {
	hasher := NewXXHash()
	memstore := NewMemstore(&hasher)
	key := []byte("Hello")
	value := []byte("World")
	err := memstore.Put(key, value)
	assert.NoError(t, err)
	valueFromMemstore, err := memstore.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, valueFromMemstore)

	expectedMemSize := len(key) + len(value) + 64 /* size of hash in memory */
	assert.Equal(t, uint64(expectedMemSize), memstore.MemSize())
}

func TestReturnErrKeyNotFoundWhenKeyIsNotFound(t *testing.T) {
	hasher := NewXXHash()
	memstore := NewMemstore(&hasher)
	_, err := memstore.Get([]byte("Hello"))
	assert.Equal(t, err, ErrKeyNotFound)
}

func TestMemstoreAddAndGetForKeyAndValueSpecs(t *testing.T) {
	hasher := NewXXHash()
	memstore := NewMemstore(&hasher)

	rapid.Check(t, func(t *rapid.T) {
		key := rapid.StringMatching(`[a-zA-Z0-9]{3,100}`).Draw(t, "addr").(string)
		// 1024 - 1KB, 20480 - 20KB
		value := rapid.StringOfN(rapid.ByteRange(48, 122), 1024, 20480, -1).Draw(t, "value").(string)
		err := memstore.Put([]byte(key), []byte(value))
		assert.NoError(t, err)

		valueFromMemstore, err := memstore.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, []byte(value), valueFromMemstore)
	})
}
