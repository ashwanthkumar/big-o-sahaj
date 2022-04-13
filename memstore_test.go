package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestSimpleAddAndGetOnMemstore(t *testing.T) {
	hasher := NewXXHash()
	memstore := NewMemstore(&hasher)
	err := memstore.Put([]byte("Hello"), []byte("World"))
	assert.NoError(t, err)
	value, err := memstore.Get([]byte("Hello"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("World"), value)
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
