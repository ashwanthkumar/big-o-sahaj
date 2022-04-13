package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test64BitXXHash(t *testing.T) {
	hasher := NewXXHash()
	hash, err := hasher.Hash([]byte("Hello World"))
	assert.NoError(t, err)

	expected := uint64(0x6334d20719245bc2)
	assert.Equal(t, expected, hash)
}
