package main

import (
	"hash"

	"github.com/cespare/xxhash/v2"
)

type Hasher struct {
	CreateHash func() hash.Hash64
}

func (h *Hasher) Hash(input []byte) (uint64, error) {
	hasher := h.CreateHash()
	_, err := hasher.Write([]byte(input))
	if err != nil {
		return 0, err
	}
	hash := hasher.Sum64()
	return hash, nil
}

// New Hasher implementation using SHA-512 from crypto std package
func NewXXHash() Hasher {
	return NewCustomHasher(func() hash.Hash64 { return xxhash.New() })
}

// New Hasher using any hash.Hash implementation
func NewCustomHasher(underlying func() hash.Hash64) Hasher {
	return Hasher{CreateHash: underlying}
}
