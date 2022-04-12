package main

import (
	"crypto/sha512"
	"hash"
)

type Hasher struct {
	Underlying hash.Hash
}

func (h *Hasher) Hash(input string) ([]byte, error) {
	h.Underlying.Reset()
	_, err := h.Underlying.Write([]byte(input))
	if err != nil {
		return nil, err
	}
	hash := h.Underlying.Sum(nil)
	return hash, nil
}

func (h *Hasher) Size() int {
	return h.Underlying.Size()
}

// New Hasher implementation using SHA-512 from crypto std package
func NewSha512Hasher() Hasher {
	return NewCustomHasher(sha512.New())
}

// New Hasher using any hash.Hash implementation
func NewCustomHasher(underlying hash.Hash) Hasher {
	return Hasher{Underlying: underlying}
}
