package main

import (
	"crypto/sha512"
	"hash"
)

type Hasher struct {
	CreateHash func() hash.Hash
}

func (h *Hasher) Hash(input string) ([]byte, error) {
	hasher := h.CreateHash()
	_, err := hasher.Write([]byte(input))
	if err != nil {
		return nil, err
	}
	hash := hasher.Sum(nil)
	return hash, nil
}

func (h *Hasher) Size() int {
	return h.CreateHash().Size()
}

// New Hasher implementation using SHA-512 from crypto std package
func NewSha512Hasher() Hasher {
	return NewCustomHasher(func() hash.Hash { return sha512.New() })
}

// New Hasher using any hash.Hash implementation
func NewCustomHasher(underlying func() hash.Hash) Hasher {
	return Hasher{CreateHash: underlying}
}
