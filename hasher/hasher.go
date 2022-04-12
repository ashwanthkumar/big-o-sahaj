package hasher

import "crypto/sha512"

type Hasher interface {
	Hash(input string) ([]byte, error)
	Size() int
}

type Sha512Hasher struct{}

func (h *Sha512Hasher) Hash(input string) ([]byte, error) {
	hasher := sha512.New()
	_, err := hasher.Write([]byte(input))
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (h *Sha512Hasher) Size() int {
	return sha512.Size
}

func NewSha512Hasher() Hasher {
	return &Sha512Hasher{}
}
