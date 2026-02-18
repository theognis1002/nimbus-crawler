package parser

import (
	"crypto/sha256"
	"fmt"
)

func ContentHash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}
