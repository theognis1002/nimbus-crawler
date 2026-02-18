package parser

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestContentHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "known input",
			input: []byte("hello world"),
			want:  fmt.Sprintf("%x", sha256.Sum256([]byte("hello world"))),
		},
		{
			name:  "empty input",
			input: []byte(""),
			want:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ContentHash(tt.input)
			if got != tt.want {
				t.Errorf("ContentHash(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestContentHash_DifferentInputsDifferentHashes(t *testing.T) {
	t.Parallel()
	h1 := ContentHash([]byte("input one"))
	h2 := ContentHash([]byte("input two"))
	if h1 == h2 {
		t.Error("different inputs should produce different hashes")
	}
}

func TestContentHash_Deterministic(t *testing.T) {
	t.Parallel()
	input := []byte("deterministic test")
	h1 := ContentHash(input)
	h2 := ContentHash(input)
	if h1 != h2 {
		t.Error("same input should always produce the same hash")
	}
}
