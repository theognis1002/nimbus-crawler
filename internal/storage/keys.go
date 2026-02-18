package storage

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
)

const (
	HTMLBucket = "nimbus-html"
	TextBucket = "nimbus-text"
)

// HTMLKey generates an S3 key for raw HTML content.
func HTMLKey(rawURL string) string {
	return objectKey(rawURL, "html")
}

// TextKey generates an S3 key for extracted text content.
func TextKey(rawURL string) string {
	return objectKey(rawURL, "txt")
}

func objectKey(rawURL, ext string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Sprintf("unknown/%s.%s", sanitize(rawURL), ext)
	}

	path := u.Path
	if path == "" || path == "/" {
		path = "/index"
	}
	path = strings.TrimSuffix(path, "/")

	// Include a short hash of the full URL to avoid collisions from query params
	h := sha256.Sum256([]byte(rawURL))
	hashPrefix := fmt.Sprintf("%x", h[:8])

	return fmt.Sprintf("%s%s_%s.%s", u.Host, path, hashPrefix, ext)
}

func sanitize(s string) string {
	r := strings.NewReplacer("/", "_", ":", "_", "?", "_", "&", "_", "=", "_")
	return r.Replace(s)
}
