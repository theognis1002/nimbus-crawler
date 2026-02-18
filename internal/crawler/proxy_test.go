package crawler

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestNewProxyPool_EmptyPath(t *testing.T) {
	t.Parallel()
	pool, err := NewProxyPool("", nil, 60, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool != nil {
		t.Error("expected nil pool for empty path")
	}
}

func TestNewProxyPool_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := NewProxyPool("/nonexistent/proxies.txt", nil, 60, testLogger())
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestNewProxyPool_EmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	os.WriteFile(path, []byte("# only comments\n\n"), 0644)

	_, err := NewProxyPool(path, nil, 60, testLogger())
	if err == nil {
		t.Error("expected error for empty proxy file")
	}
}

func TestNewProxyPool_InvalidURL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	os.WriteFile(path, []byte("not-a-valid-url\n"), 0644)

	_, err := NewProxyPool(path, nil, 60, testLogger())
	if err == nil {
		t.Error("expected error for invalid proxy URL")
	}
}

func TestNewProxyPool_ValidFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	content := "# comment\nhttp://proxy1.example.com:8080\nhttps://proxy2.example.com:8443\n\n"
	os.WriteFile(path, []byte(content), 0644)

	pool, err := NewProxyPool(path, nil, 60, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool.Len() != 2 {
		t.Errorf("Len() = %d, want 2", pool.Len())
	}
}

func TestProxyPool_Next_RoundRobin(t *testing.T) {
	t.Parallel()
	u1, _ := url.Parse("http://proxy1.example.com:8080")
	u2, _ := url.Parse("http://proxy2.example.com:8080")
	u3, _ := url.Parse("http://proxy3.example.com:8080")

	pool := &ProxyPool{
		proxies: []*url.URL{u1, u2, u3},
		logger:  testLogger(),
	}

	// Without Redis, Next falls through the Exists error path and returns healthy.
	// This tests round-robin rotation with no Redis (fail-open).
	ctx := context.Background()
	seen := make(map[string]int)
	for i := 0; i < 6; i++ {
		proxy := pool.Next(ctx)
		if proxy == nil {
			t.Fatal("expected non-nil proxy")
		}
		seen[proxy.Host]++
	}

	// Each proxy should have been returned twice in 6 calls
	for _, u := range []*url.URL{u1, u2, u3} {
		if seen[u.Host] != 2 {
			t.Errorf("proxy %s called %d times, want 2", u.Host, seen[u.Host])
		}
	}
}

func TestProxyPool_Len(t *testing.T) {
	t.Parallel()
	u1, _ := url.Parse("http://proxy1.example.com:8080")
	pool := &ProxyPool{proxies: []*url.URL{u1}, logger: testLogger()}
	if pool.Len() != 1 {
		t.Errorf("Len() = %d, want 1", pool.Len())
	}
}
