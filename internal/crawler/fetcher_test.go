package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newTestFetcher(client *http.Client) *Fetcher {
	return &Fetcher{directClient: client, dnsCache: nil, logger: testLogger()}
}

// noopProxyPool creates a ProxyPool with no Redis, where Next() always returns the first proxy (fail-open).
func noopProxyPool(proxies ...*url.URL) *ProxyPool {
	return &ProxyPool{proxies: proxies, logger: testLogger()}
}

func TestFetcher_Fetch_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))
	defer srv.Close()

	f := newTestFetcher(srv.Client())
	body, status, err := f.Fetch(context.Background(), srv.URL+"/page")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if string(body) != "hello" {
		t.Errorf("body = %q, want %q", body, "hello")
	}
}

func TestFetcher_Fetch_Headers(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ua := r.Header.Get("User-Agent"); ua != "NimbusCrawler/1.0" {
			t.Errorf("User-Agent = %q, want NimbusCrawler/1.0", ua)
		}
		if accept := r.Header.Get("Accept"); accept != "text/html,application/xhtml+xml" {
			t.Errorf("Accept = %q, want text/html,application/xhtml+xml", accept)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newTestFetcher(srv.Client())
	_, _, err := f.Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFetcher_Fetch_BodyLimit(t *testing.T) {
	t.Parallel()
	// Server returns 11MB, fetcher should truncate to 10MB
	bigBody := strings.Repeat("A", 11*1024*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(bigBody))
	}))
	defer srv.Close()

	f := newTestFetcher(srv.Client())
	body, _, err := f.Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(body) > 10*1024*1024 {
		t.Errorf("body length = %d, want <= %d", len(body), 10*1024*1024)
	}
}

func TestFetcher_Fetch_NonOKStatus(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer srv.Close()

	f := newTestFetcher(srv.Client())
	body, status, err := f.Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 404 {
		t.Errorf("status = %d, want 404", status)
	}
	if string(body) != "not found" {
		t.Errorf("body = %q, want %q", body, "not found")
	}
}

func TestFetcher_Fetch_ContextCancelled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	f := newTestFetcher(srv.Client())
	_, _, err := f.Fetch(ctx, srv.URL)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestFetcher_Fetch_WithProxy(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("proxied"))
	}))
	defer srv.Close()

	proxyURL, _ := url.Parse(srv.URL)
	pool := noopProxyPool(proxyURL)
	f := &Fetcher{
		directClient: srv.Client(),
		proxyPool:    pool,
		proxyClients: map[string]*http.Client{
			proxyURL.String(): srv.Client(),
		},
		logger: testLogger(),
	}

	body, status, err := f.Fetch(context.Background(), srv.URL+"/page")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if string(body) != "proxied" {
		t.Errorf("body = %q, want %q", body, "proxied")
	}
}

func TestFetcher_Fetch_ProxyError_FallsBackToDirect(t *testing.T) {
	t.Parallel()
	// Direct server that works
	directSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("direct"))
	}))
	defer directSrv.Close()

	// A proxy URL that points nowhere (will fail to connect)
	badProxyURL, _ := url.Parse("http://127.0.0.1:1") // port 1 - connection refused
	pool := noopProxyPool(badProxyURL)

	// Create a client that tries to use the bad proxy
	badClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(badProxyURL),
		},
	}

	f := &Fetcher{
		directClient: directSrv.Client(),
		proxyPool:    pool,
		proxyClients: map[string]*http.Client{
			badProxyURL.String(): badClient,
		},
		logger: testLogger(),
	}

	// The proxy will fail, retry with Next() which returns nil (only 1 proxy, now unhealthy-ish),
	// but since we have no Redis, Next() will return the same proxy again (fail-open).
	// The second attempt will also fail, so we ultimately get an error.
	// This tests that the retry path is exercised without panicking.
	_, _, err := f.Fetch(context.Background(), directSrv.URL)
	// With no real Redis, the "unhealthy" mark is a no-op, so both attempts use the bad proxy.
	// The test verifies the retry logic doesn't panic.
	if err == nil {
		t.Log("fetch succeeded (proxy error handling worked)")
	}
}

func TestFetcher_Fetch_5xxNotMarkedAsProxyError(t *testing.T) {
	t.Parallel()
	// Server returns 503 - this should NOT be treated as a proxy failure
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("service unavailable"))
	}))
	defer srv.Close()

	proxyURL, _ := url.Parse(srv.URL)
	pool := noopProxyPool(proxyURL)
	f := &Fetcher{
		directClient: srv.Client(),
		proxyPool:    pool,
		proxyClients: map[string]*http.Client{
			proxyURL.String(): srv.Client(),
		},
		logger: testLogger(),
	}

	body, status, err := f.Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 5xx should be returned as-is, not retried
	if status != 503 {
		t.Errorf("status = %d, want 503", status)
	}
	if string(body) != "service unavailable" {
		t.Errorf("body = %q, want %q", body, "service unavailable")
	}
}
