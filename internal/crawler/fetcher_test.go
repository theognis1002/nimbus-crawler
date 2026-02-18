package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestFetcher(client *http.Client) *Fetcher {
	return &Fetcher{client: client, dnsCache: nil}
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
