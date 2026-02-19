package cache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestIsPrivateIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		ip      string
		private bool
	}{
		{"loopback v4", "127.0.0.1", true},
		{"loopback v6", "::1", true},
		{"private 10.x", "10.0.0.1", true},
		{"private 172.16.x", "172.16.0.1", true},
		{"private 192.168.x", "192.168.1.1", true},
		{"link-local v4", "169.254.1.1", true},
		{"link-local v6", "fe80::1", true},
		{"unspecified v4", "0.0.0.0", true},
		{"unspecified v6", "::", true},
		{"public v4", "93.184.216.34", false},
		{"public v6", "2606:2800:220:1:248:1893:25c8:1946", false},
		{"invalid string", "not-an-ip", true},
		{"empty string", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isPrivateIP(tt.ip)
			if got != tt.private {
				t.Errorf("isPrivateIP(%q) = %v, want %v", tt.ip, got, tt.private)
			}
		})
	}
}

func TestLookupHost_CacheHit(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	dc := NewDNSCache(rdb)

	mr.Set("dns:example.com", "93.184.216.34")

	ip, err := dc.LookupHost(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "93.184.216.34" {
		t.Errorf("got %q, want %q", ip, "93.184.216.34")
	}
}

func TestLookupHost_CacheHitPrivateIPRejected(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	dc := NewDNSCache(rdb)

	mr.Set("dns:evil.com", "192.168.1.1")

	_, err := dc.LookupHost(context.Background(), "evil.com")
	if err == nil {
		t.Fatal("expected error for private IP, got nil")
	}
}

func TestLookupHost_CacheMiss_PrivateIPRejected(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	dc := NewDNSCache(rdb)

	// "localhost" resolves to 127.0.0.1 which is private
	_, err := dc.LookupHost(context.Background(), "localhost")
	if err == nil {
		t.Fatal("expected error for private IP resolution, got nil")
	}

	// Should not cache private IPs
	if mr.Exists("dns:localhost") {
		t.Error("private IP should not be cached")
	}
}

func TestLookupHost_RedisError(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	dc := NewDNSCache(rdb)

	mr.Close()

	_, err := dc.LookupHost(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected error when Redis is down, got nil")
	}
}
