package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAllow_FirstRequestAllowed(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb)

	allowed, err := rl.Allow(context.Background(), "example.com", 1000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("first request should be allowed")
	}
}

func TestAllow_SecondRequestBlocked(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb)

	allowed, err := rl.Allow(context.Background(), "example.com", 60000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Fatal("first request should be allowed")
	}

	allowed, err = rl.Allow(context.Background(), "example.com", 60000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("second request should be blocked within window")
	}
}

func TestAllow_WindowExpiry(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb)

	allowed, err := rl.Allow(context.Background(), "example.com", 100, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Fatal("first request should be allowed")
	}

	time.Sleep(150 * time.Millisecond)

	allowed, err = rl.Allow(context.Background(), "example.com", 100, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("request after window expiry should be allowed")
	}
}

func TestAllow_LimitGreaterThanOne(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb)

	for i := 0; i < 3; i++ {
		allowed, err := rl.Allow(context.Background(), "example.com", 60000, 3)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed (limit=3)", i)
		}
	}

	allowed, err := rl.Allow(context.Background(), "example.com", 60000, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("fourth request should be blocked (limit=3)")
	}
}

func TestWaitForAllow_ContextCancellation(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb)

	// Exhaust the limit
	_, err := rl.Allow(context.Background(), "example.com", 60000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = rl.WaitForAllow(ctx, "example.com", 60000)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestWaitForAllow_AllowedImmediately(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb)

	err := rl.WaitForAllow(context.Background(), "example.com", 1000)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
