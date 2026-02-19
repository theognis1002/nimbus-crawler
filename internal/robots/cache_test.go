package robots

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCacheRobotsHash_RoundTrip(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	c := &Checker{rdb: rdb, logger: testLogger()}
	key := robotsKeyPrefix + "example.com"

	c.cacheRobotsHash(context.Background(), key, "User-agent: *\nDisallow: /private\n", 500)

	cached, err := rdb.HGetAll(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if cached["body"] != "User-agent: *\nDisallow: /private\n" {
		t.Errorf("body = %q", cached["body"])
	}
	if cached["delay"] != "500" {
		t.Errorf("delay = %q, want 500", cached["delay"])
	}
}

func TestWRONGTYPE_DeletesStaleKeyAndRecoverOnRecache(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	key := robotsKeyPrefix + "wrongtype.com"

	// Step 1: Set key as a string (simulating old cache format)
	mr.Set(key, "stale-string-value")
	typ, _ := rdb.Type(context.Background(), key).Result()
	if typ != "string" {
		t.Fatalf("precondition: expected string type, got %s", typ)
	}

	// Step 2: HGetAll on a string key returns WRONGTYPE error
	_, err := rdb.HGetAll(context.Background(), key).Result()
	if err == nil {
		t.Fatal("expected WRONGTYPE error from HGetAll on string key")
	}

	// Step 3: Simulate what getRobotsText now does — delete the stale key
	c := &Checker{rdb: rdb, logger: testLogger()}
	_ = c.rdb.Del(context.Background(), key).Err()

	exists, _ := rdb.Exists(context.Background(), key).Result()
	if exists != 0 {
		t.Fatal("stale key should have been deleted")
	}

	// Step 4: Re-cache as a hash — this should work now
	c.cacheRobotsHash(context.Background(), key, "User-agent: *\nDisallow: /\n", 1000)

	cached, err := rdb.HGetAll(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("HGetAll after re-cache: %v", err)
	}
	if cached["body"] != "User-agent: *\nDisallow: /\n" {
		t.Errorf("body = %q after re-cache", cached["body"])
	}
	if cached["delay"] != "1000" {
		t.Errorf("delay = %q, want 1000", cached["delay"])
	}

	// Step 5: Verify HGetAll works on the re-cached hash (no WRONGTYPE)
	cached2, err := rdb.HGetAll(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("second HGetAll: %v", err)
	}
	if len(cached2) == 0 {
		t.Error("expected non-empty hash after re-cache")
	}
}

func TestGetRobotsText_HashCacheHit(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	c := &Checker{rdb: rdb, logger: testLogger()}
	key := robotsKeyPrefix + "cached.com"

	// Pre-populate the hash cache
	c.cacheRobotsHash(context.Background(), key, "User-agent: *\nDisallow: /secret\n", 750)

	body, delay, err := c.getRobotsText(context.Background(), "cached.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body != "User-agent: *\nDisallow: /secret\n" {
		t.Errorf("body = %q", body)
	}
	if delay != 750 {
		t.Errorf("delay = %d, want 750", delay)
	}
}

func TestGetRobotsText_EmptyCacheHitReturnsDefault(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	c := &Checker{rdb: rdb, logger: testLogger()}
	key := robotsKeyPrefix + "empty.com"

	// Cache an empty body with default delay
	c.cacheRobotsHash(context.Background(), key, "", DefaultCrawlDelayMs)

	body, delay, err := c.getRobotsText(context.Background(), "empty.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body != "" {
		t.Errorf("body = %q, want empty", body)
	}
	if delay != DefaultCrawlDelayMs {
		t.Errorf("delay = %d, want %d", delay, DefaultCrawlDelayMs)
	}
}

func TestIsAllowed_CachedDisallow(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	c := &Checker{rdb: rdb, logger: testLogger()}
	key := robotsKeyPrefix + "blocked.com"

	c.cacheRobotsHash(context.Background(), key, "User-agent: *\nDisallow: /admin/\n", 500)

	// Full URL — IsAllowed extracts the path for robots.txt matching
	allowed, delay, err := c.IsAllowed(context.Background(), "https://blocked.com/admin/page", "blocked.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("expected disallowed for /admin/page")
	}
	if delay != 500 {
		t.Errorf("delay = %d, want 500", delay)
	}

	// Allowed path (also as full URL)
	allowed, _, err = c.IsAllowed(context.Background(), "https://blocked.com/public", "blocked.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("expected allowed for /public")
	}
}

func TestIsAllowed_FullURLWithQueryParams(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	c := &Checker{rdb: rdb, logger: testLogger()}
	key := robotsKeyPrefix + "query.com"

	c.cacheRobotsHash(context.Background(), key, "User-agent: *\nDisallow: /search\n", DefaultCrawlDelayMs)

	// Full URL with query params
	allowed, _, err := c.IsAllowed(context.Background(), "https://query.com/search?q=test", "query.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("expected disallowed for /search?q=test")
	}

	// Allowed path
	allowed, _, err = c.IsAllowed(context.Background(), "https://query.com/about", "query.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("expected allowed for /about")
	}
}
