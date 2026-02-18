package cache

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
    redis.call('EXPIRE', key, math.ceil(window / 1000))
    return 1
end
return 0
`)

type RateLimiter struct {
	client *redis.Client
}

func NewRateLimiter(client *redis.Client) *RateLimiter {
	return &RateLimiter{client: client}
}

// Allow checks if a request to the given domain is allowed.
// windowMs is the sliding window size in milliseconds.
// limit is the max number of requests in that window (typically 1).
// Returns true if allowed, false if rate-limited.
func (r *RateLimiter) Allow(ctx context.Context, domain string, windowMs int, limit int) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s", domain)
	now := time.Now().UnixMilli()

	result, err := slidingWindowScript.Run(ctx, r.client, []string{key}, now, windowMs, limit).Int()
	if err != nil {
		return false, fmt.Errorf("rate limit script: %w", err)
	}

	return result == 1, nil
}

// WaitForAllow blocks until the rate limiter allows the request, adding jitter.
func (r *RateLimiter) WaitForAllow(ctx context.Context, domain string, crawlDelayMs int) error {
	for {
		allowed, err := r.Allow(ctx, domain, crawlDelayMs, 1)
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}

		jitter := time.Duration(float64(crawlDelayMs)*0.5*rand.Float64()) * time.Millisecond
		wait := time.Duration(crawlDelayMs)*time.Millisecond/2 + jitter

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}
