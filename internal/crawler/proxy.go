package crawler

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const proxyHealthKeyPrefix = "proxy:health:"

type ProxyPool struct {
	proxies  []*url.URL
	counter  atomic.Uint64
	rdb      *redis.Client
	cooldown time.Duration
	logger   *slog.Logger
}

// NewProxyPool loads proxies from a file and returns a pool for round-robin selection.
// Returns (nil, nil) if path is empty, meaning no proxy file is configured.
func NewProxyPool(path string, rdb *redis.Client, cooldownSecs int, logger *slog.Logger) (*ProxyPool, error) {
	if path == "" {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening proxy file %s: %w", path, err)
	}
	defer f.Close()

	var proxies []*url.URL
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		u, err := url.Parse(line)
		if err != nil {
			return nil, fmt.Errorf("parsing proxy URL %q: %w", line, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("invalid proxy URL %q: missing scheme or host", line)
		}
		proxies = append(proxies, u)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading proxy file: %w", err)
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("proxy file %s contains no valid proxy URLs", path)
	}

	return &ProxyPool{
		proxies:  proxies,
		rdb:      rdb,
		cooldown: time.Duration(cooldownSecs) * time.Second,
		logger:   logger,
	}, nil
}

// Next returns the next healthy proxy using round-robin selection.
// Returns nil if all proxies are currently in cooldown (caller should fall back to direct).
func (p *ProxyPool) Next(ctx context.Context) *url.URL {
	n := len(p.proxies)
	start := p.counter.Add(1) - 1
	for i := 0; i < n; i++ {
		proxy := p.proxies[(start+uint64(i))%uint64(n)]
		if p.rdb == nil {
			return proxy
		}
		key := proxyHealthKeyPrefix + proxy.String()
		exists, err := p.rdb.Exists(ctx, key).Result()
		if err != nil {
			p.logger.WarnContext(ctx, "redis error checking proxy health, assuming healthy", "proxy", proxy.Redacted(), "error", err)
			return proxy
		}
		if exists == 0 {
			return proxy
		}
	}
	return nil
}

// MarkUnhealthy marks a proxy as unhealthy in Redis with a TTL-based cooldown.
// Uses SetNX so concurrent workers don't reset the TTL.
func (p *ProxyPool) MarkUnhealthy(ctx context.Context, proxy *url.URL) {
	if p.rdb == nil {
		return
	}
	key := proxyHealthKeyPrefix + proxy.String()
	if err := p.rdb.SetNX(ctx, key, "1", p.cooldown).Err(); err != nil {
		p.logger.WarnContext(ctx, "failed to mark proxy unhealthy in redis", "proxy", proxy.Redacted(), "error", err)
	}
}

// Len returns the number of configured proxies.
func (p *ProxyPool) Len() int {
	return len(p.proxies)
}
