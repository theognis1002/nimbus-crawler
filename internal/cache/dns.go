package cache

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	dnsTTL       = 5 * time.Minute
	dnsKeyPrefix = "dns:"
)

type DNSCache struct {
	client *redis.Client
}

func NewDNSCache(client *redis.Client) *DNSCache {
	return &DNSCache{client: client}
}

func (d *DNSCache) LookupHost(ctx context.Context, host string) (string, error) {
	key := dnsKeyPrefix + host

	cached, err := d.client.Get(ctx, key).Result()
	if err == nil {
		if isPrivateIP(cached) {
			return "", fmt.Errorf("resolved to private IP %s for host %s", cached, host)
		}
		return cached, nil
	}
	if err != redis.Nil {
		return "", fmt.Errorf("redis get dns: %w", err)
	}

	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return "", fmt.Errorf("dns lookup %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("no addresses for %s", host)
	}

	ip := addrs[0]

	if isPrivateIP(ip) {
		return "", fmt.Errorf("resolved to private IP %s for host %s", ip, host)
	}

	if err := d.client.Set(ctx, key, ip, dnsTTL).Err(); err != nil {
		return ip, nil // return IP even if caching fails
	}

	return ip, nil
}

// isPrivateIP returns true if the IP is loopback, private, link-local, or otherwise not a public address.
func isPrivateIP(ip string) bool {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return true // reject unparseable
	}
	return addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsUnspecified()
}
