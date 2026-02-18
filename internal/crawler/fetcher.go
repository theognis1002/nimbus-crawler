package crawler

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
)

type Fetcher struct {
	client   *http.Client
	dnsCache *cache.DNSCache
}

func NewFetcher(dnsCache *cache.DNSCache, timeoutSecs, maxRedirects int) *Fetcher {
	dialer := &net.Dialer{Timeout: 10 * time.Second}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return dialer.DialContext(ctx, network, addr)
			}

			ip, err := dnsCache.LookupHost(ctx, host)
			if err != nil {
				return nil, err
			}

			return dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		},
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutSecs) * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			return nil
		},
	}

	return &Fetcher{client: client, dnsCache: dnsCache}
}

func (f *Fetcher) Fetch(ctx context.Context, rawURL string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("User-Agent", "NimbusCrawler/1.0")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("fetching %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("reading response body: %w", err)
	}

	return body, resp.StatusCode, nil
}
