package crawler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/theognis1002/nimbus-crawler/internal/cache"
	"github.com/theognis1002/nimbus-crawler/internal/robots"
)

const (
	maxBodyBytes        = 10 * 1024 * 1024 // 10MB
	dialTimeout         = 10 * time.Second
	maxIdleConns        = 100
	maxIdleConnsPerHost = 10
	idleConnTimeout     = 90 * time.Second
	acceptHeader        = "text/html,application/xhtml+xml"
)

type Fetcher struct {
	directClient *http.Client
	proxyClients map[string]*http.Client
	proxyPool    *ProxyPool
	dnsCache     *cache.DNSCache
	logger       *slog.Logger
}

func NewFetcher(dnsCache *cache.DNSCache, proxyPool *ProxyPool, timeoutSecs, maxRedirects int, logger *slog.Logger) *Fetcher {
	dialer := &net.Dialer{Timeout: dialTimeout}
	timeout := time.Duration(timeoutSecs) * time.Second

	directTransport := &http.Transport{
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
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}

	checkRedirect := func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		return nil
	}

	directClient := &http.Client{
		Transport:     directTransport,
		Timeout:       timeout,
		CheckRedirect: checkRedirect,
	}

	f := &Fetcher{
		directClient: directClient,
		dnsCache:     dnsCache,
		proxyPool:    proxyPool,
		logger:       logger,
	}

	if proxyPool != nil {
		f.proxyClients = make(map[string]*http.Client, proxyPool.Len())
		for _, proxyURL := range proxyPool.proxies {
			transport := &http.Transport{
				Proxy:                 http.ProxyURL(proxyURL),
				MaxIdleConns:          maxIdleConns,
				MaxIdleConnsPerHost:   maxIdleConnsPerHost,
				IdleConnTimeout:       idleConnTimeout,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 15 * time.Second,
			}
			f.proxyClients[proxyURL.String()] = &http.Client{
				Transport:     transport,
				Timeout:       timeout,
				CheckRedirect: checkRedirect,
			}
		}
	}

	return f
}

func (f *Fetcher) Fetch(ctx context.Context, rawURL string) ([]byte, int, error) {
	if f.proxyPool == nil {
		return f.doFetch(ctx, rawURL, f.directClient)
	}

	proxy := f.proxyPool.Next(ctx)
	if proxy == nil {
		f.logger.WarnContext(ctx, "all proxies unhealthy, falling back to direct", "url", rawURL)
		return f.doFetch(ctx, rawURL, f.directClient)
	}

	client, ok := f.proxyClients[proxy.String()]
	if !ok {
		f.logger.ErrorContext(ctx, "no http client for proxy", "proxy", proxy.Redacted())
		return f.doFetch(ctx, rawURL, f.directClient)
	}

	body, status, err := f.doFetch(ctx, rawURL, client)
	if err != nil {
		f.proxyPool.MarkUnhealthy(ctx, proxy)
		f.logger.WarnContext(ctx, "proxy failed, retrying with next", "proxy", proxy.Redacted(), "url", rawURL, "error", err)

		nextProxy := f.proxyPool.Next(ctx)
		if nextProxy == nil {
			return f.doFetch(ctx, rawURL, f.directClient)
		}
		nextClient, ok := f.proxyClients[nextProxy.String()]
		if !ok {
			return f.doFetch(ctx, rawURL, f.directClient)
		}
		return f.doFetch(ctx, rawURL, nextClient)
	}

	return body, status, nil
}

func (f *Fetcher) doFetch(ctx context.Context, rawURL string, client *http.Client) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("User-Agent", robots.CrawlerUserAgent)
	req.Header.Set("Accept", acceptHeader)

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("fetching %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		mediaType, _, _ := mime.ParseMediaType(ct)
		if mediaType != "" && !strings.HasPrefix(mediaType, "text/") && mediaType != "application/xhtml+xml" {
			return nil, resp.StatusCode, fmt.Errorf("unexpected content-type %q for %s", ct, rawURL)
		}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("reading response body: %w", err)
	}

	return body, resp.StatusCode, nil
}
