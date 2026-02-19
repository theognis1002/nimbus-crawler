package robots

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
	"github.com/theognis1002/nimbus-crawler/internal/database/models"
)

const (
	robotsCacheTTL  = 1 * time.Hour
	robotsKeyPrefix = "robots:"

	// CrawlerName is the user-agent token for robots.txt group matching.
	CrawlerName = "NimbusCrawler"
	// CrawlerUserAgent is the full User-Agent header value.
	CrawlerUserAgent = "NimbusCrawler/1.0"

	// DefaultCrawlDelayMs is the fallback crawl delay when no Crawl-Delay directive exists.
	DefaultCrawlDelayMs = 200
	// MinCrawlDelayMs is the floor applied to any parsed Crawl-Delay.
	MinCrawlDelayMs = 100

	robotsFetchTimeout = 2 * time.Second
	maxRobotsBodySize  = 512 * 1024 // 512KB
)

type Checker struct {
	pool   *pgxpool.Pool
	rdb    *redis.Client
	client *http.Client
	logger *slog.Logger
}

func NewChecker(pool *pgxpool.Pool, rdb *redis.Client, logger *slog.Logger) *Checker {
	return &Checker{
		pool:   pool,
		rdb:    rdb,
		client: &http.Client{Timeout: robotsFetchTimeout},
		logger: logger,
	}
}

func (c *Checker) IsAllowed(ctx context.Context, rawURL, domain string) (bool, int, error) {
	robotsBody, crawlDelay, err := c.getRobotsText(ctx, domain)
	if err != nil {
		c.logger.Warn("failed to get robots.txt, allowing", "domain", domain, "error", err)
		return true, DefaultCrawlDelayMs, nil
	}

	if robotsBody == "" {
		return true, crawlDelay, nil
	}

	robots, err := robotstxt.FromString(robotsBody)
	if err != nil {
		c.logger.Warn("failed to parse robots.txt, allowing", "domain", domain, "error", err)
		return true, crawlDelay, nil
	}

	group := robots.FindGroup(CrawlerName)
	if group == nil {
		group = robots.FindGroup("*")
	}

	// group.Test expects a path, not a full URL
	testPath := rawURL
	if parsed, err := url.Parse(rawURL); err == nil {
		testPath = parsed.RequestURI()
	}

	return group.Test(testPath), crawlDelay, nil
}

func (c *Checker) cacheRobotsHash(ctx context.Context, key, body string, delay int) {
	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key, "body", body, "delay", strconv.Itoa(delay))
	pipe.Expire(ctx, key, robotsCacheTTL)
	_, _ = pipe.Exec(ctx)
}

func (c *Checker) getRobotsText(ctx context.Context, domain string) (string, int, error) {
	key := robotsKeyPrefix + domain

	// Try Redis hash cache — returns both body and delay in one call
	cached, err := c.rdb.HGetAll(ctx, key).Result()
	if err == nil && len(cached) > 0 {
		delay := DefaultCrawlDelayMs
		if d, parseErr := strconv.Atoi(cached["delay"]); parseErr == nil {
			delay = d
		}
		return cached["body"], delay, nil
	}
	if err != nil && err != redis.Nil {
		// Key exists but is wrong type (e.g. leftover string from old cache format).
		// Delete the stale key and fall through to re-fetch.
		c.logger.Warn("deleting stale robots cache key", "key", key, "error", err)
		_ = c.rdb.Del(ctx, key).Err()
	}

	// Try DB
	domRec, err := models.GetDomain(ctx, c.pool, domain)
	if err == nil && domRec.RobotsTxt != nil {
		c.cacheRobotsHash(ctx, key, *domRec.RobotsTxt, domRec.CrawlDelayMs)
		return *domRec.RobotsTxt, domRec.CrawlDelayMs, nil
	}
	if err != nil && err != pgx.ErrNoRows {
		return "", DefaultCrawlDelayMs, fmt.Errorf("db get domain: %w", err)
	}

	// Fetch from remote
	robotsURL := fmt.Sprintf("https://%s/robots.txt", domain)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		_ = models.UpsertDomain(ctx, c.pool, domain, DefaultCrawlDelayMs)
		return "", DefaultCrawlDelayMs, nil
	}
	req.Header.Set("User-Agent", CrawlerUserAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		_ = models.UpsertDomain(ctx, c.pool, domain, DefaultCrawlDelayMs)
		c.cacheRobotsHash(ctx, key, "", DefaultCrawlDelayMs)
		return "", DefaultCrawlDelayMs, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_ = models.UpsertDomain(ctx, c.pool, domain, DefaultCrawlDelayMs)
		c.cacheRobotsHash(ctx, key, "", DefaultCrawlDelayMs)
		return "", DefaultCrawlDelayMs, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRobotsBodySize))
	if err != nil {
		return "", DefaultCrawlDelayMs, fmt.Errorf("reading robots.txt: %w", err)
	}

	robotsBody := string(body)
	crawlDelay := extractCrawlDelay(robotsBody)

	_ = models.UpsertDomainWithRobots(ctx, c.pool, domain, robotsBody, crawlDelay)
	c.cacheRobotsHash(ctx, key, robotsBody, crawlDelay)

	return robotsBody, crawlDelay, nil
}

func extractCrawlDelay(robotsBody string) int {
	robots, err := robotstxt.FromString(robotsBody)
	if err != nil {
		return DefaultCrawlDelayMs
	}

	group := robots.FindGroup(CrawlerName)
	if group == nil {
		group = robots.FindGroup("*")
	}

	if group.CrawlDelay > 0 {
		delay := int(group.CrawlDelay.Milliseconds())
		if delay < MinCrawlDelayMs {
			delay = MinCrawlDelayMs
		}
		return delay
	}

	return DefaultCrawlDelayMs
}
