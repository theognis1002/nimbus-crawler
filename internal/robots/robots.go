package robots

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database/models"
	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
)

const (
	robotsCacheTTL  = 1 * time.Hour
	robotsKeyPrefix = "robots:"

	// CrawlerName is the user-agent token for robots.txt group matching.
	CrawlerName = "NimbusCrawler"
	// CrawlerUserAgent is the full User-Agent header value.
	CrawlerUserAgent = "NimbusCrawler/1.0"

	// DefaultCrawlDelayMs is the fallback crawl delay when no Crawl-Delay directive exists.
	DefaultCrawlDelayMs = 1000
	// MinCrawlDelayMs is the floor applied to any parsed Crawl-Delay.
	MinCrawlDelayMs = 500

	robotsFetchTimeout = 10 * time.Second
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

	return group.Test(rawURL), crawlDelay, nil
}

func (c *Checker) getRobotsText(ctx context.Context, domain string) (string, int, error) {
	key := robotsKeyPrefix + domain

	cached, err := c.rdb.Get(ctx, key).Result()
	if err == nil {
		domRec, dbErr := models.GetDomain(ctx, c.pool, domain)
		delay := DefaultCrawlDelayMs
		if dbErr == nil {
			delay = domRec.CrawlDelayMs
		}
		return cached, delay, nil
	}
	if err != redis.Nil {
		return "", DefaultCrawlDelayMs, fmt.Errorf("redis get robots: %w", err)
	}

	// Try DB
	domRec, err := models.GetDomain(ctx, c.pool, domain)
	if err == nil && domRec.RobotsTxt != nil {
		_ = c.rdb.Set(ctx, key, *domRec.RobotsTxt, robotsCacheTTL).Err()
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
	resp, err := c.client.Do(req)
	if err != nil {
		_ = models.UpsertDomain(ctx, c.pool, domain, DefaultCrawlDelayMs)
		return "", DefaultCrawlDelayMs, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_ = models.UpsertDomain(ctx, c.pool, domain, DefaultCrawlDelayMs)
		return "", DefaultCrawlDelayMs, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRobotsBodySize))
	if err != nil {
		return "", DefaultCrawlDelayMs, fmt.Errorf("reading robots.txt: %w", err)
	}

	robotsBody := string(body)
	crawlDelay := extractCrawlDelay(robotsBody)

	_ = models.UpsertDomain(ctx, c.pool, domain, crawlDelay)
	_ = models.UpdateDomainRobotsTxt(ctx, c.pool, domain, robotsBody, crawlDelay)
	_ = c.rdb.Set(ctx, key, robotsBody, robotsCacheTTL).Err()

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
