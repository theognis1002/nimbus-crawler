package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database/models"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/robots"
	"github.com/michaelmcclelland/nimbus-crawler/internal/storage"
)

type Crawler struct {
	cfg         config.CrawlerConfig
	pool        *pgxpool.Pool
	fetcher     *Fetcher
	publisher   *queue.Publisher
	rateLimiter *cache.RateLimiter
	robotsCheck *robots.Checker
	minio       *storage.MinIOClient
	logger      *slog.Logger
	domainCache sync.Map
	retryWg     sync.WaitGroup
}

func New(
	cfg config.CrawlerConfig,
	pool *pgxpool.Pool,
	fetcher *Fetcher,
	publisher *queue.Publisher,
	rateLimiter *cache.RateLimiter,
	robotsCheck *robots.Checker,
	minio *storage.MinIOClient,
	logger *slog.Logger,
) *Crawler {
	return &Crawler{
		cfg:         cfg,
		pool:        pool,
		fetcher:     fetcher,
		publisher:   publisher,
		rateLimiter: rateLimiter,
		robotsCheck: robotsCheck,
		minio:       minio,
		logger:      logger,
	}
}

func (c *Crawler) Run(ctx context.Context, deliveries <-chan queue.Delivery) {
	var wg sync.WaitGroup

	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID, deliveries)
		}(i)
	}

	wg.Wait()
	c.retryWg.Wait()
	c.logger.Info("all crawler workers stopped")
}

func (c *Crawler) worker(ctx context.Context, id int, deliveries <-chan queue.Delivery) {
	logger := c.logger.With("worker", id)
	logger.Info("crawler worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info("crawler worker stopping")
			return
		case d, ok := <-deliveries:
			if !ok {
				logger.Info("delivery channel closed")
				return
			}
			c.processMessage(ctx, logger, d)
		}
	}
}

func (c *Crawler) processMessage(ctx context.Context, logger *slog.Logger, d queue.Delivery) {
	var msg queue.URLMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		logger.Error("failed to unmarshal message", "error", err)
		if err := d.Nack(true); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	logger = logger.With("url", msg.URL, "depth", msg.Depth)

	if msg.Depth > c.cfg.MaxDepth {
		logger.Info("max depth exceeded, skipping")
		if err := d.Ack(); err != nil {
			logger.Error("failed to ack message", "error", err)
		}
		return
	}

	parsed, err := url.Parse(msg.URL)
	if err != nil {
		logger.Error("invalid url", "error", err)
		if err := d.Ack(); err != nil {
			logger.Error("failed to ack message", "error", err)
		}
		return
	}
	domain := parsed.Hostname()

	// Ensure domain exists (skip DB call if already cached in-process)
	if _, loaded := c.domainCache.LoadOrStore(domain, true); !loaded {
		if err := models.UpsertDomain(ctx, c.pool, domain, robots.DefaultCrawlDelayMs); err != nil {
			c.domainCache.Delete(domain)
			logger.Error("failed to upsert domain", "domain", domain, "error", err)
			if err := d.Nack(false); err != nil {
				logger.Error("failed to nack message", "error", err)
			}
			return
		}
	}

	// Single upsert: insert or get existing URL, sets status to 'crawling' on insert
	urlID, status, err := models.UpsertURLReturning(ctx, c.pool, msg.URL, domain, msg.Depth)
	if err != nil {
		logger.Error("failed to upsert url", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}
	if status != models.StatusCrawling {
		logger.Info("url not in crawlable state, skipping", "status", string(status))
		if err := d.Ack(); err != nil {
			logger.Error("failed to ack message", "error", err)
		}
		return
	}

	// Check robots.txt
	crawlDelay := robots.DefaultCrawlDelayMs
	if c.cfg.RespectRobotsTxt == nil || *c.cfg.RespectRobotsTxt {
		allowed, delay, err := c.robotsCheck.IsAllowed(ctx, msg.URL, domain)
		if err != nil {
			logger.Warn("robots check failed", "error", err)
		}
		crawlDelay = delay
		if !allowed {
			logger.Info("disallowed by robots.txt")
			_ = models.UpdateURLStatus(ctx, c.pool, urlID, models.StatusSkipped)
			if err := d.Ack(); err != nil {
				logger.Error("failed to ack message", "error", err)
			}
			return
		}
	} else {
		logger.Debug("robots.txt checking disabled")
	}

	// Rate limit
	if err := c.rateLimiter.WaitForAllow(ctx, domain, crawlDelay); err != nil {
		if ctx.Err() != nil {
			logger.Info("shutting down, requeueing message")
		} else {
			logger.Error("rate limiter error", "error", err)
		}
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	// Fetch
	body, statusCode, err := c.fetcher.Fetch(ctx, msg.URL)
	if err != nil || statusCode != http.StatusOK {
		logger.Warn("fetch failed", "error", err, "status", statusCode)
		retryCount, _ := models.IncrementRetryAndMaybeFailURL(ctx, c.pool, urlID, c.cfg.MaxRetries)
		if retryCount >= c.cfg.MaxRetries {
			if err := d.Nack(true); err != nil {
				logger.Error("failed to nack message to DLQ", "error", err)
			}
		} else {
			// Ack the original and re-publish after backoff delay
			if err := d.Ack(); err != nil {
				logger.Error("failed to ack message for retry", "error", err)
			}
			delay := backoffDuration(retryCount)
			logger.Info("scheduling retry", "retry", retryCount, "delay", delay)
			c.retryWg.Add(1)
			go func() {
				defer c.retryWg.Done()
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
				pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := c.publisher.PublishURL(pubCtx, msg); err != nil {
					logger.Error("failed to re-publish after backoff", "error", err)
				}
			}()
		}
		return
	}

	// Store HTML in MinIO
	s3Key := storage.HTMLKey(msg.URL)
	if err := c.minio.PutObject(ctx, storage.HTMLBucket, s3Key, body, "text/html"); err != nil {
		logger.Error("failed to store html", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	s3Link := fmt.Sprintf("%s/%s", storage.HTMLBucket, s3Key)
	if err := models.UpdateURLCrawled(ctx, c.pool, urlID, s3Link); err != nil {
		logger.Error("failed to update url record", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	// Publish parse message
	parseMsg := queue.ParseMessage{
		URLID:      urlID,
		URL:        msg.URL,
		S3HTMLLink: s3Link,
		Depth:      msg.Depth,
	}
	if err := c.publisher.PublishParse(ctx, parseMsg); err != nil {
		logger.Error("failed to publish parse message", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	logger.Info("crawled successfully")
	if err := d.Ack(); err != nil {
		logger.Error("failed to ack message", "error", err)
	}
}
