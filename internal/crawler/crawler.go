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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database/models"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/robots"
	"github.com/michaelmcclelland/nimbus-crawler/internal/storage"
	amqp "github.com/rabbitmq/amqp091-go"
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

func (c *Crawler) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
	var wg sync.WaitGroup

	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID, deliveries)
		}(i)
	}

	wg.Wait()
	c.logger.Info("all crawler workers stopped")
}

func (c *Crawler) worker(ctx context.Context, id int, deliveries <-chan amqp.Delivery) {
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

func (c *Crawler) processMessage(ctx context.Context, logger *slog.Logger, d amqp.Delivery) {
	var msg queue.URLMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		logger.Error("failed to unmarshal message", "error", err)
		_ = d.Nack(false, false)
		return
	}

	logger = logger.With("url", msg.URL, "depth", msg.Depth)

	if msg.Depth > c.cfg.MaxDepth {
		logger.Debug("max depth exceeded, skipping")
		_ = d.Ack(false)
		return
	}

	// Check if already crawled
	existing, err := models.GetURLByURL(ctx, c.pool, msg.URL)
	if err != nil && err != pgx.ErrNoRows {
		logger.Error("failed to check url status", "error", err)
		_ = d.Nack(false, true)
		return
	}
	if existing != nil && (existing.Status == "crawled" || existing.Status == "parsed") {
		logger.Debug("already crawled, skipping")
		_ = d.Ack(false)
		return
	}

	parsed, err := url.Parse(msg.URL)
	if err != nil {
		logger.Error("invalid url", "error", err)
		_ = d.Ack(false)
		return
	}
	domain := parsed.Hostname()

	// Ensure domain exists
	if err := models.UpsertDomain(ctx, c.pool, domain, 1000); err != nil {
		logger.Error("failed to upsert domain", "domain", domain, "error", err)
		_ = d.Nack(false, true)
		return
	}

	// Check robots.txt
	allowed, crawlDelay, err := c.robotsCheck.IsAllowed(ctx, msg.URL, domain)
	if err != nil {
		logger.Warn("robots check failed", "error", err)
	}
	if !allowed {
		logger.Debug("disallowed by robots.txt")
		if existing != nil {
			_ = models.UpdateURLStatus(ctx, c.pool, existing.ID, "skipped")
		}
		_ = d.Ack(false)
		return
	}

	// Rate limit
	if err := c.rateLimiter.WaitForAllow(ctx, domain, crawlDelay); err != nil {
		logger.Error("rate limiter error", "error", err)
		_ = d.Nack(false, true)
		return
	}

	// Ensure URL record exists
	var urlID string
	if existing != nil {
		urlID = existing.ID
	} else {
		urlID, err = models.InsertURL(ctx, c.pool, msg.URL, domain, msg.Depth)
		if err != nil {
			logger.Error("failed to insert url", "error", err)
			_ = d.Nack(false, true)
			return
		}
		if urlID == "" {
			// Race: another worker inserted it
			_ = d.Ack(false)
			return
		}
	}

	_ = models.UpdateURLStatus(ctx, c.pool, urlID, "crawling")

	// Fetch
	body, statusCode, err := c.fetcher.Fetch(ctx, msg.URL)
	if err != nil || statusCode != http.StatusOK {
		logger.Warn("fetch failed", "error", err, "status", statusCode)
		retryCount, _ := models.IncrementRetryCount(ctx, c.pool, urlID)
		if retryCount >= c.cfg.MaxRetries {
			_ = models.UpdateURLStatus(ctx, c.pool, urlID, "failed")
			_ = d.Nack(false, false) // send to DLQ
		} else {
			timer := time.NewTimer(backoffDuration(retryCount))
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
			_ = d.Nack(false, true) // requeue
		}
		return
	}

	// Store HTML in MinIO
	s3Key := storage.HTMLKey(msg.URL)
	if err := c.minio.PutObject(ctx, storage.HTMLBucket, s3Key, body, "text/html"); err != nil {
		logger.Error("failed to store html", "error", err)
		_ = d.Nack(false, true)
		return
	}

	s3Link := fmt.Sprintf("%s/%s", storage.HTMLBucket, s3Key)
	if err := models.UpdateURLCrawled(ctx, c.pool, urlID, s3Link); err != nil {
		logger.Error("failed to update url record", "error", err)
		_ = d.Nack(false, true)
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
		_ = d.Nack(false, true)
		return
	}

	logger.Info("crawled successfully")
	_ = d.Ack(false)
}
