package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/crawler"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database/models"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/robots"
	"github.com/michaelmcclelland/nimbus-crawler/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load("configs/development.yaml")
	if err != nil {
		logger.Debug("config file not found, using env vars", "error", err)
		cfg = config.LoadFromEnv()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := database.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	rdb, err := cache.NewRedisClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer rdb.Close()

	if err := queue.EnsureStreams(ctx, rdb, logger); err != nil {
		return fmt.Errorf("ensure streams: %w", err)
	}

	publisher := queue.NewPublisher(rdb)

	minioClient, err := storage.NewMinIOClient(ctx, cfg.MinIO)
	if err != nil {
		return fmt.Errorf("connect to minio: %w", err)
	}

	dnsCache := cache.NewDNSCache(rdb)
	rateLimiter := cache.NewRateLimiter(rdb)
	robotsChecker := robots.NewChecker(pool, rdb, logger)

	proxyPool, err := crawler.NewProxyPool(cfg.Crawler.Proxy.File, rdb, cfg.Crawler.Proxy.HealthCooldownS, logger)
	if err != nil {
		return fmt.Errorf("load proxy pool: %w", err)
	}
	if proxyPool != nil {
		logger.Info("proxy pool loaded", "count", proxyPool.Len())
	} else {
		logger.Info("no proxy file configured, using direct connections")
	}

	fetcher := crawler.NewFetcher(dnsCache, proxyPool, cfg.Crawler.TimeoutSecs, cfg.Crawler.MaxRedirects, logger)

	count, err := models.ResetStaleCrawlingURLs(ctx, pool, 5*time.Minute)
	if err != nil {
		logger.Error("failed to reset stale crawling urls", "error", err)
	} else if count > 0 {
		logger.Info("reset stale crawling urls", "count", count)
	}

	c := crawler.New(cfg.Crawler, pool, fetcher, publisher, rateLimiter, robotsChecker, minioClient, logger)

	consumerName := fmt.Sprintf("crawler-%d", os.Getpid())
	consumer := queue.NewConsumer(rdb, queue.FrontierStream, queue.FrontierDLQ, queue.CrawlerGroup, consumerName, cfg.Crawler.PrefetchCount, logger)
	deliveries := consumer.Run(ctx)

	logger.Info("crawler starting", "workers", cfg.Crawler.Workers, "max_depth", cfg.Crawler.MaxDepth)
	c.Run(ctx, deliveries)
	consumer.Wait()

	return nil
}
