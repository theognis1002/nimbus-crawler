package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/crawler"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/robots"
	"github.com/michaelmcclelland/nimbus-crawler/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load("configs/development.yaml")
	if err != nil {
		logger.Info("config file not found, using env vars", "error", err)
		cfg = config.LoadFromEnv()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := database.NewPool(ctx, cfg.Postgres)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := cache.NewRedisClient(ctx, cfg.Redis)
	if err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	qConn, err := queue.NewConnection(cfg.RabbitMQ.URL(), logger)
	if err != nil {
		logger.Error("failed to connect to rabbitmq", "error", err)
		os.Exit(1)
	}
	defer qConn.Close()

	if err := qConn.SetPrefetch(cfg.Crawler.PrefetchCount); err != nil {
		logger.Error("failed to set prefetch", "error", err)
		os.Exit(1)
	}

	publisher, err := queue.NewPublisher(qConn)
	if err != nil {
		logger.Error("failed to create publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	minioClient, err := storage.NewMinIOClient(ctx, cfg.MinIO)
	if err != nil {
		logger.Error("failed to connect to minio", "error", err)
		os.Exit(1)
	}

	dnsCache := cache.NewDNSCache(rdb)
	rateLimiter := cache.NewRateLimiter(rdb)
	robotsChecker := robots.NewChecker(pool, rdb, logger)
	fetcher := crawler.NewFetcher(dnsCache, cfg.Crawler.TimeoutSecs, cfg.Crawler.MaxRedirects)

	c := crawler.New(cfg.Crawler, pool, fetcher, publisher, rateLimiter, robotsChecker, minioClient, logger)

	deliveries, err := queue.Consume(qConn, queue.FrontierQueue)
	if err != nil {
		logger.Error("failed to start consuming", "error", err)
		os.Exit(1)
	}

	// Monitor RabbitMQ connection; exit on disconnect so container restarts
	go func() {
		err := <-qConn.NotifyClose()
		if err != nil {
			logger.Error("rabbitmq connection lost", "error", err)
		}
		cancel()
	}()

	logger.Info("crawler starting", "workers", cfg.Crawler.Workers, "max_depth", cfg.Crawler.MaxDepth)
	c.Run(ctx, deliveries)
}
