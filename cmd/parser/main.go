package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database"
	internalparser "github.com/michaelmcclelland/nimbus-crawler/internal/parser"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
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

	if err := queue.EnsureStreams(ctx, rdb, logger); err != nil {
		logger.Error("failed to ensure streams", "error", err)
		os.Exit(1)
	}

	publisher := queue.NewPublisher(rdb)
	defer publisher.Close()

	minioClient, err := storage.NewMinIOClient(ctx, cfg.MinIO)
	if err != nil {
		logger.Error("failed to connect to minio", "error", err)
		os.Exit(1)
	}

	p := internalparser.New(cfg.Parser, pool, publisher, minioClient, logger)

	consumerName := fmt.Sprintf("parser-%d", os.Getpid())
	consumer := queue.NewConsumer(rdb, queue.ParseStream, queue.ParseDLQ, queue.ParserGroup, consumerName, cfg.Parser.PrefetchCount, logger)
	deliveries := consumer.Run(ctx)

	logger.Info("parser starting", "workers", cfg.Parser.Workers, "max_depth", cfg.Parser.MaxDepth)
	p.Run(ctx, deliveries)
}
