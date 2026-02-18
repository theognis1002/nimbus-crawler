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

	p := internalparser.New(cfg.Parser, pool, publisher, minioClient, logger)

	consumerName := fmt.Sprintf("parser-%d", os.Getpid())
	consumer := queue.NewConsumer(rdb, queue.ParseStream, queue.ParseDLQ, queue.ParserGroup, consumerName, cfg.Parser.PrefetchCount, logger)
	deliveries := consumer.Run(ctx)

	logger.Info("parser starting", "workers", cfg.Parser.Workers, "max_depth", cfg.Parser.MaxDepth)
	p.Run(ctx, deliveries)
	consumer.Wait()

	return nil
}
