package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/michaelmcclelland/nimbus-crawler/internal/cache"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/seeder"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load("configs/development.yaml")
	if err != nil {
		logger.Info("config file not found, using env vars", "error", err)
		cfg = config.LoadFromEnv()
	}

	ctx := context.Background()

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

	seedFile := "seeds.txt"
	if len(os.Args) > 1 {
		seedFile = os.Args[1]
	}

	if err := seeder.LoadAndPublish(ctx, seedFile, pool, publisher, logger); err != nil {
		logger.Error("seeding failed", "error", err)
		os.Exit(1)
	}
}
