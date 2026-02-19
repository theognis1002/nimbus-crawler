package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/theognis1002/nimbus-crawler/internal/cache"
	"github.com/theognis1002/nimbus-crawler/internal/config"
	"github.com/theognis1002/nimbus-crawler/internal/database"
	"github.com/theognis1002/nimbus-crawler/internal/queue"
	"github.com/theognis1002/nimbus-crawler/internal/seeder"
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

	seedFile := "seeds.txt"
	if len(os.Args) > 1 {
		seedFile = os.Args[1]
	}

	if err := seeder.LoadAndPublish(ctx, seedFile, pool, publisher, logger); err != nil {
		return fmt.Errorf("seeding failed: %w", err)
	}

	return nil
}
