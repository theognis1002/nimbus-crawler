package seeder

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database/models"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/robots"
)

func LoadAndPublish(ctx context.Context, seedFile string, pool *pgxpool.Pool, publisher *queue.Publisher, logger *slog.Logger) error {
	f, err := os.Open(seedFile)
	if err != nil {
		return fmt.Errorf("opening seed file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parsed, err := url.Parse(line)
		if err != nil {
			logger.Warn("invalid seed url", "url", line, "error", err)
			continue
		}

		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			logger.Warn("unsupported scheme in seed url", "url", line, "scheme", parsed.Scheme)
			continue
		}

		domain := parsed.Hostname()
		if domain == "" {
			logger.Warn("no domain in seed url", "url", line)
			continue
		}

		if err := models.UpsertDomain(ctx, pool, domain, robots.DefaultCrawlDelayMs); err != nil {
			logger.Warn("failed to upsert domain", "domain", domain, "error", err)
			continue
		}

		id, err := models.InsertURL(ctx, pool, line, domain, 0)
		if err != nil {
			logger.Warn("failed to insert seed url", "url", line, "error", err)
			continue
		}
		if id == "" {
			logger.Info("seed url already exists", "url", line)
			continue
		}

		if err := publisher.PublishURL(ctx, queue.URLMessage{URL: line, Depth: 0}); err != nil {
			logger.Error("failed to publish seed url", "url", line, "error", err)
			continue
		}

		count++
		logger.Info("seeded url", "url", line)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading seed file: %w", err)
	}

	logger.Info("seeding complete", "count", count)
	return nil
}
