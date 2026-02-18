package queue

import (
	"context"
	"log/slog"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	FrontierStream = "stream:frontier"
	ParseStream    = "stream:parse"
	FrontierDLQ    = "stream:frontier:dlq"
	ParseDLQ       = "stream:parse:dlq"

	CrawlerGroup = "crawler-workers"
	ParserGroup  = "parser-workers"
)

// EnsureStreams creates consumer groups (and their underlying streams) idempotently.
func EnsureStreams(ctx context.Context, rdb *redis.Client, logger *slog.Logger) error {
	groups := []struct {
		stream string
		group  string
	}{
		{FrontierStream, CrawlerGroup},
		{ParseStream, ParserGroup},
	}

	for _, g := range groups {
		err := rdb.XGroupCreateMkStream(ctx, g.stream, g.group, "0").Err()
		if err != nil {
			// BUSYGROUP means group already exists — that's fine.
			if !isBusyGroupError(err) {
				return err
			}
			logger.Debug("consumer group already exists", "stream", g.stream, "group", g.group)
		} else {
			logger.Info("created consumer group", "stream", g.stream, "group", g.group)
		}
	}
	return nil
}

func isBusyGroupError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
