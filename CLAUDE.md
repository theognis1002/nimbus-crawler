# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Build all binaries (always use -o bin/ to keep repo root clean)
go build -o bin/crawler ./cmd/crawler
go build -o bin/parser ./cmd/parser
go build -o bin/migrate ./cmd/migrate
go build -o bin/seeder ./cmd/seeder

# Run individual services (requires infrastructure running)
go run ./cmd/crawler
go run ./cmd/parser
go run ./cmd/migrate

# Full stack via Docker
docker-compose up -d          # start all services
docker-compose build          # rebuild images

# No tests or linter configured yet
go vet ./...                  # basic static analysis
```

## Architecture

Nimbus Crawler is a distributed web crawler built in Go 1.26 with a message-driven microservices architecture. Licensed under MIT.

### Data Flow

```
Seeder → Redis Streams(stream:frontier) → Crawler Workers → MinIO(html) + PostgreSQL
    → Redis Streams(stream:parse) → Parser Workers → MinIO(text) + PostgreSQL
    → new URLs back to stream:frontier (up to max_depth=3)
```

### Services (docker-compose.yml)

- **crawler** (2 replicas): Fetches URLs, respects robots.txt + per-domain rate limits, stores HTML in MinIO, publishes parse jobs
- **parser** (3 replicas): Extracts text and URLs from HTML, deduplicates by content hash (SHA256), publishes new crawl jobs
- **migrate**: One-shot schema migration runner
- **seeder**: One-shot initial URL seeder (reads seeds.txt, publishes to frontier stream)

### Infrastructure

| Service    | Purpose                                          | Port(s)     |
|------------|--------------------------------------------------|-------------|
| PostgreSQL | URL/domain records, crawl state                  | 5432        |
| Redis      | DNS cache, rate limiting, robots.txt, job queues  | 6379        |
| MinIO      | S3-compatible storage (html + text)              | 9000, 9001  |

### Key Internal Packages

- `internal/crawler/` — Worker loop (`crawler.go`), HTTP fetcher with DNS caching (`fetcher.go`), exponential backoff with jitter (`backoff.go`)
- `internal/queue/` — Redis Streams consumer groups with XREADGROUP/XACK, dead-letter queues via separate streams, XAUTOCLAIM-based reclaim loop for stuck messages
- `internal/cache/` — Redis DNS cache (5m TTL), sliding-window rate limiter (Lua script), Redis client init
- `internal/robots/` — robots.txt fetching/parsing with Redis caching (1h TTL), Crawl-Delay extraction
- `internal/parser/` — HTML text/URL extraction via goquery (`extractor.go`), content-hash deduplication (`dedup.go`)
- `internal/database/models/` — URL and Domain CRUD with pgx
- `internal/storage/` — MinIO client for `nimbus-html` and `nimbus-text` buckets
- `internal/config/` — YAML config with env var overrides (env vars take priority)

### Configuration

Config loads from `configs/development.yaml` (optional) with environment variable overrides. Key env vars: `POSTGRES_*`, `REDIS_*`, `MINIO_*`, `MAX_DEPTH`, `CRAWLER_WORKERS`, `PARSER_WORKERS`. See `.env.example` for the full list.

### Database Schema

Defined in `internal/database/migrations/001_initial.up.sql`:
- `domains` table: domain (PK), robots_txt, crawl_delay_ms
- `urls` table: UUID PK, url (unique), domain (FK), status enum (pending/crawling/crawled/parsed/failed/skipped), content_hash, depth, s3 links

### Patterns

- Workers use goroutines + WaitGroup; graceful shutdown via context cancellation on OS signals
- HTTP fetcher has 10MB body limit, custom redirect handling, DNS cache integration
- Rate limiter uses atomic Redis Lua script for sliding-window per domain (min 500ms, default 1000ms)
- Job queues use Redis Streams with consumer groups; unacked messages are reclaimed via XAUTOCLAIM after 60s idle
- Structured logging via `log/slog` with JSON output
- Docker multi-stage build: `golang:1.26-alpine` builder → `alpine:3.21` runtime with static binaries (CGO_ENABLED=0)
