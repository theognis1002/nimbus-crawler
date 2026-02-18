# Nimbus Crawler

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A distributed web crawler built in Go with a message-driven microservices architecture. Nimbus fetches, parses, and stores web pages at scale using a pipeline of loosely coupled workers coordinated through RabbitMQ.

## Features

- Distributed crawling with configurable worker pools (crawler + parser replicas)
- Respects `robots.txt` rules and `Crawl-Delay` directives per domain
- Per-domain sliding-window rate limiting via Redis (Lua script)
- DNS caching with Redis to reduce lookup overhead
- Content deduplication using SHA-256 hashing
- Dead-letter queues for failed job recovery
- S3-compatible object storage (MinIO) for raw HTML and extracted text
- Configurable crawl depth with URL normalization
- Graceful shutdown via OS signal handling
- Single-command deployment with Docker Compose

## Architecture

```
seeds.txt
    |
    v
 Seeder --> RabbitMQ (frontier_queue) --> Crawler Workers --> MinIO (html)
                                               |
                                               v
                                        RabbitMQ (parse_queue)
                                               |
                                               v
                                        Parser Workers --> MinIO (text)
                                               |
                                               '-> new URLs back to frontier_queue
                                                    (up to max_depth)
```

| Component  | Technology | Purpose                              |
|------------|-----------|---------------------------------------|
| PostgreSQL | 18        | URL/domain records, crawl state       |
| Redis      | 8         | DNS cache, rate limiting, robots.txt  |
| RabbitMQ   | 4.0       | Job queues (frontier + parse)         |
| MinIO      | latest    | S3-compatible storage (HTML + text)   |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Go 1.23+](https://go.dev/dl/) (for local development only)

## Quick Start

1. Clone the repository:

   ```bash
   git clone https://github.com/michaelmcclelland/nimbus-crawler.git
   cd nimbus-crawler
   ```

2. Copy the environment config:

   ```bash
   cp .env.example .env
   ```

3. Start the full stack:

   ```bash
   make dev
   ```

4. View crawler and parser logs:

   ```bash
   make logs
   ```

5. Stop all services:

   ```bash
   make down
   ```

## Services

- **migrate** -- One-shot database schema migration runner
- **seeder** -- Reads `seeds.txt` and publishes initial URLs to the frontier queue at depth 0
- **crawler** (2 replicas) -- Consumes URLs, respects robots.txt and per-domain rate limits, fetches HTML, stores in MinIO, publishes parse jobs
- **parser** (3 replicas) -- Downloads HTML from MinIO, extracts text and links, deduplicates by SHA-256 content hash, publishes new URLs back to the frontier queue

## Configuration

Config loads from `configs/development.yaml` with environment variable overrides (env vars take priority). See [`.env.example`](.env.example) for the full variable list.

Key variables:

| Variable           | Default        | Description                    |
|--------------------|----------------|--------------------------------|
| `MAX_DEPTH`        | 3              | Maximum link-follow depth      |
| `CRAWLER_WORKERS`  | 10             | Goroutines per crawler replica |
| `PARSER_WORKERS`   | 5              | Goroutines per parser replica  |

### Seed URLs

Edit `seeds.txt` to configure starting URLs (one per line, `#` for comments), then re-run the seeder:

```bash
make seed
```

### Web UIs

- **RabbitMQ Management**: [http://localhost:15672](http://localhost:15672) (`nimbus` / `nimbus_secret`)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (`nimbus` / `nimbus_secret`)

## Make Targets

| Command      | Description                        |
|--------------|------------------------------------|
| `make dev`   | Build and start all services       |
| `make build` | Build Docker images                |
| `make test`  | Run Go tests                       |
| `make seed`  | Run the seeder independently       |
| `make logs`  | Tail crawler and parser logs       |
| `make down`  | Stop all services                  |
| `make clean` | Stop all services and remove data  |

## Development

### Local Development

Start just the backing services:

```bash
docker-compose up -d postgres redis rabbitmq minio
```

Update `.env` to use `localhost` for `POSTGRES_HOST`, `REDIS_HOST`, `RABBITMQ_HOST`, and `MINIO_ENDPOINT`, then run:

```bash
go run ./cmd/migrate
go run ./cmd/seeder
go run ./cmd/crawler
go run ./cmd/parser
```

### Building Locally

```bash
go build -o bin/crawler ./cmd/crawler
go build -o bin/parser ./cmd/parser
go build -o bin/migrate ./cmd/migrate
go build -o bin/seeder ./cmd/seeder
```

### Static Analysis

```bash
go vet ./...
```

## Database Schema

Defined in `internal/database/migrations/001_initial.up.sql`:

- **domains** -- domain (PK), robots_txt, crawl_delay_ms
- **urls** -- UUID PK, url (unique), domain (FK), status enum (`pending`/`crawling`/`crawled`/`parsed`/`failed`/`skipped`), content_hash, depth, S3 links

## Project Structure

```
cmd/
  crawler/     Crawler worker entry point
  parser/      Parser worker entry point
  migrate/     Database migration runner
  seeder/      Seed URL publisher
internal/
  cache/       Redis DNS cache, sliding-window rate limiter
  config/      YAML + env var config loading
  crawler/     Fetch logic, backoff, worker loop
  database/    PostgreSQL pool, migrations, URL/domain models
  parser/      HTML text/URL extraction, content dedup
  queue/       RabbitMQ topology, publisher, consumer
  robots/      robots.txt fetch/parse/cache
  seeder/      Seed file reader + publisher
  storage/     MinIO S3 client for HTML and text buckets
configs/       YAML config profiles (development, production)
docker/        Dockerfile (multi-stage build)
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
