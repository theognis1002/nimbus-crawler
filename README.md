# Nimbus Crawler

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A distributed web crawler built in Go with a message-driven microservices architecture. Nimbus fetches, parses, and stores web pages at scale using a pipeline of loosely coupled workers coordinated through Redis Streams.

## Architecture

```
seeds.txt
    |
    v
 Seeder --> Redis Streams (stream:frontier) --> Crawler Workers --> MinIO (html)
                                                     |
                                                     v
                                              Redis Streams (stream:parse)
                                                     |
                                                     v
                                              Parser Workers --> MinIO (text)
                                                     |
                                                     '-> new URLs back to stream:frontier
                                                          (up to max_depth)
```

| Component  | Technology | Purpose                                          |
| ---------- | ---------- | ------------------------------------------------ |
| PostgreSQL | 18         | URL/domain records, crawl state                  |
| Redis      | 8          | DNS cache, rate limiting, robots.txt, job queues |
| MinIO      | pinned     | S3-compatible storage (HTML + text)              |

## Quick Start

```bash
git clone https://github.com/theognis1002/nimbus-crawler.git
cd nimbus-crawler
cp .env.example .env
make dev
```

- **Logs**: `make logs`
- **Stop**: `make down`
- **Seed URLs**: Edit `seeds.txt` (one URL per line), then `make seed`

### Web UIs

- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (`nimbus` / `nimbus_secret`)

## Configuration

Config loads from `configs/development.yaml` with environment variable overrides (env vars take priority). See [`.env.example`](.env.example) for the full variable list.

| Variable          | Default | Description                    |
| ----------------- | ------- | ------------------------------ |
| `MAX_DEPTH`       | 3       | Maximum link-follow depth      |
| `CRAWLER_WORKERS` | 10      | Goroutines per crawler replica |
| `PARSER_WORKERS`  | 5       | Goroutines per parser replica  |

## Make Targets

| Command      | Description                       |
| ------------ | --------------------------------- |
| `make dev`   | Build and start all services      |
| `make build` | Build Docker images               |
| `make test`  | Run Go tests                      |
| `make seed`  | Run the seeder independently      |
| `make logs`  | Tail crawler and parser logs      |
| `make down`  | Stop all services                 |
| `make clean` | Stop all services and remove data |

## Local Development

Start backing services, then run Go services directly:

```bash
docker-compose up -d postgres redis minio
go run ./cmd/migrate  # apply database schema migrations
go run ./cmd/seeder   # seed initial URLs from seeds.txt into Redis frontier stream
go run ./cmd/crawler  # fetch pages, store HTML in MinIO, publish parse jobs
go run ./cmd/parser   # extract text/links from HTML, deduplicate, publish new crawl jobs
```

Update `.env` to use `localhost` for `POSTGRES_HOST`, `REDIS_HOST`, and `MINIO_ENDPOINT`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## License

MIT. See [LICENSE](LICENSE) for details.
