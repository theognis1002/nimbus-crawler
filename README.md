# Nimbus Crawler

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A distributed web crawler built in Go with a message-driven microservices architecture. Nimbus fetches, parses, and stores web pages at scale using a pipeline of loosely coupled workers coordinated through RabbitMQ.

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

## Quick Start

```bash
git clone https://github.com/michaelmcclelland/nimbus-crawler.git
cd nimbus-crawler
cp .env.example .env
make dev
```

- **Logs**: `make logs`
- **Stop**: `make down`
- **Seed URLs**: Edit `seeds.txt` (one URL per line), then `make seed`

### Web UIs

- **RabbitMQ Management**: [http://localhost:15672](http://localhost:15672) (`nimbus` / `nimbus_secret`)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (`nimbus` / `nimbus_secret`)

## Configuration

Config loads from `configs/development.yaml` with environment variable overrides (env vars take priority). See [`.env.example`](.env.example) for the full variable list.

| Variable           | Default | Description                    |
|--------------------|---------|--------------------------------|
| `MAX_DEPTH`        | 3       | Maximum link-follow depth      |
| `CRAWLER_WORKERS`  | 10      | Goroutines per crawler replica |
| `PARSER_WORKERS`   | 5       | Goroutines per parser replica  |

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

## Local Development

Start backing services, then run Go services directly:

```bash
docker-compose up -d postgres redis rabbitmq minio
go run ./cmd/migrate
go run ./cmd/seeder
go run ./cmd/crawler
go run ./cmd/parser
```

Update `.env` to use `localhost` for `POSTGRES_HOST`, `REDIS_HOST`, `RABBITMQ_HOST`, and `MINIO_ENDPOINT`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## License

MIT. See [LICENSE](LICENSE) for details.
