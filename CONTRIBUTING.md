# Contributing to Nimbus Crawler

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

1. Install [Go 1.23+](https://go.dev/dl/) and [Docker Compose](https://docs.docker.com/compose/install/).

2. Clone the repo and copy the environment file:

   ```bash
   git clone https://github.com/michaelmcclelland/nimbus-crawler.git
   cd nimbus-crawler
   cp .env.example .env
   ```

3. Start infrastructure services:

   ```bash
   docker-compose up -d postgres redis rabbitmq minio
   ```

4. Run the migration:

   ```bash
   go run ./cmd/migrate
   ```

5. Install pre-commit hooks:

   ```bash
   cp .githooks/pre-commit .git/hooks/pre-commit
   chmod +x .git/hooks/pre-commit
   ```

   Or configure git to use the hooks directory:

   ```bash
   git config core.hooksPath .githooks
   ```

## Running Tests

```bash
make test
```

## Code Style

- Run `gofmt` before committing (the pre-commit hook checks this).
- Run `go vet ./...` to catch common issues.
- Follow standard Go conventions and the patterns already established in the codebase.

## Submitting Changes

1. Create a feature branch from `main`:

   ```bash
   git checkout -b feature/your-feature
   ```

2. Make your changes and commit with a clear message.

3. Push your branch and open a pull request.

4. Describe what your PR does and why. Link any related issues.

## Reporting Issues

Open an issue on GitHub with steps to reproduce, expected behavior, and actual behavior.

## Questions

Open a discussion or issue on GitHub. We're happy to help.
