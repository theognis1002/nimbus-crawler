package main

import (
	"log/slog"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load("configs/development.yaml")
	if err != nil {
		logger.Info("config file not found, using env vars", "error", err)
		cfg = config.LoadFromEnv()
	}

	logger.Info("running migrations", "host", cfg.Postgres.Host, "db", cfg.Postgres.Database)

	m, err := migrate.New(cfg.Migration.Path, cfg.Postgres.DSN())
	if err != nil {
		logger.Error("failed to create migrator", "error", err)
		os.Exit(1)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}

	logger.Info("migrations completed successfully")
}
