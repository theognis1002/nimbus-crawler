package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostgresConfig_DSN(t *testing.T) {
	t.Parallel()
	c := PostgresConfig{Host: "db", Port: 5432, User: "u", Password: "p", Database: "d"}
	want := "postgres://u:p@db:5432/d?sslmode=disable"
	if got := c.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestPostgresConfig_DSN_SpecialChars(t *testing.T) {
	t.Parallel()
	c := PostgresConfig{Host: "db", Port: 5432, User: "user", Password: "p@ss:word/!", Database: "d"}
	got := c.DSN()
	if !strings.Contains(got, "postgres://") {
		t.Errorf("DSN() = %q, expected postgres:// scheme", got)
	}
	if strings.Contains(got, "p@ss:word/!") {
		t.Errorf("DSN() = %q, special chars in password should be escaped", got)
	}
}

func TestRedisConfig_Addr(t *testing.T) {
	t.Parallel()
	c := RedisConfig{Host: "redis", Port: 6379}
	want := "redis:6379"
	if got := c.Addr(); got != want {
		t.Errorf("Addr() = %q, want %q", got, want)
	}
}

func TestLoadFromEnv_Defaults(t *testing.T) {
	cfg := LoadFromEnv()

	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Postgres.Host = %q, want localhost", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 5432 {
		t.Errorf("Postgres.Port = %d, want 5432", cfg.Postgres.Port)
	}
	if cfg.Postgres.User != "nimbus" {
		t.Errorf("Postgres.User = %q, want nimbus", cfg.Postgres.User)
	}
	if cfg.Postgres.Database != "nimbus" {
		t.Errorf("Postgres.Database = %q, want nimbus", cfg.Postgres.Database)
	}
	if cfg.Redis.Host != "localhost" {
		t.Errorf("Redis.Host = %q, want localhost", cfg.Redis.Host)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("Redis.Port = %d, want 6379", cfg.Redis.Port)
	}
	if cfg.Crawler.Workers != 10 {
		t.Errorf("Crawler.Workers = %d, want 10", cfg.Crawler.Workers)
	}
	if cfg.Crawler.MaxDepth != 3 {
		t.Errorf("Crawler.MaxDepth = %d, want 3", cfg.Crawler.MaxDepth)
	}
	if cfg.Parser.Workers != 5 {
		t.Errorf("Parser.Workers = %d, want 5", cfg.Parser.Workers)
	}
}

func TestLoadFromEnv_EnvOverrides(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "pg-host")
	t.Setenv("POSTGRES_PORT", "9999")
	t.Setenv("POSTGRES_USER", "admin")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB", "mydb")
	t.Setenv("REDIS_HOST", "redis-host")
	t.Setenv("REDIS_PORT", "7777")
	t.Setenv("CRAWLER_WORKERS", "20")
	t.Setenv("PARSER_WORKERS", "15")
	t.Setenv("MAX_DEPTH", "5")
	t.Setenv("MINIO_ENDPOINT", "minio:9999")
	t.Setenv("MINIO_USE_SSL", "true")

	cfg := LoadFromEnv()

	if cfg.Postgres.Host != "pg-host" {
		t.Errorf("Postgres.Host = %q, want pg-host", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 9999 {
		t.Errorf("Postgres.Port = %d, want 9999", cfg.Postgres.Port)
	}
	if cfg.Postgres.User != "admin" {
		t.Errorf("Postgres.User = %q, want admin", cfg.Postgres.User)
	}
	if cfg.Postgres.Password != "secret" {
		t.Errorf("Postgres.Password = %q, want secret", cfg.Postgres.Password)
	}
	if cfg.Postgres.Database != "mydb" {
		t.Errorf("Postgres.Database = %q, want mydb", cfg.Postgres.Database)
	}
	if cfg.Redis.Host != "redis-host" {
		t.Errorf("Redis.Host = %q, want redis-host", cfg.Redis.Host)
	}
	if cfg.Redis.Port != 7777 {
		t.Errorf("Redis.Port = %d, want 7777", cfg.Redis.Port)
	}
	if cfg.Crawler.Workers != 20 {
		t.Errorf("Crawler.Workers = %d, want 20", cfg.Crawler.Workers)
	}
	if cfg.Parser.Workers != 15 {
		t.Errorf("Parser.Workers = %d, want 15", cfg.Parser.Workers)
	}
	if cfg.Crawler.MaxDepth != 5 {
		t.Errorf("Crawler.MaxDepth = %d, want 5", cfg.Crawler.MaxDepth)
	}
	if cfg.Parser.MaxDepth != 5 {
		t.Errorf("Parser.MaxDepth = %d, want 5", cfg.Parser.MaxDepth)
	}
	if cfg.MinIO.Endpoint != "minio:9999" {
		t.Errorf("MinIO.Endpoint = %q, want minio:9999", cfg.MinIO.Endpoint)
	}
	if !cfg.MinIO.UseSSL {
		t.Error("MinIO.UseSSL should be true")
	}
}

func TestLoad_YAMLFile(t *testing.T) {
	yaml := `
postgres:
  host: yamlhost
  port: 1234
  user: yamluser
  password: yamlpass
  database: yamldb
redis:
  host: yamlredis
  port: 4321
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Postgres.Host != "yamlhost" {
		t.Errorf("Postgres.Host = %q, want yamlhost", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 1234 {
		t.Errorf("Postgres.Port = %d, want 1234", cfg.Postgres.Port)
	}
	if cfg.Redis.Host != "yamlredis" {
		t.Errorf("Redis.Host = %q, want yamlredis", cfg.Redis.Host)
	}
	// Defaults should still apply for unset fields
	if cfg.Crawler.Workers != 10 {
		t.Errorf("Crawler.Workers = %d, want 10 (default)", cfg.Crawler.Workers)
	}
}

func TestLoad_NonexistentFile(t *testing.T) {
	t.Parallel()
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Load() should return error for nonexistent file")
	}
}

func TestRespectRobotsTxt_DefaultTrue(t *testing.T) {
	t.Parallel()
	cfg := LoadFromEnv()
	if cfg.Crawler.RespectRobotsTxt == nil {
		t.Fatal("RespectRobotsTxt should not be nil after defaults")
	}
	if !*cfg.Crawler.RespectRobotsTxt {
		t.Error("RespectRobotsTxt should default to true")
	}
}

func TestRespectRobotsTxt_YAMLFalse(t *testing.T) {
	yaml := `
crawler:
  respect_robots_txt: false
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Crawler.RespectRobotsTxt == nil {
		t.Fatal("RespectRobotsTxt should not be nil")
	}
	if *cfg.Crawler.RespectRobotsTxt {
		t.Error("RespectRobotsTxt should be false when set in YAML")
	}
}

func TestRespectRobotsTxt_EnvOverride(t *testing.T) {
	yaml := `
crawler:
  respect_robots_txt: true
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}

	t.Setenv("RESPECT_ROBOTS_TXT", "false")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Crawler.RespectRobotsTxt == nil {
		t.Fatal("RespectRobotsTxt should not be nil")
	}
	if *cfg.Crawler.RespectRobotsTxt {
		t.Error("RespectRobotsTxt should be false when env override is set")
	}
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	yaml := `
postgres:
  host: yamlhost
  port: 1234
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}

	t.Setenv("POSTGRES_HOST", "envhost")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Postgres.Host != "envhost" {
		t.Errorf("Postgres.Host = %q, want envhost (env should override YAML)", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 1234 {
		t.Errorf("Postgres.Port = %d, want 1234 (YAML value should persist)", cfg.Postgres.Port)
	}
}
