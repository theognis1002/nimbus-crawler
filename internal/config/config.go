package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres  PostgresConfig  `yaml:"postgres"`
	Redis     RedisConfig     `yaml:"redis"`
	MinIO     MinIOConfig     `yaml:"minio"`
	Crawler   CrawlerConfig   `yaml:"crawler"`
	Parser    ParserConfig    `yaml:"parser"`
	Migration MigrationConfig `yaml:"migration"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslmode"`
	MaxConns int32  `yaml:"max_conns"`
}

func (c PostgresConfig) DSN() string {
	sslmode := c.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Database,
		RawQuery: fmt.Sprintf("sslmode=%s", sslmode),
	}
	return u.String()
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
}

func (c RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type MinIOConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type CrawlerConfig struct {
	Workers       int         `yaml:"workers"`
	MaxDepth      int         `yaml:"max_depth"`
	MaxRetries    int         `yaml:"max_retries"`
	TimeoutSecs   int         `yaml:"timeout_secs"`
	MaxRedirects  int         `yaml:"max_redirects"`
	PrefetchCount int         `yaml:"prefetch_count"`
	Proxy         ProxyConfig `yaml:"proxy"`
}

type ProxyConfig struct {
	File            string `yaml:"file"`
	HealthCooldownS int    `yaml:"health_cooldown_s"`
}

type ParserConfig struct {
	Workers       int `yaml:"workers"`
	MaxDepth      int `yaml:"max_depth"`
	PrefetchCount int `yaml:"prefetch_count"`
}

type MigrationConfig struct {
	Path string `yaml:"path"`
}

const (
	defaultPostgresHost     = "localhost"
	defaultPostgresPort     = 5432
	defaultPostgresUser     = "nimbus"
	defaultPostgresDB       = "nimbus"
	defaultPostgresMaxConns = 20
	defaultRedisHost        = "localhost"
	defaultRedisPort        = 6379
	defaultMinIOEndpoint    = "localhost:9000"
	defaultCrawlerWorkers   = 10
	defaultMaxDepth         = 3
	defaultMaxRetries       = 3
	defaultTimeoutSecs      = 30
	defaultMaxRedirects     = 5
	defaultPrefetchCount    = 10
	defaultParserWorkers    = 5
	defaultMigrationPath        = "file://internal/database/migrations"
	defaultProxyHealthCooldownS = 60
)

func LoadFromEnv() *Config {
	cfg := &Config{}
	cfg.applyDefaults()
	cfg.applyEnvOverrides()
	return cfg
}

func (c *Config) applyDefaults() {
	if c.Postgres.Host == "" {
		c.Postgres.Host = defaultPostgresHost
	}
	if c.Postgres.Port == 0 {
		c.Postgres.Port = defaultPostgresPort
	}
	if c.Postgres.User == "" {
		c.Postgres.User = defaultPostgresUser
	}
	if c.Postgres.Database == "" {
		c.Postgres.Database = defaultPostgresDB
	}
	if c.Postgres.MaxConns == 0 {
		c.Postgres.MaxConns = defaultPostgresMaxConns
	}
	if c.Redis.Host == "" {
		c.Redis.Host = defaultRedisHost
	}
	if c.Redis.Port == 0 {
		c.Redis.Port = defaultRedisPort
	}
	if c.MinIO.Endpoint == "" {
		c.MinIO.Endpoint = defaultMinIOEndpoint
	}
	if c.Crawler.Workers == 0 {
		c.Crawler.Workers = defaultCrawlerWorkers
	}
	if c.Crawler.MaxDepth == 0 {
		c.Crawler.MaxDepth = defaultMaxDepth
	}
	if c.Crawler.MaxRetries == 0 {
		c.Crawler.MaxRetries = defaultMaxRetries
	}
	if c.Crawler.TimeoutSecs == 0 {
		c.Crawler.TimeoutSecs = defaultTimeoutSecs
	}
	if c.Crawler.MaxRedirects == 0 {
		c.Crawler.MaxRedirects = defaultMaxRedirects
	}
	if c.Crawler.PrefetchCount == 0 {
		c.Crawler.PrefetchCount = defaultPrefetchCount
	}
	if c.Parser.Workers == 0 {
		c.Parser.Workers = defaultParserWorkers
	}
	if c.Parser.MaxDepth == 0 {
		c.Parser.MaxDepth = defaultMaxDepth
	}
	if c.Parser.PrefetchCount == 0 {
		c.Parser.PrefetchCount = defaultPrefetchCount
	}
	if c.Crawler.Proxy.HealthCooldownS == 0 {
		c.Crawler.Proxy.HealthCooldownS = defaultProxyHealthCooldownS
	}
	if c.Migration.Path == "" {
		c.Migration.Path = defaultMigrationPath
	}
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}

	expanded := os.Expand(string(data), os.Getenv)

	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.applyDefaults()
	cfg.applyEnvOverrides()
	return cfg, nil
}

func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("POSTGRES_HOST"); v != "" {
		c.Postgres.Host = v
	}
	if v := os.Getenv("POSTGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.Postgres.Port = p
		}
	}
	if v := os.Getenv("POSTGRES_USER"); v != "" {
		c.Postgres.User = v
	}
	if v := os.Getenv("POSTGRES_PASSWORD"); v != "" {
		c.Postgres.Password = v
	}
	if v := os.Getenv("POSTGRES_DB"); v != "" {
		c.Postgres.Database = v
	}
	if v := os.Getenv("POSTGRES_SSLMODE"); v != "" {
		c.Postgres.SSLMode = v
	}
	if v := os.Getenv("REDIS_HOST"); v != "" {
		c.Redis.Host = v
	}
	if v := os.Getenv("REDIS_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.Redis.Port = p
		}
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		c.Redis.Password = v
	}
	if v := os.Getenv("MINIO_ENDPOINT"); v != "" {
		c.MinIO.Endpoint = v
	}
	if v := os.Getenv("MINIO_ACCESS_KEY"); v != "" {
		c.MinIO.AccessKey = v
	}
	if v := os.Getenv("MINIO_SECRET_KEY"); v != "" {
		c.MinIO.SecretKey = v
	}
	if v := os.Getenv("MINIO_USE_SSL"); v != "" {
		c.MinIO.UseSSL = strings.EqualFold(v, "true")
	}
	if v := os.Getenv("MAX_DEPTH"); v != "" {
		if d, err := strconv.Atoi(v); err == nil {
			c.Crawler.MaxDepth = d
			c.Parser.MaxDepth = d
		}
	}
	if v := os.Getenv("CRAWLER_WORKERS"); v != "" {
		if w, err := strconv.Atoi(v); err == nil {
			c.Crawler.Workers = w
		}
	}
	if v := os.Getenv("PARSER_WORKERS"); v != "" {
		if w, err := strconv.Atoi(v); err == nil {
			c.Parser.Workers = w
		}
	}
	if v := os.Getenv("PROXY_FILE"); v != "" {
		c.Crawler.Proxy.File = v
	}
	if v := os.Getenv("PROXY_HEALTH_COOLDOWN_S"); v != "" {
		if s, err := strconv.Atoi(v); err == nil {
			c.Crawler.Proxy.HealthCooldownS = s
		}
	}
	if v := os.Getenv("MIGRATION_PATH"); v != "" {
		c.Migration.Path = v
	}
}
