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
	RabbitMQ  RabbitMQConfig  `yaml:"rabbitmq"`
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
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		url.PathEscape(c.User), url.PathEscape(c.Password), c.Host, c.Port, c.Database, sslmode)
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
}

func (c RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type RabbitMQConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

func (c RabbitMQConfig) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/",
		url.PathEscape(c.User), url.PathEscape(c.Password), c.Host, c.Port)
}

type MinIOConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type CrawlerConfig struct {
	Workers       int `yaml:"workers"`
	MaxDepth      int `yaml:"max_depth"`
	MaxRetries    int `yaml:"max_retries"`
	TimeoutSecs   int `yaml:"timeout_secs"`
	MaxRedirects  int `yaml:"max_redirects"`
	PrefetchCount int `yaml:"prefetch_count"`
}

type ParserConfig struct {
	Workers       int `yaml:"workers"`
	MaxDepth      int `yaml:"max_depth"`
	PrefetchCount int `yaml:"prefetch_count"`
}

type MigrationConfig struct {
	Path string `yaml:"path"`
}

func LoadFromEnv() *Config {
	cfg := &Config{}
	cfg.applyDefaults()
	cfg.applyEnvOverrides()
	return cfg
}

func (c *Config) applyDefaults() {
	if c.Postgres.Host == "" {
		c.Postgres.Host = "localhost"
	}
	if c.Postgres.Port == 0 {
		c.Postgres.Port = 5432
	}
	if c.Postgres.User == "" {
		c.Postgres.User = "nimbus"
	}
	if c.Postgres.Database == "" {
		c.Postgres.Database = "nimbus"
	}
	if c.Postgres.MaxConns == 0 {
		c.Postgres.MaxConns = 20
	}
	if c.Redis.Host == "" {
		c.Redis.Host = "localhost"
	}
	if c.Redis.Port == 0 {
		c.Redis.Port = 6379
	}
	if c.RabbitMQ.Host == "" {
		c.RabbitMQ.Host = "localhost"
	}
	if c.RabbitMQ.Port == 0 {
		c.RabbitMQ.Port = 5672
	}
	if c.RabbitMQ.User == "" {
		c.RabbitMQ.User = "guest"
	}
	if c.MinIO.Endpoint == "" {
		c.MinIO.Endpoint = "localhost:9000"
	}
	if c.Crawler.Workers == 0 {
		c.Crawler.Workers = 10
	}
	if c.Crawler.MaxDepth == 0 {
		c.Crawler.MaxDepth = 3
	}
	if c.Crawler.MaxRetries == 0 {
		c.Crawler.MaxRetries = 3
	}
	if c.Crawler.TimeoutSecs == 0 {
		c.Crawler.TimeoutSecs = 30
	}
	if c.Crawler.MaxRedirects == 0 {
		c.Crawler.MaxRedirects = 5
	}
	if c.Crawler.PrefetchCount == 0 {
		c.Crawler.PrefetchCount = 10
	}
	if c.Parser.Workers == 0 {
		c.Parser.Workers = 5
	}
	if c.Parser.MaxDepth == 0 {
		c.Parser.MaxDepth = 3
	}
	if c.Parser.PrefetchCount == 0 {
		c.Parser.PrefetchCount = 10
	}
	if c.Migration.Path == "" {
		c.Migration.Path = "file://migrations"
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
	if v := os.Getenv("RABBITMQ_HOST"); v != "" {
		c.RabbitMQ.Host = v
	}
	if v := os.Getenv("RABBITMQ_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.RabbitMQ.Port = p
		}
	}
	if v := os.Getenv("RABBITMQ_USER"); v != "" {
		c.RabbitMQ.User = v
	}
	if v := os.Getenv("RABBITMQ_PASSWORD"); v != "" {
		c.RabbitMQ.Password = v
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
}
