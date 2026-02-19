package models

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DomainRecord struct {
	Domain        string
	LastCrawlTime *time.Time
	RobotsTxt     *string
	CrawlDelayMs  int
	CreatedAt     time.Time
}

func UpsertDomain(ctx context.Context, pool *pgxpool.Pool, domain string, crawlDelayMs int) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO domains (domain, crawl_delay_ms) VALUES ($1, $2)
		 ON CONFLICT (domain) DO NOTHING`,
		domain, crawlDelayMs)
	if err != nil {
		return fmt.Errorf("upserting domain %s: %w", domain, err)
	}
	return nil
}

func GetDomain(ctx context.Context, pool *pgxpool.Pool, domain string) (*DomainRecord, error) {
	row := pool.QueryRow(ctx,
		`SELECT domain, last_crawl_time, robots_txt, crawl_delay_ms, created_at
		 FROM domains WHERE domain = $1`, domain)

	d := &DomainRecord{}
	if err := row.Scan(&d.Domain, &d.LastCrawlTime, &d.RobotsTxt, &d.CrawlDelayMs, &d.CreatedAt); err != nil {
		return nil, fmt.Errorf("getting domain %s: %w", domain, err)
	}
	return d, nil
}

func UpdateDomainRobotsTxt(ctx context.Context, pool *pgxpool.Pool, domain string, robotsTxt string, crawlDelayMs int) error {
	_, err := pool.Exec(ctx,
		`UPDATE domains SET robots_txt = $2, crawl_delay_ms = $3 WHERE domain = $1`,
		domain, robotsTxt, crawlDelayMs)
	if err != nil {
		return fmt.Errorf("updating robots.txt for %s: %w", domain, err)
	}
	return nil
}

// UpsertDomainWithRobots inserts or updates a domain with robots.txt and crawl delay in a single query.
func UpsertDomainWithRobots(ctx context.Context, pool *pgxpool.Pool, domain, robotsTxt string, crawlDelayMs int) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO domains (domain, robots_txt, crawl_delay_ms) VALUES ($1, $2, $3)
		 ON CONFLICT (domain) DO UPDATE SET robots_txt = EXCLUDED.robots_txt, crawl_delay_ms = EXCLUDED.crawl_delay_ms`,
		domain, robotsTxt, crawlDelayMs)
	if err != nil {
		return fmt.Errorf("upserting domain with robots %s: %w", domain, err)
	}
	return nil
}

func UpdateDomainLastCrawlTime(ctx context.Context, pool *pgxpool.Pool, domain string) error {
	_, err := pool.Exec(ctx,
		`UPDATE domains SET last_crawl_time = NOW() WHERE domain = $1`, domain)
	return err
}
