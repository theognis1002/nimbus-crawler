package models

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type URLRecord struct {
	ID            string
	URL           string
	Domain        string
	S3HTMLLink    *string
	S3TextLink    *string
	ContentHash   *string
	Depth         int
	Status        string
	RetryCount    int
	LastCrawlTime *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func InsertURL(ctx context.Context, pool *pgxpool.Pool, url, domain string, depth int) (string, error) {
	var id string
	err := pool.QueryRow(ctx,
		`INSERT INTO urls (url, domain, depth) VALUES ($1, $2, $3)
		 ON CONFLICT (url) DO NOTHING
		 RETURNING id`,
		url, domain, depth).Scan(&id)
	if err == pgx.ErrNoRows {
		return "", nil // already exists
	}
	if err != nil {
		return "", fmt.Errorf("inserting url: %w", err)
	}
	return id, nil
}

// BulkInsertURLs inserts URLs and returns only the ones that were actually inserted (not already existing).
func BulkInsertURLs(ctx context.Context, pool *pgxpool.Pool, urls []string, domains []string, depth int) ([]string, error) {
	batch := &pgx.Batch{}
	for i, u := range urls {
		batch.Queue(
			`INSERT INTO urls (url, domain, depth) VALUES ($1, $2, $3) ON CONFLICT (url) DO NOTHING RETURNING url`,
			u, domains[i], depth)
	}
	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	var inserted []string
	for range urls {
		var u string
		err := br.QueryRow().Scan(&u)
		if err == pgx.ErrNoRows {
			continue // already existed
		}
		if err != nil {
			return inserted, fmt.Errorf("bulk inserting urls: %w", err)
		}
		inserted = append(inserted, u)
	}
	return inserted, nil
}

func GetURLByURL(ctx context.Context, pool *pgxpool.Pool, url string) (*URLRecord, error) {
	row := pool.QueryRow(ctx,
		`SELECT id, url, domain, s3_html_link, s3_text_link, content_hash, depth, status, retry_count, last_crawl_time, created_at, updated_at
		 FROM urls WHERE url = $1`, url)

	r := &URLRecord{}
	if err := row.Scan(&r.ID, &r.URL, &r.Domain, &r.S3HTMLLink, &r.S3TextLink, &r.ContentHash,
		&r.Depth, &r.Status, &r.RetryCount, &r.LastCrawlTime, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	return r, nil
}

func UpdateURLStatus(ctx context.Context, pool *pgxpool.Pool, id string, status string) error {
	_, err := pool.Exec(ctx,
		`UPDATE urls SET status = $2, updated_at = NOW() WHERE id = $1`,
		id, status)
	return err
}

func UpdateURLCrawled(ctx context.Context, pool *pgxpool.Pool, id, s3HTMLLink string) error {
	_, err := pool.Exec(ctx,
		`UPDATE urls SET status = 'crawled', s3_html_link = $2, last_crawl_time = NOW(), updated_at = NOW()
		 WHERE id = $1`,
		id, s3HTMLLink)
	return err
}

func UpdateURLParsed(ctx context.Context, pool *pgxpool.Pool, id, contentHash, s3TextLink string) error {
	_, err := pool.Exec(ctx,
		`UPDATE urls SET status = 'parsed', content_hash = $2, s3_text_link = $3, updated_at = NOW()
		 WHERE id = $1`,
		id, contentHash, s3TextLink)
	return err
}

func IncrementRetryCount(ctx context.Context, pool *pgxpool.Pool, id string) (int, error) {
	var count int
	err := pool.QueryRow(ctx,
		`UPDATE urls SET retry_count = retry_count + 1, updated_at = NOW()
		 WHERE id = $1 RETURNING retry_count`, id).Scan(&count)
	return count, err
}

func ContentHashExists(ctx context.Context, pool *pgxpool.Pool, hash string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM urls WHERE content_hash = $1)`, hash).Scan(&exists)
	return exists, err
}
