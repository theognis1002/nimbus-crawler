package models

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type URLStatus string

const (
	StatusPending  URLStatus = "pending"
	StatusCrawling URLStatus = "crawling"
	StatusCrawled  URLStatus = "crawled"
	StatusParsed   URLStatus = "parsed"
	StatusFailed   URLStatus = "failed"
	StatusSkipped  URLStatus = "skipped"
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
	if len(urls) != len(domains) {
		return nil, fmt.Errorf("bulk insert: urls and domains length mismatch (%d != %d)", len(urls), len(domains))
	}
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

func UpdateURLStatus(ctx context.Context, pool *pgxpool.Pool, id string, status URLStatus) error {
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

// UpdateURLCrawledAndDomainTime batches the URL crawled update and the domain
// last_crawl_time update into a single DB round-trip using pgx.Batch.
func UpdateURLCrawledAndDomainTime(ctx context.Context, pool *pgxpool.Pool, urlID, s3HTMLLink, domain string) error {
	batch := &pgx.Batch{}
	batch.Queue(
		`UPDATE urls SET status = 'crawled', s3_html_link = $2, last_crawl_time = NOW(), updated_at = NOW()
		 WHERE id = $1`,
		urlID, s3HTMLLink)
	batch.Queue(
		`UPDATE domains SET last_crawl_time = NOW() WHERE domain = $1`,
		domain)

	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	if _, err := br.Exec(); err != nil {
		return fmt.Errorf("updating url crawled: %w", err)
	}
	if _, err := br.Exec(); err != nil {
		return fmt.Errorf("updating domain last_crawl_time: %w", err)
	}
	return nil
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

// UpsertURLReturning inserts a URL with status 'crawling', or on conflict
// atomically transitions pending/failed → 'crawling' to claim it for this worker.
// URLs already crawled/parsed/skipped are left unchanged. The caller should
// check the returned status to decide whether to proceed.
func UpsertURLReturning(ctx context.Context, pool *pgxpool.Pool, rawURL, domain string, depth int) (id string, status URLStatus, err error) {
	var statusStr string
	err = pool.QueryRow(ctx,
		`INSERT INTO urls (url, domain, depth, status) VALUES ($1, $2, $3, 'crawling')
		 ON CONFLICT (url) DO UPDATE SET
		   status = CASE WHEN urls.status IN ('pending', 'failed') THEN 'crawling' ELSE urls.status END,
		   updated_at = NOW()
		 RETURNING id, status`,
		rawURL, domain, depth).Scan(&id, &statusStr)
	if err != nil {
		return "", "", fmt.Errorf("upserting url: %w", err)
	}
	return id, URLStatus(statusStr), nil
}

// IncrementRetryAndMaybeFailURL atomically increments retry_count and sets status to 'failed'
// if the new count reaches maxRetries.
func IncrementRetryAndMaybeFailURL(ctx context.Context, pool *pgxpool.Pool, id string, maxRetries int) (int, error) {
	var count int
	err := pool.QueryRow(ctx,
		`UPDATE urls SET retry_count = retry_count + 1,
		   status = CASE WHEN retry_count + 1 >= $2 THEN 'failed' ELSE status END,
		   updated_at = NOW()
		 WHERE id = $1 RETURNING retry_count`, id, maxRetries).Scan(&count)
	return count, err
}

func ResetStaleCrawlingURLs(ctx context.Context, pool *pgxpool.Pool, staleDuration time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx,
		`UPDATE urls SET status = 'pending', updated_at = NOW()
		 WHERE status = 'crawling' AND updated_at < NOW() - make_interval(secs => $1)`,
		staleDuration.Seconds())
	if err != nil {
		return 0, fmt.Errorf("resetting stale crawling urls: %w", err)
	}
	return tag.RowsAffected(), nil
}

func ContentHashExists(ctx context.Context, pool *pgxpool.Pool, hash string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM urls WHERE content_hash = $1)`, hash).Scan(&exists)
	return exists, err
}
