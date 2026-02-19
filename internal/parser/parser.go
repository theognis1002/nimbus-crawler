package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/michaelmcclelland/nimbus-crawler/internal/database/models"
	"github.com/michaelmcclelland/nimbus-crawler/internal/queue"
	"github.com/michaelmcclelland/nimbus-crawler/internal/robots"
	"github.com/michaelmcclelland/nimbus-crawler/internal/storage"
)

type Parser struct {
	cfg         config.ParserConfig
	pool        *pgxpool.Pool
	publisher   *queue.Publisher
	minio       *storage.MinIOClient
	logger      *slog.Logger
	domainCache sync.Map
}

func New(
	cfg config.ParserConfig,
	pool *pgxpool.Pool,
	publisher *queue.Publisher,
	minio *storage.MinIOClient,
	logger *slog.Logger,
) *Parser {
	return &Parser{
		cfg:       cfg,
		pool:      pool,
		publisher: publisher,
		minio:     minio,
		logger:    logger,
	}
}

func (p *Parser) Run(ctx context.Context, deliveries <-chan queue.Delivery) {
	var wg sync.WaitGroup

	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.worker(ctx, workerID, deliveries)
		}(i)
	}

	wg.Wait()
	p.logger.Info("all parser workers stopped")
}

func (p *Parser) worker(ctx context.Context, id int, deliveries <-chan queue.Delivery) {
	logger := p.logger.With("worker", id)
	logger.Info("parser worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info("parser worker stopping")
			return
		case d, ok := <-deliveries:
			if !ok {
				logger.Info("delivery channel closed")
				return
			}
			p.processMessage(ctx, logger, d)
		}
	}
}

func (p *Parser) processMessage(ctx context.Context, logger *slog.Logger, d queue.Delivery) {
	var msg queue.ParseMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		logger.Error("failed to unmarshal message", "error", err)
		if err := d.Nack(true); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	logger = logger.With("url_id", msg.URLID, "url", msg.URL)

	// Download HTML from MinIO
	parts := strings.SplitN(msg.S3HTMLLink, "/", 2)
	if len(parts) != 2 {
		logger.Error("invalid s3 link", "link", msg.S3HTMLLink)
		if err := d.Nack(true); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	htmlData, err := p.minio.GetObject(ctx, parts[0], parts[1])
	if err != nil {
		logger.Error("failed to get html from minio", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	// Content dedup
	hash := ContentHash(htmlData)
	exists, err := models.ContentHashExists(ctx, p.pool, hash)
	if err != nil {
		logger.Error("content hash check failed, will retry", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}
	if exists {
		logger.Debug("duplicate content, skipping")
		_ = models.UpdateURLStatus(ctx, p.pool, msg.URLID, models.StatusSkipped)
		if err := d.Ack(); err != nil {
			logger.Error("failed to ack message", "error", err)
		}
		return
	}

	// Parse HTML
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlData))
	if err != nil {
		logger.Error("failed to parse html", "error", err)
		if err := d.Nack(true); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	// Extract URLs before ExtractText (which mutates the document by removing elements)
	extractedURLs := ExtractURLs(doc, msg.URL)

	// Extract text (mutates doc by removing script/style/noscript/iframe)
	text := ExtractText(doc)
	textKey := storage.TextKey(msg.URL)
	if err := p.minio.PutObject(ctx, storage.TextBucket, textKey, []byte(text), "text/plain"); err != nil {
		logger.Error("failed to store text", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}
	s3TextLink := storage.TextBucket + "/" + textKey

	// Bulk insert new URLs and publish only newly-inserted ones.
	// Skip if frontier stream is under backpressure — the current page is still
	// fully parsed and marked as 'parsed', but discovered URLs are not enqueued.
	const backpressureThreshold int64 = 80000
	underBackpressure := false
	if streamLen, bpErr := p.publisher.StreamLen(ctx, queue.FrontierStream); bpErr == nil && streamLen > backpressureThreshold {
		logger.Warn("frontier stream backpressure, skipping URL publishing", "stream_len", streamLen)
		underBackpressure = true
	}

	if !underBackpressure && len(extractedURLs) > 0 && msg.Depth+1 <= p.cfg.MaxDepth {
		newDepth := msg.Depth + 1
		var validURLs []string
		var validDomains []string

		// Deduplicate domains to minimize DB calls
		unseenDomains := make(map[string]struct{})
		for _, u := range extractedURLs {
			parsed, err := url.Parse(u)
			if err != nil {
				continue
			}
			domain := parsed.Hostname()
			if domain == "" {
				continue
			}
			// Only upsert domains we haven't seen in-process
			if _, loaded := p.domainCache.LoadOrStore(domain, true); !loaded {
				unseenDomains[domain] = struct{}{}
			}
			validURLs = append(validURLs, u)
			validDomains = append(validDomains, domain)
		}

		for domain := range unseenDomains {
			if err := models.UpsertDomain(ctx, p.pool, domain, robots.DefaultCrawlDelayMs); err != nil {
				logger.Warn("failed to upsert domain", "domain", domain, "error", err)
				p.domainCache.Delete(domain)
			}
		}

		if len(validURLs) > 0 {
			inserted, err := models.BulkInsertURLs(ctx, p.pool, validURLs, validDomains, newDepth)

			// Publish whatever was successfully inserted, even on partial failure
			if len(inserted) > 0 {
				msgs := make([]queue.URLMessage, len(inserted))
				for i, u := range inserted {
					msgs[i] = queue.URLMessage{URL: u, Depth: newDepth}
				}
				if pubErr := p.publisher.PublishURLBatch(ctx, msgs); pubErr != nil {
					logger.Warn("failed to publish url batch", "error", pubErr)
				}
			}

			if err != nil {
				logger.Error("bulk insert partially failed", "error", err, "inserted", len(inserted))
			}
		}
	}

	// Update URL record
	if err := models.UpdateURLParsed(ctx, p.pool, msg.URLID, hash, s3TextLink); err != nil {
		logger.Error("failed to update url record", "error", err)
		if err := d.Nack(false); err != nil {
			logger.Error("failed to nack message", "error", err)
		}
		return
	}

	logger.Info("parsed successfully", "extracted_urls", len(extractedURLs))
	if err := d.Ack(); err != nil {
		logger.Error("failed to ack message", "error", err)
	}
}
