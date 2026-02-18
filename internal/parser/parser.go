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
	"github.com/michaelmcclelland/nimbus-crawler/internal/storage"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Parser struct {
	cfg       config.ParserConfig
	pool      *pgxpool.Pool
	publisher *queue.Publisher
	minio     *storage.MinIOClient
	logger    *slog.Logger
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

func (p *Parser) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
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

func (p *Parser) worker(ctx context.Context, id int, deliveries <-chan amqp.Delivery) {
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

func (p *Parser) processMessage(ctx context.Context, logger *slog.Logger, d amqp.Delivery) {
	var msg queue.ParseMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		logger.Error("failed to unmarshal message", "error", err)
		_ = d.Nack(false, false)
		return
	}

	logger = logger.With("url_id", msg.URLID, "url", msg.URL)

	// Download HTML from MinIO
	parts := strings.SplitN(msg.S3HTMLLink, "/", 2)
	if len(parts) != 2 {
		logger.Error("invalid s3 link", "link", msg.S3HTMLLink)
		_ = d.Nack(false, false)
		return
	}

	htmlData, err := p.minio.GetObject(ctx, parts[0], parts[1])
	if err != nil {
		logger.Error("failed to get html from minio", "error", err)
		_ = d.Nack(false, true)
		return
	}

	// Content dedup
	hash := ContentHash(htmlData)
	exists, err := models.ContentHashExists(ctx, p.pool, hash)
	if err != nil {
		logger.Warn("content hash check failed", "error", err)
	}
	if exists {
		logger.Debug("duplicate content, skipping")
		_ = models.UpdateURLStatus(ctx, p.pool, msg.URLID, "skipped")
		_ = d.Ack(false)
		return
	}

	// Parse HTML
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlData))
	if err != nil {
		logger.Error("failed to parse html", "error", err)
		_ = d.Nack(false, false)
		return
	}

	// Extract URLs before ExtractText (which mutates the document by removing elements)
	extractedURLs := ExtractURLs(doc, msg.URL)

	// Extract text (mutates doc by removing script/style/noscript/iframe)
	text := ExtractText(doc)
	textKey := storage.TextKey(msg.URL)
	if err := p.minio.PutObject(ctx, storage.TextBucket, textKey, []byte(text), "text/plain"); err != nil {
		logger.Error("failed to store text", "error", err)
		_ = d.Nack(false, true)
		return
	}
	s3TextLink := storage.TextBucket + "/" + textKey

	// Bulk insert new URLs and publish only newly-inserted ones
	if len(extractedURLs) > 0 && msg.Depth+1 <= p.cfg.MaxDepth {
		newDepth := msg.Depth + 1
		var validURLs []string
		var validDomains []string

		for _, u := range extractedURLs {
			parsed, err := url.Parse(u)
			if err != nil {
				continue
			}
			domain := parsed.Hostname()
			if domain == "" {
				continue
			}
			if err := models.UpsertDomain(ctx, p.pool, domain, 1000); err != nil {
				logger.Warn("failed to upsert domain", "domain", domain, "error", err)
				continue
			}
			validURLs = append(validURLs, u)
			validDomains = append(validDomains, domain)
		}

		if len(validURLs) > 0 {
			inserted, err := models.BulkInsertURLs(ctx, p.pool, validURLs, validDomains, newDepth)
			if err != nil {
				logger.Warn("bulk insert failed", "error", err)
			}

			for _, u := range inserted {
				if err := p.publisher.PublishURL(ctx, queue.URLMessage{URL: u, Depth: newDepth}); err != nil {
					logger.Warn("failed to publish url", "url", u, "error", err)
				}
			}
		}
	}

	// Update URL record
	if err := models.UpdateURLParsed(ctx, p.pool, msg.URLID, hash, s3TextLink); err != nil {
		logger.Error("failed to update url record", "error", err)
		_ = d.Nack(false, true)
		return
	}

	logger.Info("parsed successfully", "extracted_urls", len(extractedURLs))
	_ = d.Ack(false)
}
