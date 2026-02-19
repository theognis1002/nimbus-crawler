CREATE TYPE url_status AS ENUM ('pending', 'crawling', 'crawled', 'parsed', 'failed', 'skipped');

CREATE TABLE domains (
    domain          TEXT PRIMARY KEY,
    last_crawl_time TIMESTAMPTZ,
    robots_txt      TEXT,
    crawl_delay_ms  INTEGER NOT NULL DEFAULT 200,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE urls (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url             TEXT NOT NULL UNIQUE,
    domain          TEXT NOT NULL REFERENCES domains(domain),
    s3_html_link    TEXT,
    s3_text_link    TEXT,
    content_hash    CHAR(64),
    depth           INTEGER NOT NULL DEFAULT 0,
    status          url_status NOT NULL DEFAULT 'pending',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_crawl_time TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_urls_status ON urls(status);
CREATE INDEX idx_urls_domain ON urls(domain);
CREATE INDEX idx_urls_depth ON urls(depth);
CREATE INDEX idx_urls_content_hash ON urls(content_hash);
