package parser

import (
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
)

func docFromHTML(t *testing.T, html string) *goquery.Document {
	t.Helper()
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatalf("failed to parse HTML: %v", err)
	}
	return doc
}

func TestExtractText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		html     string
		contains string
		empty    bool
	}{
		{
			name:     "basic body text",
			html:     `<html><body><p>Hello World</p></body></html>`,
			contains: "Hello World",
		},
		{
			name:  "strips script style noscript iframe",
			html:  `<html><body><script>var x=1;</script><style>.a{}</style><noscript>no</noscript><iframe>frame</iframe><p>Visible</p></body></html>`,
			contains: "Visible",
		},
		{
			name:  "empty body",
			html:  `<html><body></body></html>`,
			empty: true,
		},
		{
			name:     "nested elements",
			html:     `<html><body><div><span>Nested</span> <b>Text</b></div></body></html>`,
			contains: "Nested",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			doc := docFromHTML(t, tt.html)
			got := ExtractText(doc)
			if tt.empty {
				if got != "" {
					t.Errorf("expected empty string, got %q", got)
				}
				return
			}
			if !strings.Contains(got, tt.contains) {
				t.Errorf("expected text to contain %q, got %q", tt.contains, got)
			}
		})
	}
}

func TestExtractText_StripsScriptContent(t *testing.T) {
	t.Parallel()
	doc := docFromHTML(t, `<html><body><script>var secret=1;</script><p>OK</p></body></html>`)
	got := ExtractText(doc)
	if strings.Contains(got, "secret") {
		t.Errorf("script content should be stripped, got %q", got)
	}
}

func TestExtractURLs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		html    string
		baseURL string
		want    []string
		wantNil bool
	}{
		{
			name:    "absolute URLs",
			html:    `<html><body><a href="https://example.com/page">link</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://example.com/page"},
		},
		{
			name:    "relative URLs resolved",
			html:    `<html><body><a href="/about">about</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://example.com/about"},
		},
		{
			name:    "javascript mailto tel hash filtered",
			html:    `<html><body><a href="javascript:void(0)">js</a><a href="mailto:a@b.com">mail</a><a href="tel:123">tel</a><a href="#top">hash</a><a href="https://ok.com">ok</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://ok.com"},
		},
		{
			name:    "non-http scheme filtered after resolution",
			html:    `<html><body><a href="ftp://files.example.com/data">ftp</a><a href="https://ok.com">ok</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://ok.com"},
		},
		{
			name:    "duplicates deduplicated",
			html:    `<html><body><a href="https://example.com/page">a</a><a href="https://example.com/page">b</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://example.com/page"},
		},
		{
			name:    "URL normalization lowercase host remove fragment sort query",
			html:    `<html><body><a href="https://Example.COM/path?b=2&a=1#frag">link</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://example.com/path?a=1&b=2"},
		},
		{
			name:    "invalid base URL returns nil",
			html:    `<html><body><a href="/page">link</a></body></html>`,
			baseURL: "://invalid",
			wantNil: true,
		},
		{
			name:    "empty href skipped",
			html:    `<html><body><a href="">empty</a><a href="https://ok.com">ok</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://ok.com"},
		},
		{
			name:    "mixed valid and invalid hrefs",
			html:    `<html><body><a href="javascript:alert(1)">bad</a><a href="https://good.com/a">good</a><a href="mailto:x@y.z">mail</a><a href="/relative">rel</a></body></html>`,
			baseURL: "https://example.com",
			want:    []string{"https://good.com/a", "https://example.com/relative"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			doc := docFromHTML(t, tt.html)
			got := ExtractURLs(doc, tt.baseURL)
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d URLs, got %d: %v", len(tt.want), len(got), got)
			}
			for i, u := range tt.want {
				if got[i] != u {
					t.Errorf("URL[%d] = %q, want %q", i, got[i], u)
				}
			}
		})
	}
}
