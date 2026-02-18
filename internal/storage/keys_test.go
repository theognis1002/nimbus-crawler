package storage

import (
	"strings"
	"testing"
)

func TestHTMLKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		url        string
		wantPrefix string
		wantSuffix string
	}{
		{
			name:       "standard URL",
			url:        "https://example.com/page/about",
			wantPrefix: "example.com/page/about_",
			wantSuffix: ".html",
		},
		{
			name:       "root path",
			url:        "https://example.com/",
			wantPrefix: "example.com/index_",
			wantSuffix: ".html",
		},
		{
			name:       "no path",
			url:        "https://example.com",
			wantPrefix: "example.com/index_",
			wantSuffix: ".html",
		},
		{
			name:       "trailing slash stripped",
			url:        "https://example.com/page/",
			wantPrefix: "example.com/page_",
			wantSuffix: ".html",
		},
		{
			name:       "URL with query and fragment",
			url:        "https://example.com/search?q=test#top",
			wantPrefix: "example.com/search_",
			wantSuffix: ".html",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := HTMLKey(tt.url)
			if !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("HTMLKey(%q) = %q, want prefix %q", tt.url, got, tt.wantPrefix)
			}
			if !strings.HasSuffix(got, tt.wantSuffix) {
				t.Errorf("HTMLKey(%q) = %q, want suffix %q", tt.url, got, tt.wantSuffix)
			}
		})
	}
}

func TestHTMLKey_InvalidURL(t *testing.T) {
	t.Parallel()
	got := HTMLKey("://invalid")
	if !strings.HasPrefix(got, "unknown/") || !strings.HasSuffix(got, ".html") {
		t.Errorf("HTMLKey(invalid) = %q, want unknown/*.html", got)
	}
}

func TestHTMLKey_Deterministic(t *testing.T) {
	t.Parallel()
	a := HTMLKey("https://example.com/page")
	b := HTMLKey("https://example.com/page")
	if a != b {
		t.Errorf("HTMLKey should be deterministic: %q != %q", a, b)
	}
}

func TestHTMLKey_DifferentURLsDifferentKeys(t *testing.T) {
	t.Parallel()
	a := HTMLKey("https://example.com/page?a=1")
	b := HTMLKey("https://example.com/page?b=2")
	if a == b {
		t.Errorf("different URLs should produce different keys: both = %q", a)
	}
}

func TestTextKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		url        string
		wantPrefix string
		wantSuffix string
	}{
		{
			name:       "standard URL",
			url:        "https://example.com/page/about",
			wantPrefix: "example.com/page/about_",
			wantSuffix: ".txt",
		},
		{
			name:       "root path",
			url:        "https://example.com/",
			wantPrefix: "example.com/index_",
			wantSuffix: ".txt",
		},
		{
			name:       "no path",
			url:        "https://example.com",
			wantPrefix: "example.com/index_",
			wantSuffix: ".txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := TextKey(tt.url)
			if !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("TextKey(%q) = %q, want prefix %q", tt.url, got, tt.wantPrefix)
			}
			if !strings.HasSuffix(got, tt.wantSuffix) {
				t.Errorf("TextKey(%q) = %q, want suffix %q", tt.url, got, tt.wantSuffix)
			}
		})
	}
}
