package storage

import "testing"

func TestHTMLKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "standard URL",
			url:  "https://example.com/page/about",
			want: "example.com/page/about.html",
		},
		{
			name: "root path",
			url:  "https://example.com/",
			want: "example.com/index.html",
		},
		{
			name: "no path",
			url:  "https://example.com",
			want: "example.com/index.html",
		},
		{
			name: "trailing slash stripped",
			url:  "https://example.com/page/",
			want: "example.com/page.html",
		},
		{
			name: "URL with query and fragment",
			url:  "https://example.com/search?q=test#top",
			want: "example.com/search.html",
		},
		{
			name: "invalid URL fallback",
			url:  "://invalid",
			want: "unknown/___invalid.html",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := HTMLKey(tt.url)
			if got != tt.want {
				t.Errorf("HTMLKey(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestTextKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "standard URL",
			url:  "https://example.com/page/about",
			want: "example.com/page/about.txt",
		},
		{
			name: "root path",
			url:  "https://example.com/",
			want: "example.com/index.txt",
		},
		{
			name: "no path",
			url:  "https://example.com",
			want: "example.com/index.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := TextKey(tt.url)
			if got != tt.want {
				t.Errorf("TextKey(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}
