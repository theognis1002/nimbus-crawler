package robots

import "testing"

func TestExtractCrawlDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "NimbusCrawler user-agent with crawl delay",
			body: "User-agent: NimbusCrawler\nCrawl-delay: 2\n",
			want: 2000,
		},
		{
			name: "fallback to wildcard group",
			body: "User-agent: *\nCrawl-delay: 3\n",
			want: 3000,
		},
		{
			name: "no crawl delay returns default",
			body: "User-agent: *\nDisallow: /private\n",
			want: DefaultCrawlDelayMs,
		},
		{
			name: "crawl delay below minimum clamped",
			body: "User-agent: NimbusCrawler\nCrawl-delay: 0.01\n",
			want: MinCrawlDelayMs,
		},
		{
			name: "empty body returns default",
			body: "",
			want: DefaultCrawlDelayMs,
		},
		{
			name: "NimbusCrawler preferred over wildcard",
			body: "User-agent: *\nCrawl-delay: 5\n\nUser-agent: NimbusCrawler\nCrawl-delay: 2\n",
			want: 2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractCrawlDelay(tt.body)
			if got != tt.want {
				t.Errorf("extractCrawlDelay() = %d, want %d", got, tt.want)
			}
		})
	}
}
