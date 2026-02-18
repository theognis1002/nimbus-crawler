package parser

import (
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
)

const normalizationFlags = purell.FlagLowercaseScheme |
	purell.FlagLowercaseHost |
	purell.FlagUppercaseEscapes |
	purell.FlagRemoveDefaultPort |
	purell.FlagRemoveTrailingSlash |
	purell.FlagRemoveDotSegments |
	purell.FlagRemoveDuplicateSlashes |
	purell.FlagRemoveFragment |
	purell.FlagSortQuery

func ExtractText(doc *goquery.Document) string {
	doc.Find("script, style, noscript, iframe").Remove()

	var sb strings.Builder
	doc.Find("body").Each(func(_ int, s *goquery.Selection) {
		sb.WriteString(strings.TrimSpace(s.Text()))
	})

	return sb.String()
}

func ExtractURLs(doc *goquery.Document, baseURL string) []string {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}

	seen := make(map[string]struct{})
	var urls []string

	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists || href == "" {
			return
		}

		href = strings.TrimSpace(href)

		// Skip non-HTTP
		if strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") ||
			strings.HasPrefix(href, "tel:") || strings.HasPrefix(href, "#") {
			return
		}

		parsed, err := url.Parse(href)
		if err != nil {
			return
		}

		resolved := base.ResolveReference(parsed)

		if resolved.Scheme != "http" && resolved.Scheme != "https" {
			return
		}

		normalized := purell.NormalizeURL(resolved, normalizationFlags)

		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		urls = append(urls, normalized)
	})

	return urls
}
