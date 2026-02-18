package queue

type URLMessage struct {
	URL   string `json:"url"`
	Depth int    `json:"depth"`
}

type ParseMessage struct {
	URLID      string `json:"url_id"`
	URL        string `json:"url"`
	S3HTMLLink string `json:"s3_html_link"`
	Depth      int    `json:"depth"`
}
