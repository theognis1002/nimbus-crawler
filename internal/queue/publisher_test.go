package queue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPublishURLBatch_Empty(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	if err := p.PublishURLBatch(context.Background(), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Stream should not exist
	length, err := rdb.XLen(context.Background(), FrontierStream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != 0 {
		t.Errorf("stream length = %d, want 0", length)
	}
}

func TestPublishURLBatch_SingleChunk(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	msgs := make([]URLMessage, 10)
	for i := range msgs {
		msgs[i] = URLMessage{URL: "https://example.com/" + string(rune('a'+i)), Depth: 1}
	}

	if err := p.PublishURLBatch(context.Background(), msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	length, err := rdb.XLen(context.Background(), FrontierStream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != 10 {
		t.Errorf("stream length = %d, want 10", length)
	}
}

func TestPublishURLBatch_MultipleChunks(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	// Create more messages than pipelineBatchMax (500)
	count := pipelineBatchMax + 100
	msgs := make([]URLMessage, count)
	for i := range msgs {
		msgs[i] = URLMessage{URL: "https://example.com/page", Depth: 0}
	}

	if err := p.PublishURLBatch(context.Background(), msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	length, err := rdb.XLen(context.Background(), FrontierStream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != int64(count) {
		t.Errorf("stream length = %d, want %d", length, count)
	}
}

func TestPublishURLBatch_ExactChunkBoundary(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	// Exactly pipelineBatchMax messages
	msgs := make([]URLMessage, pipelineBatchMax)
	for i := range msgs {
		msgs[i] = URLMessage{URL: "https://example.com/page", Depth: 0}
	}

	if err := p.PublishURLBatch(context.Background(), msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	length, err := rdb.XLen(context.Background(), FrontierStream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != int64(pipelineBatchMax) {
		t.Errorf("stream length = %d, want %d", length, pipelineBatchMax)
	}
}

func TestPublishURL(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	msg := URLMessage{URL: "https://example.com/page", Depth: 2}
	if err := p.PublishURL(context.Background(), msg); err != nil {
		t.Fatalf("PublishURL: %v", err)
	}

	// Read the stream entry and verify payload
	entries, err := rdb.XRange(context.Background(), FrontierStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	payload, ok := entries[0].Values["payload"].(string)
	if !ok {
		t.Fatal("payload field not found or not a string")
	}

	var got URLMessage
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.URL != msg.URL || got.Depth != msg.Depth {
		t.Errorf("got %+v, want %+v", got, msg)
	}
}

func TestPublishParse(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	msg := ParseMessage{
		URLID:      "uuid-123",
		URL:        "https://example.com/page",
		S3HTMLLink: "html/uuid-123.html",
		Depth:      1,
	}
	if err := p.PublishParse(context.Background(), msg); err != nil {
		t.Fatalf("PublishParse: %v", err)
	}

	entries, err := rdb.XRange(context.Background(), ParseStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	payload, ok := entries[0].Values["payload"].(string)
	if !ok {
		t.Fatal("payload field not found or not a string")
	}

	var got ParseMessage
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got != msg {
		t.Errorf("got %+v, want %+v", got, msg)
	}
}

func TestStreamLen(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := NewPublisher(rdb)

	// Publish a few messages
	for i := 0; i < 5; i++ {
		if err := p.PublishURL(context.Background(), URLMessage{URL: "https://example.com", Depth: 0}); err != nil {
			t.Fatalf("PublishURL: %v", err)
		}
	}

	length, err := p.StreamLen(context.Background(), FrontierStream)
	if err != nil {
		t.Fatalf("StreamLen: %v", err)
	}
	if length != 5 {
		t.Errorf("StreamLen = %d, want 5", length)
	}
}
