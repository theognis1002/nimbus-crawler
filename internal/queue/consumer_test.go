package queue

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func setupConsumer(t *testing.T) (*miniredis.Miniredis, *redis.Client, *Consumer) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	stream := "stream:test"
	dlq := "stream:test:dlq"
	group := "test-group"
	consumer := "test-consumer"

	// Create consumer group
	err := rdb.XGroupCreateMkStream(context.Background(), stream, group, "0").Err()
	if err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}

	c := NewConsumer(rdb, stream, dlq, group, consumer, 10, testLogger())
	return mr, rdb, c
}

func TestBuildDelivery_ValidPayload(t *testing.T) {
	t.Parallel()
	_, _, c := setupConsumer(t)

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"payload": `{"url":"https://example.com","depth":0}`},
	}

	d, ok := c.buildDelivery(msg)
	if !ok {
		t.Fatal("expected ok=true for valid payload")
	}
	if string(d.Body) != `{"url":"https://example.com","depth":0}` {
		t.Errorf("body = %q, want JSON payload", string(d.Body))
	}
}

func TestBuildDelivery_MissingPayload(t *testing.T) {
	t.Parallel()
	_, _, c := setupConsumer(t)

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"other": "data"},
	}

	_, ok := c.buildDelivery(msg)
	if ok {
		t.Error("expected ok=false for missing payload")
	}
}

func TestBuildDelivery_EmptyPayload(t *testing.T) {
	t.Parallel()
	_, _, c := setupConsumer(t)

	msg := redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"payload": ""},
	}

	_, ok := c.buildDelivery(msg)
	if ok {
		t.Error("expected ok=false for empty payload")
	}
}

func TestConsumerRun_DeliversMessage(t *testing.T) {
	t.Parallel()
	_, rdb, c := setupConsumer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Run(ctx)

	// Add a message to the stream
	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "stream:test",
		Values: map[string]interface{}{"payload": `{"url":"https://example.com","depth":1}`},
	}).Err()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		if string(d.Body) != `{"url":"https://example.com","depth":1}` {
			t.Errorf("body = %q, want JSON payload", string(d.Body))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestConsumerRun_ContextCancelClosesChannel(t *testing.T) {
	t.Parallel()
	_, _, c := setupConsumer(t)

	ctx, cancel := context.WithCancel(context.Background())
	ch := c.Run(ctx)

	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			// Got a message, drain and wait for close
			for range ch {
			}
		}
		// Channel closed as expected
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestDelivery_Ack(t *testing.T) {
	t.Parallel()
	_, rdb, c := setupConsumer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Run(ctx)

	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "stream:test",
		Values: map[string]interface{}{"payload": "test-data"},
	}).Err()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	select {
	case d := <-ch:
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		// Verify PEL is empty
		pending, err := rdb.XPending(context.Background(), "stream:test", "test-group").Result()
		if err != nil {
			t.Fatalf("XPending: %v", err)
		}
		if pending.Count != 0 {
			t.Errorf("PEL count = %d, want 0", pending.Count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestDelivery_NackToDLQ(t *testing.T) {
	t.Parallel()
	_, rdb, c := setupConsumer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Run(ctx)

	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "stream:test",
		Values: map[string]interface{}{"payload": "bad-data"},
	}).Err()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	select {
	case d := <-ch:
		if err := d.Nack(true); err != nil {
			t.Fatalf("Nack(true): %v", err)
		}

		// Verify message in DLQ
		dlqLen, err := rdb.XLen(context.Background(), "stream:test:dlq").Result()
		if err != nil {
			t.Fatalf("XLen DLQ: %v", err)
		}
		if dlqLen != 1 {
			t.Errorf("DLQ length = %d, want 1", dlqLen)
		}

		// Verify PEL is cleared
		pending, err := rdb.XPending(context.Background(), "stream:test", "test-group").Result()
		if err != nil {
			t.Fatalf("XPending: %v", err)
		}
		if pending.Count != 0 {
			t.Errorf("PEL count = %d, want 0 after Nack to DLQ", pending.Count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestDelivery_NackWithoutDLQ(t *testing.T) {
	t.Parallel()
	_, rdb, c := setupConsumer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Run(ctx)

	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "stream:test",
		Values: map[string]interface{}{"payload": "retry-data"},
	}).Err()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	select {
	case d := <-ch:
		if err := d.Nack(false); err != nil {
			t.Fatalf("Nack(false): %v", err)
		}

		// Verify DLQ is empty
		dlqLen, err := rdb.XLen(context.Background(), "stream:test:dlq").Result()
		if err != nil {
			t.Fatalf("XLen DLQ: %v", err)
		}
		if dlqLen != 0 {
			t.Errorf("DLQ length = %d, want 0 (no-op nack)", dlqLen)
		}

		// Verify message stays in PEL
		pending, err := rdb.XPending(context.Background(), "stream:test", "test-group").Result()
		if err != nil {
			t.Fatalf("XPending: %v", err)
		}
		if pending.Count != 1 {
			t.Errorf("PEL count = %d, want 1 (message should remain)", pending.Count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}
